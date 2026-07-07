/*
Organizarr - A qBittorrent companion that organizes torrents with state-aware rules, complementing the *arr suite.
Copyright (C) 2023 Harrison Chin
Copyright (C) 2026 Loadst0ne and Organizarr contributors

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

use super::config::{ErroredCompletedAction, ServerConfig};
use anyhow::{bail, Context, Result};
use log::{info, warn};
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::time::sleep;

const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
/// Page size used when listing torrents. Fetching in pages keeps memory and
/// response sizes bounded on clients saturated with thousands of torrents.
const TORRENT_PAGE_SIZE: usize = 500;
/// First retry delay for the errored-torrent recovery backoff.
const RECOVERY_BACKOFF_BASE: Duration = Duration::from_secs(60);
/// Upper bound for the recovery backoff delay.
const RECOVERY_BACKOFF_CAP: Duration = Duration::from_secs(3600);

/// The state of a torrent as reported by the qBittorrent WebUI API in the
/// `state` field of `torrents/info`. Covers both qBittorrent 5.x
/// (`stoppedUP`/`stoppedDL`) and 4.x (`pausedUP`/`pausedDL`) spellings.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TorrentState {
    // Error states
    #[serde(rename = "error")]
    Error,
    #[serde(rename = "missingFiles")]
    MissingFiles,
    // Download finished (upload-side) states
    #[serde(rename = "uploading")]
    Uploading,
    #[serde(rename = "stoppedUP", alias = "pausedUP")]
    StoppedUpload,
    #[serde(rename = "queuedUP")]
    QueuedUpload,
    #[serde(rename = "stalledUP")]
    StalledUpload,
    #[serde(rename = "checkingUP")]
    CheckingUpload,
    #[serde(rename = "forcedUP")]
    ForcedUpload,
    // Download in progress states
    #[serde(rename = "allocating")]
    Allocating,
    #[serde(rename = "downloading")]
    Downloading,
    #[serde(rename = "metaDL")]
    FetchingMetadata,
    #[serde(rename = "forcedMetaDL")]
    ForcedFetchingMetadata,
    #[serde(rename = "stoppedDL", alias = "pausedDL")]
    StoppedDownload,
    #[serde(rename = "queuedDL")]
    QueuedDownload,
    #[serde(rename = "stalledDL")]
    StalledDownload,
    #[serde(rename = "checkingDL")]
    CheckingDownload,
    #[serde(rename = "forcedDL")]
    ForcedDownload,
    // Transitional / other states
    #[serde(rename = "checkingResumeData")]
    CheckingResumeData,
    #[serde(rename = "moving")]
    Moving,
    #[serde(other)]
    #[default]
    Unknown,
}

impl TorrentState {
    /// True when the torrent's download has finished (the payload on disk
    /// is complete), regardless of what it is doing now.
    pub fn download_finished(&self) -> bool {
        matches!(
            self,
            Self::Uploading
                | Self::StoppedUpload
                | Self::QueuedUpload
                | Self::StalledUpload
                | Self::CheckingUpload
                | Self::ForcedUpload
                | Self::Moving
        )
    }

    /// True when it is safe for this tool to move the torrent's files:
    /// the download is finished AND qBittorrent is not actively using the
    /// payload for something that a concurrent move would corrupt.
    ///
    /// Deliberately excluded even though the download is finished:
    /// - `Moving`: qBittorrent is relocating the files itself; racing it
    ///   would corrupt or lose data.
    /// - `CheckingUpload`: the payload is being re-verified, with files
    ///   held open for hashing.
    /// - `Error`/`MissingFiles`: something is wrong; acting on inconsistent
    ///   state risks destroying data.
    pub fn safe_to_move(&self) -> bool {
        matches!(
            self,
            Self::Uploading
                | Self::StoppedUpload
                | Self::QueuedUpload
                | Self::StalledUpload
                | Self::ForcedUpload
        )
    }

    /// True for states that indicate something is wrong with the torrent.
    pub fn is_errored(&self) -> bool {
        matches!(self, Self::Error | Self::MissingFiles)
    }

    /// Human-readable description used in logs.
    pub fn describe(&self) -> &'static str {
        match self {
            Self::Error => "errored",
            Self::MissingFiles => "errored (missing files)",
            Self::Uploading => "seeding",
            Self::StoppedUpload => "completed (stopped)",
            Self::QueuedUpload => "completed (queued for seeding)",
            Self::StalledUpload => "seeding (stalled)",
            Self::CheckingUpload => "completed (checking)",
            Self::ForcedUpload => "seeding (forced)",
            Self::Allocating => "allocating space",
            Self::Downloading => "downloading",
            Self::FetchingMetadata => "fetching metadata",
            Self::ForcedFetchingMetadata => "fetching metadata (forced)",
            Self::StoppedDownload => "stopped (incomplete)",
            Self::QueuedDownload => "queued for download",
            Self::StalledDownload => "downloading (stalled)",
            Self::CheckingDownload => "checking (incomplete)",
            Self::ForcedDownload => "downloading (forced)",
            Self::CheckingResumeData => "checking resume data",
            Self::Moving => "being moved by qBittorrent",
            Self::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for TorrentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.describe())
    }
}

/// Remembers each torrent's last observed state for one server, so that
/// state transitions can be detected and logged across polling cycles, and
/// tracks the exponential-backoff schedule for errored-torrent recovery.
#[derive(Debug, Default, Clone)]
pub struct StateTracker {
    states: std::collections::HashMap<String, TorrentState>,
    /// Previous state (from the last cycle) for torrents whose state was
    /// already known, captured by the most recent `observe` call. Used to
    /// match state *transitions* in user rules and default behaviors.
    previous: std::collections::HashMap<String, TorrentState>,
    retries: std::collections::HashMap<String, RetryState>,
}

/// Per-torrent recovery backoff bookkeeping.
#[derive(Debug, Clone, Copy)]
struct RetryState {
    attempts: u32,
    next_attempt: Instant,
}

/// Exponential backoff delay for the given (1-based) attempt number:
/// 60s, 120s, 240s, ... capped at one hour.
fn recovery_backoff_delay(attempts: u32) -> Duration {
    let factor = 2u32.saturating_pow(attempts.saturating_sub(1));
    RECOVERY_BACKOFF_BASE
        .saturating_mul(factor)
        .min(RECOVERY_BACKOFF_CAP)
}

impl StateTracker {
    /// Records the states observed this cycle, logging every transition
    /// (and newly finished downloads in particular). Torrents that are no
    /// longer reported (e.g. removed after a move) are forgotten.
    pub fn observe(&mut self, torrents: &[Torrent]) {
        let mut seen = std::collections::HashMap::with_capacity(torrents.len());
        for torrent in torrents {
            seen.insert(torrent.hash.clone(), torrent.state);
            match self.states.get(&torrent.hash) {
                Some(previous) if *previous != torrent.state => {
                    if torrent.state.is_errored() {
                        warn!(
                            "Torrent '{}' changed state: {} -> {}",
                            torrent.name, previous, torrent.state
                        );
                    } else if !previous.download_finished() && torrent.state.download_finished() {
                        info!(
                            "Torrent '{}' finished downloading ({} -> {})",
                            torrent.name, previous, torrent.state
                        );
                    } else {
                        info!(
                            "Torrent '{}' changed state: {} -> {}",
                            torrent.name, previous, torrent.state
                        );
                    }
                }
                Some(_) => {}
                None => {
                    if torrent.state.is_errored() {
                        warn!("Torrent '{}' is in state: {}", torrent.name, torrent.state);
                    }
                }
            }
        }
        // Forget retry schedules for torrents that recovered or disappeared.
        self.retries
            .retain(|hash, _| seen.get(hash).is_some_and(|s| s.is_errored()));
        self.previous = std::mem::take(&mut self.states);
        // Previous states only make sense for torrents still present.
        self.previous.retain(|hash, _| seen.contains_key(hash));
        self.states = seen;
    }

    /// The state this torrent was in on the previous cycle, if it was
    /// known then. `None` for torrents observed for the first time.
    pub fn previous_state(&self, hash: &str) -> Option<TorrentState> {
        self.previous.get(hash).copied()
    }

    /// True when an errored torrent's recovery may be attempted now, i.e.
    /// it has never been attempted or its backoff delay has elapsed.
    pub fn recovery_due(&self, hash: &str, now: Instant) -> bool {
        self.retries.get(hash).is_none_or(|r| now >= r.next_attempt)
    }

    /// Records a recovery attempt for the torrent, scheduling the next one
    /// with exponential backoff. Returns the attempt number (1-based).
    pub fn record_recovery_attempt(&mut self, hash: &str, now: Instant) -> u32 {
        let entry = self.retries.entry(hash.to_string()).or_insert(RetryState {
            attempts: 0,
            next_attempt: now,
        });
        entry.attempts = entry.attempts.saturating_add(1);
        entry.next_attempt = now + recovery_backoff_delay(entry.attempts);
        entry.attempts
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct Torrent {
    pub save_path: String,
    pub name: String,
    pub category: String,
    pub hash: String,
    /// Current state as reported by qBittorrent. Defaults to `Unknown`
    /// (treated as unsafe to act on) if the field is absent.
    #[serde(default)]
    pub state: TorrentState,
    /// Absolute path of the torrent's content on the qBittorrent host
    /// (root path for multi-file torrents, file path for single-file ones).
    /// Available since qBittorrent 4.2; preferred over `save_path` + `name`
    /// because it stays correct when a torrent's content is renamed.
    #[serde(default)]
    pub content_path: Option<String>,
    /// Download progress as a fraction (0.0 to 1.0).
    #[serde(default)]
    pub progress: f64,
    /// Bytes still to be downloaded.
    #[serde(default)]
    pub amount_left: i64,
    /// Unix timestamp of when the download completed; 0 or negative while
    /// the torrent is still incomplete.
    #[serde(default)]
    pub completion_on: i64,
    /// Tags on the torrent, as reported by qBittorrent: a single
    /// comma-separated string (e.g. `"tag1, tag2"`).
    #[serde(default)]
    pub tags: String,
    /// Share ratio.
    #[serde(default)]
    pub ratio: f64,
    /// Total seconds the torrent has been active.
    #[serde(default)]
    pub time_active: i64,
}

impl Torrent {
    /// The torrent's tags as a trimmed list.
    pub fn tag_list(&self) -> Vec<&str> {
        self.tags
            .split(',')
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .collect()
    }

    /// Time the torrent has been active, as a `Duration`.
    pub fn active_duration(&self) -> Duration {
        Duration::from_secs(self.time_active.max(0) as u64)
    }
    /// True when the torrent's payload has been fully downloaded, judged
    /// from actual transfer data rather than the (sometimes ambiguous)
    /// state name. This is what disambiguates errored/queued/stopped
    /// torrents *before* completion from the same states *after*
    /// completion.
    pub fn is_download_complete(&self) -> bool {
        self.state.download_finished()
            || self.progress >= 1.0
            || (self.amount_left == 0 && self.completion_on > 0)
    }

    /// True when this tool may act on the torrent's payload: the download
    /// is complete (per [`Self::is_download_complete`]) and the torrent is
    /// in a state where qBittorrent is not actively using or relocating
    /// the files. Stopped/queued download-side states qualify only when
    /// the transfer data confirms completion.
    pub fn eligible_for_move(&self) -> bool {
        if !self.is_download_complete() {
            return false;
        }
        self.state.safe_to_move()
            || matches!(
                self.state,
                TorrentState::StoppedDownload | TorrentState::QueuedDownload
            )
    }
}

#[derive(Clone)]
pub struct TorrentClient {
    client: Client,
    server: ServerConfig,
}

impl TorrentClient {
    pub fn new(server: ServerConfig) -> Result<Self> {
        // The cookie store holds the SID session cookie issued by
        // /api/v2/auth/login; it is shared across clones of this client.
        let client = Client::builder()
            .cookie_store(true)
            .timeout(HTTP_TIMEOUT)
            .build()
            .context("Failed to build HTTP client")?;
        Ok(Self { client, server })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.server.qbit_url.trim_end_matches('/'), path)
    }

    async fn get(&self, path: &str, query: &[(&str, &str)]) -> Result<Response> {
        let url = self.url(path);
        let response = self
            .client
            .get(&url)
            .query(query)
            .send()
            .await
            .with_context(|| format!("Request to {} failed", url))?
            .error_for_status()
            .with_context(|| format!("Request to {} returned an error status", url))?;
        Ok(response)
    }

    async fn post_form(&self, path: &str, form: &[(&str, &str)]) -> Result<Response> {
        let url = self.url(path);
        let response = self
            .client
            .post(&url)
            .form(form)
            .send()
            .await
            .with_context(|| format!("Request to {} failed", url))?
            .error_for_status()
            .with_context(|| format!("Request to {} returned an error status", url))?;
        Ok(response)
    }

    /// Authenticates against the qBittorrent WebUI, storing the SID session
    /// cookie for subsequent requests. Skipped when `username` is empty,
    /// which supports WebUI setups with authentication bypass enabled.
    pub async fn login(&self) -> Result<()> {
        if self.server.username.is_empty() {
            return Ok(());
        }
        let response = self
            .post_form(
                "/api/v2/auth/login",
                &[
                    ("username", &self.server.username),
                    ("password", &self.server.password),
                ],
            )
            .await
            .with_context(|| format!("Failed to reach qBittorrent at {}", self.server.qbit_url))?;
        let body = response.text().await?;
        if body.trim() != "Ok." {
            bail!(
                "Authentication failed for {} (check username/password)",
                self.server.qbit_url
            );
        }
        Ok(())
    }

    /// Ends the WebUI session. Best-effort; errors can safely be ignored.
    pub async fn logout(&self) -> Result<()> {
        if self.server.username.is_empty() {
            return Ok(());
        }
        self.post_form("/api/v2/auth/logout", &[]).await?;
        Ok(())
    }

    /// Fetches all torrents with their current states. The full list (not
    /// just completed torrents) is needed so state transitions such as
    /// downloading -> seeding can be observed. The list is fetched in pages
    /// of [`TORRENT_PAGE_SIZE`] so that clients saturated with thousands of
    /// torrents don't produce one enormous request/response.
    pub async fn get_torrents(&self) -> Result<Vec<Torrent>> {
        let mut all = Vec::new();
        let mut offset = 0usize;
        loop {
            let limit = TORRENT_PAGE_SIZE.to_string();
            let offset_str = offset.to_string();
            let response = self
                .get(
                    "/api/v2/torrents/info",
                    &[("limit", limit.as_str()), ("offset", offset_str.as_str())],
                )
                .await?;
            let page = response
                .json::<Vec<Torrent>>()
                .await
                .context("Failed to parse torrent list from qBittorrent")?;
            let page_len = page.len();
            all.extend(page);
            if page_len < TORRENT_PAGE_SIZE {
                break;
            }
            offset += page_len;
        }
        Ok(all)
    }

    /// Removes the torrent from qBittorrent. With `delete_files: false` the
    /// payload is kept (used after this tool has moved it); with `true`
    /// qBittorrent deletes the payload too, honoring its own "move to
    /// trash" preference.
    pub async fn remove_torrent(&self, hash: &str, delete_files: bool) -> Result<()> {
        self.post_form(
            "/api/v2/torrents/delete",
            &[
                ("hashes", hash),
                ("deleteFiles", if delete_files { "true" } else { "false" }),
            ],
        )
        .await?;
        Ok(())
    }

    /// POSTs `hashes` to the first endpoint that exists, falling back on
    /// 404. Used for endpoints renamed between qBittorrent 4.x and 5.x.
    async fn post_hashes_with_fallback(&self, endpoints: &[&str], hash: &str) -> Result<()> {
        for endpoint in endpoints {
            let url = self.url(endpoint);
            let response = self
                .client
                .post(&url)
                .form(&[("hashes", hash)])
                .send()
                .await
                .with_context(|| format!("Request to {} failed", url))?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                // Endpoint not present on this qBittorrent version; try next.
                continue;
            }
            response
                .error_for_status()
                .with_context(|| format!("Request to {} returned an error status", url))?;
            return Ok(());
        }
        bail!(
            "None of {:?} is available on this qBittorrent version",
            endpoints
        );
    }

    /// Stops the torrent so qBittorrent closes its file handles before the
    /// payload is moved. This matters in particular when qBittorrent runs in
    /// a container and this tool runs on the host: open handles held through
    /// the bind-mount file-sharing layer (e.g. Docker Desktop) can otherwise
    /// make the host-side move fail with sharing violations.
    ///
    /// qBittorrent 5.x renamed the endpoint from `torrents/pause` to
    /// `torrents/stop`; the new name is tried first with a fallback for 4.x.
    pub async fn stop_torrent(&self, hash: &str) -> Result<()> {
        self.post_hashes_with_fallback(&["/api/v2/torrents/stop", "/api/v2/torrents/pause"], hash)
            .await
    }

    /// Starts (resumes) the torrent. qBittorrent 5.x renamed the endpoint
    /// from `torrents/resume` to `torrents/start`; the new name is tried
    /// first with a fallback for 4.x.
    pub async fn start_torrent(&self, hash: &str) -> Result<()> {
        self.post_hashes_with_fallback(&["/api/v2/torrents/start", "/api/v2/torrents/resume"], hash)
            .await
    }

    /// Asks qBittorrent to re-verify the torrent's payload on disk.
    pub async fn recheck_torrent(&self, hash: &str) -> Result<()> {
        self.post_form("/api/v2/torrents/recheck", &[("hashes", hash)])
            .await?;
        Ok(())
    }

    /// Attempts to recover an errored torrent whose download has not yet
    /// completed: re-verify the payload, then start it again.
    pub async fn recover_torrent(&self, hash: &str) -> Result<()> {
        self.recheck_torrent(hash).await?;
        self.start_torrent(hash).await?;
        Ok(())
    }

    /// Handles a torrent in an errored state, distinguishing where in its
    /// life the error occurred:
    ///
    /// - **Before/during download** (transfer data shows the payload is
    ///   incomplete): attempt recovery (recheck + start) on an exponential
    ///   backoff schedule tracked per torrent in the [`StateTracker`], so a
    ///   persistently broken torrent is not hammered every polling cycle.
    /// - **After a completed download** (e.g. `missingFiles` after the
    ///   payload was moved or deleted externally): apply the configured
    ///   [`ErroredCompletedAction`]. Only torrents in a category mapped in
    ///   this tool's config are acted on; anything else is left alone.
    pub async fn handle_errored_torrent(
        &self,
        torrent: &Torrent,
        tracker: &mut StateTracker,
        action: ErroredCompletedAction,
    ) {
        if !torrent.is_download_complete() {
            let now = Instant::now();
            if !tracker.recovery_due(&torrent.hash, now) {
                return;
            }
            let attempt = tracker.record_recovery_attempt(&torrent.hash, now);
            info!(
                "Attempting recovery of errored torrent '{}' (attempt {}, next retry in {:?} if it stays errored)",
                torrent.name,
                attempt,
                recovery_backoff_delay(attempt.saturating_add(1))
            );
            if let Err(e) = self.recover_torrent(&torrent.hash).await {
                warn!("Recovery of torrent '{}' failed: {:#}", torrent.name, e);
            }
            return;
        }

        // The download had already completed when the error occurred.
        if !self.server.categories.contains_key(&torrent.category) {
            return;
        }
        match action {
            ErroredCompletedAction::Keep => {
                warn!(
                    "Torrent '{}' errored after completing its download ({}); keeping it as configured",
                    torrent.name, torrent.state
                );
            }
            ErroredCompletedAction::Remove => {
                match self.remove_torrent(&torrent.hash, false).await {
                    Ok(()) => info!(
                        "Removed torrent '{}' (errored after completion: {}), files kept",
                        torrent.name, torrent.state
                    ),
                    Err(e) => warn!(
                        "Failed to remove errored torrent '{}': {:#}",
                        torrent.name, e
                    ),
                }
            }
            ErroredCompletedAction::RemoveWithData => {
                match self.remove_torrent(&torrent.hash, true).await {
                    Ok(()) => info!(
                        "Removed torrent '{}' and its files (errored after completion: {})",
                        torrent.name, torrent.state
                    ),
                    Err(e) => warn!(
                        "Failed to remove errored torrent '{}': {:#}",
                        torrent.name, e
                    ),
                }
            }
        }
    }

    /// Creates a category in qBittorrent. A 409 Conflict (returned when the
    /// category already exists) is treated as success; if the name was truly
    /// invalid the subsequent `setCategory` call will surface the problem.
    pub async fn create_category(&self, category: &str) -> Result<()> {
        let url = self.url("/api/v2/torrents/createCategory");
        let response = self
            .client
            .post(&url)
            .form(&[("category", category), ("savePath", "")])
            .send()
            .await
            .with_context(|| format!("Request to {} failed", url))?;
        if response.status() == reqwest::StatusCode::CONFLICT {
            return Ok(());
        }
        response
            .error_for_status()
            .with_context(|| format!("Request to {} returned an error status", url))?;
        Ok(())
    }

    /// Assigns the torrent to `category`, overwriting any existing category.
    /// qBittorrent answers 409 Conflict when the category does not exist;
    /// with `create_if_missing` the category is then created and the
    /// assignment retried.
    pub async fn set_category(
        &self,
        hash: &str,
        category: &str,
        create_if_missing: bool,
    ) -> Result<()> {
        let url = self.url("/api/v2/torrents/setCategory");
        let form = [("hashes", hash), ("category", category)];
        let response = self
            .client
            .post(&url)
            .form(&form)
            .send()
            .await
            .with_context(|| format!("Request to {} failed", url))?;
        if response.status() == reqwest::StatusCode::CONFLICT && create_if_missing {
            self.create_category(category).await?;
            self.post_form("/api/v2/torrents/setCategory", &form)
                .await
                .with_context(|| {
                    format!("Failed to set category {:?} after creating it", category)
                })?;
            return Ok(());
        }
        response
            .error_for_status()
            .with_context(|| format!("Request to {} returned an error status", url))?;
        Ok(())
    }

    /// Adds tags (comma-separated) to the torrent. Unknown tags are created
    /// by qBittorrent automatically.
    pub async fn add_tags(&self, hash: &str, tags: &str) -> Result<()> {
        self.post_form(
            "/api/v2/torrents/addTags",
            &[("hashes", hash), ("tags", tags)],
        )
        .await?;
        Ok(())
    }

    /// Removes tags (comma-separated) from the torrent.
    pub async fn remove_tags(&self, hash: &str, tags: &str) -> Result<()> {
        self.post_form(
            "/api/v2/torrents/removeTags",
            &[("hashes", hash), ("tags", tags)],
        )
        .await?;
        Ok(())
    }

    /// Removes every tag from the torrent. The torrent's current tags are
    /// passed explicitly rather than relying on the "empty tags parameter
    /// removes all tags" behavior, which varies between qBittorrent versions.
    pub async fn clear_tags(&self, torrent: &Torrent) -> Result<()> {
        let tags = torrent.tag_list().join(",");
        if tags.is_empty() {
            return Ok(());
        }
        self.remove_tags(&torrent.hash, &tags).await
    }

    /// Deletes the torrent's payload to the OS trash / recycle bin (resolved
    /// through the configured path remapping, so the *host-side* copy of the
    /// files is trashed), then removes the torrent entry from qBittorrent.
    ///
    /// Returns an error when trashing is not possible (e.g. the files live
    /// on a mount without a trash location) so the caller can park the
    /// torrent in the delete fallback category and retry on a later cycle.
    pub async fn delete_torrent_to_trash(&self, torrent: &Torrent) -> Result<()> {
        let src = self.resolve_source_path(torrent)?;
        match self.stop_torrent(&torrent.hash).await {
            // Let qBittorrent release its file handles first.
            Ok(()) => sleep(Duration::from_millis(500)).await,
            Err(e) => warn!(
                "Could not stop torrent '{}' before trashing (continuing anyway): {:#}",
                torrent.name, e
            ),
        }
        let src_clone = src.clone();
        let trashed = tokio::task::spawn_blocking(move || -> Result<bool> {
            if !src_clone.exists() {
                // Payload already gone (removed manually or by a previous
                // cycle); only the torrent entry is left to clean up.
                return Ok(false);
            }
            trash::delete(&src_clone)
                .with_context(|| format!("Failed to move {:?} to the trash", src_clone))?;
            Ok(true)
        })
        .await
        .context("Trash task panicked")??;

        self.remove_torrent(&torrent.hash, false)
            .await
            .with_context(|| {
                format!(
                    "Payload of '{}' was trashed, but removing the torrent from qBittorrent failed",
                    torrent.name
                )
            })?;
        if trashed {
            info!(
                "Moved payload of '{}' to the trash and removed it from qBittorrent",
                torrent.name
            );
        } else {
            info!(
                "Payload of '{}' was already gone; removed it from qBittorrent",
                torrent.name
            );
        }
        Ok(())
    }

    /// Maps the torrent's path on the qBittorrent host to a local path,
    /// applying the configured `path_prefix` -> `root_path` remapping.
    fn resolve_source_path(&self, torrent: &Torrent) -> Result<PathBuf> {
        let remote_path = match &torrent.content_path {
            Some(p) if !p.is_empty() => PathBuf::from(p),
            _ => PathBuf::from(&torrent.save_path).join(&torrent.name),
        };
        let relative_path = match self.server.path_prefix.as_deref() {
            Some(prefix) if !prefix.is_empty() => remote_path
                .strip_prefix(prefix)
                .with_context(|| {
                    format!(
                        "Torrent path {:?} does not start with configured path_prefix {:?}",
                        remote_path, prefix
                    )
                })?
                .to_path_buf(),
            _ => remote_path,
        };
        let root_path = PathBuf::from(self.server.root_path.as_deref().unwrap_or(""));
        Ok(root_path.join(relative_path))
    }

    /// Moves a completed torrent's payload to the directory configured for
    /// its category, then removes the torrent from qBittorrent (keeping the
    /// moved files). Torrents in unmapped categories, or in states where
    /// acting on the payload is unsafe, are left untouched.
    ///
    /// The torrent is stopped first so its file handles are released, and
    /// the operation is retry-safe: if a previous cycle moved the files but
    /// failed to remove the torrent, the removal is completed this cycle.
    pub async fn move_and_clean_torrent_files(&self, torrent: &Torrent) -> Result<()> {
        let Some(dest_dir) = self.server.categories.get(&torrent.category) else {
            return Ok(());
        };
        self.move_torrent_files_to(torrent, Path::new(dest_dir))
            .await
    }

    /// Moves the torrent's payload into an explicit destination directory
    /// (stop-before-move, path remapping and retry-safety included), then
    /// removes the torrent from qBittorrent keeping the moved files. Used
    /// both by the category-mapping flow and by `move_files` rule actions.
    pub async fn move_torrent_files_to(&self, torrent: &Torrent, dest_dir: &Path) -> Result<()> {
        if !torrent.eligible_for_move() {
            info!(
                "Skipping '{}': state is {} (waiting for a safe, completed state)",
                torrent.name, torrent.state
            );
            return Ok(());
        }
        let src = self.resolve_source_path(torrent)?;
        let dest_dir = dest_dir.to_path_buf();

        match self.stop_torrent(&torrent.hash).await {
            // Give qBittorrent (and any container file-sharing layer) a
            // moment to actually release the file handles.
            Ok(()) => sleep(Duration::from_millis(500)).await,
            Err(e) => warn!(
                "Could not stop torrent '{}' before moving (continuing anyway): {:#}",
                torrent.name, e
            ),
        }

        // File operations are blocking; keep them off the async runtime.
        let (src_clone, dest_clone) = (src.clone(), dest_dir.clone());
        let outcome = tokio::task::spawn_blocking(move || move_path(&src_clone, &dest_clone))
            .await
            .context("Move task panicked")??;

        self.remove_torrent(&torrent.hash, false)
            .await
            .with_context(|| {
            format!(
                "Files for '{}' were moved to {:?}, but removing the torrent from qBittorrent failed",
                torrent.name, dest_dir
            )
        })?;

        match outcome {
            MoveOutcome::Moved => info!(
                "Moved '{}' to {:?} and removed it from qBittorrent",
                torrent.name, dest_dir
            ),
            MoveOutcome::AlreadyMoved => info!(
                "'{}' was already present in {:?} (previous move); removed it from qBittorrent",
                torrent.name, dest_dir
            ),
        }
        Ok(())
    }
}

/// Result of a [`move_path`] call.
#[derive(Debug, PartialEq)]
enum MoveOutcome {
    /// The payload was moved to the destination.
    Moved,
    /// The payload was already at the destination and the source is gone —
    /// a previous cycle moved the files but failed before finishing.
    AlreadyMoved,
}

/// Moves `src` (file or directory) into `dest_dir`, preserving its name.
/// Tries a cheap rename first (same-filesystem move) and falls back to
/// copy + delete for cross-filesystem moves. Refuses to overwrite.
fn move_path(src: &Path, dest_dir: &Path) -> Result<MoveOutcome> {
    let file_name = src
        .file_name()
        .with_context(|| format!("Source path has no file name: {:?}", src))?;
    let dest = dest_dir.join(file_name);

    if !src.exists() {
        if dest.exists() {
            // Retry-safety: an earlier cycle completed the move but did not
            // get to remove the torrent from qBittorrent.
            return Ok(MoveOutcome::AlreadyMoved);
        }
        bail!("Source path does not exist: {:?}", src);
    }
    fs::create_dir_all(dest_dir)
        .with_context(|| format!("Failed to create destination directory {:?}", dest_dir))?;
    if dest.exists() {
        bail!(
            "Destination already exists, refusing to overwrite: {:?}",
            dest
        );
    }

    if fs::rename(src, &dest).is_ok() {
        return Ok(MoveOutcome::Moved);
    }

    if src.is_file() {
        fs::copy(src, &dest).with_context(|| format!("Failed to copy {:?} to {:?}", src, dest))?;
        fs::remove_file(src).with_context(|| format!("Failed to remove source file {:?}", src))?;
    } else if src.is_dir() {
        let mut options = fs_extra::dir::CopyOptions::new();
        // Copy the *contents* of src into dest (which already carries the
        // torrent's name); the default would nest them as dest/name/name.
        options.copy_inside = true;
        fs_extra::dir::copy(src, &dest, &options)
            .with_context(|| format!("Failed to copy {:?} to {:?}", src, dest))?;
        fs::remove_dir_all(src)
            .with_context(|| format!("Failed to remove source directory {:?}", src))?;
    } else {
        bail!("Source path is not a file or directory: {:?}", src);
    }
    Ok(MoveOutcome::Moved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::Server;

    fn bypass_auth_config(url: String) -> ServerConfig {
        // Empty username skips the login/logout flow.
        ServerConfig {
            qbit_url: url,
            username: String::new(),
            password: String::new(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_login_success() {
        let mut server = Server::new_async().await;
        let m = server
            .mock("POST", "/api/v2/auth/login")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("username".into(), "admin".into()),
                mockito::Matcher::UrlEncoded("password".into(), "adminadmin".into()),
            ]))
            .with_status(200)
            .with_body("Ok.")
            .create_async()
            .await;

        let client = TorrentClient::new(ServerConfig {
            qbit_url: server.url(),
            ..Default::default()
        })
        .unwrap();
        assert!(client.login().await.is_ok());
        m.assert_async().await;
    }

    #[tokio::test]
    async fn test_login_bad_credentials() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("POST", "/api/v2/auth/login")
            .with_status(200)
            .with_body("Fails.")
            .create_async()
            .await;

        let client = TorrentClient::new(ServerConfig {
            qbit_url: server.url(),
            ..Default::default()
        })
        .unwrap();
        let result = client.login().await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Authentication failed"));
    }

    #[tokio::test]
    async fn test_login_skipped_when_username_empty() {
        // No mock server interaction expected at all.
        let client =
            TorrentClient::new(bypass_auth_config("http://127.0.0.1:1".to_string())).unwrap();
        assert!(client.login().await.is_ok());
        assert!(client.logout().await.is_ok());
    }

    #[tokio::test]
    async fn test_get_torrents() {
        let mut server = Server::new_async().await;
        let m = server
            .mock("GET", "/api/v2/torrents/info")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"save_path":"/downloads","name":"ubuntu.iso","category":"distros","hash":"abc123","state":"uploading","content_path":"/downloads/ubuntu.iso"}]"#,
            )
            .create_async()
            .await;

        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        let torrents = client.get_torrents().await.unwrap();
        assert_eq!(torrents.len(), 1);
        assert_eq!(torrents[0].name, "ubuntu.iso");
        assert_eq!(torrents[0].state, TorrentState::Uploading);
        assert_eq!(
            torrents[0].content_path.as_deref(),
            Some("/downloads/ubuntu.iso")
        );
        m.assert_async().await;
    }

    #[tokio::test]
    async fn test_get_torrents_without_optional_fields() {
        // Older qBittorrent versions may not report content_path; an
        // unrecognized or missing state must parse as Unknown, not error.
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/api/v2/torrents/info")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"save_path":"/downloads","name":"ubuntu.iso","category":"distros","hash":"abc123"},
                    {"save_path":"/downloads","name":"other.iso","category":"distros","hash":"def456","state":"someFutureState"}]"#,
            )
            .create_async()
            .await;

        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        let torrents = client.get_torrents().await.unwrap();
        assert_eq!(torrents[0].content_path, None);
        assert_eq!(torrents[0].state, TorrentState::Unknown);
        assert_eq!(torrents[1].state, TorrentState::Unknown);
    }

    #[tokio::test]
    async fn test_http_error_status_is_reported() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/api/v2/torrents/info")
            .match_query(mockito::Matcher::Any)
            .with_status(403)
            .create_async()
            .await;

        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        assert!(client.get_torrents().await.is_err());
    }

    #[tokio::test]
    async fn test_remove_torrent() {
        let mut server = Server::new_async().await;
        let m = server
            .mock("POST", "/api/v2/torrents/delete")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("hashes".into(), "test_hash".into()),
                mockito::Matcher::UrlEncoded("deleteFiles".into(), "false".into()),
            ]))
            .with_status(200)
            .create_async()
            .await;

        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        assert!(client.remove_torrent("test_hash", false).await.is_ok());
        m.assert_async().await;
    }

    #[tokio::test]
    async fn test_stop_torrent_uses_stop_endpoint() {
        // qBittorrent 5.x path: torrents/stop exists.
        let mut server = Server::new_async().await;
        let stop_mock = server
            .mock("POST", "/api/v2/torrents/stop")
            .match_body(mockito::Matcher::UrlEncoded(
                "hashes".into(),
                "test_hash".into(),
            ))
            .with_status(200)
            .create_async()
            .await;

        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        assert!(client.stop_torrent("test_hash").await.is_ok());
        stop_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_stop_torrent_falls_back_to_pause() {
        // qBittorrent 4.x path: torrents/stop is 404, torrents/pause works.
        let mut server = Server::new_async().await;
        let stop_mock = server
            .mock("POST", "/api/v2/torrents/stop")
            .with_status(404)
            .create_async()
            .await;
        let pause_mock = server
            .mock("POST", "/api/v2/torrents/pause")
            .match_body(mockito::Matcher::UrlEncoded(
                "hashes".into(),
                "test_hash".into(),
            ))
            .with_status(200)
            .create_async()
            .await;

        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        assert!(client.stop_torrent("test_hash").await.is_ok());
        stop_mock.assert_async().await;
        pause_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_move_and_clean_torrent_files() -> Result<()> {
        let mut server = Server::new_async().await;
        let stop_mock = server
            .mock("POST", "/api/v2/torrents/stop")
            .with_status(200)
            .create_async()
            .await;
        let delete_mock = server
            .mock("POST", "/api/v2/torrents/delete")
            .with_status(200)
            .create_async()
            .await;

        let tmp_dir = tempfile::tempdir()?;
        let src_dir = tmp_dir.path().join("src");
        let dest_dir = tmp_dir.path().join("dest");
        fs::create_dir_all(&src_dir)?;

        let torrent = Torrent {
            save_path: src_dir.to_str().unwrap().to_string(),
            name: String::from("test_torrent"),
            category: String::from("test_category"),
            hash: String::from("test_hash"),
            state: TorrentState::StoppedUpload,
            content_path: None,
            ..Default::default()
        };

        let src_file = src_dir.join(&torrent.name);
        fs::File::create(&src_file)?;

        let mut server_config = bypass_auth_config(server.url());
        server_config.categories.insert(
            torrent.category.clone(),
            dest_dir.to_str().unwrap().to_string(),
        );
        let client = TorrentClient::new(server_config)?;

        client.move_and_clean_torrent_files(&torrent).await?;

        assert!(!src_file.exists());
        assert!(dest_dir.join(&torrent.name).exists());
        stop_mock.assert_async().await;
        delete_mock.assert_async().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_move_directory_is_not_nested() -> Result<()> {
        let mut server = Server::new_async().await;
        let _stop_mock = server
            .mock("POST", "/api/v2/torrents/stop")
            .with_status(200)
            .create_async()
            .await;
        let _delete_mock = server
            .mock("POST", "/api/v2/torrents/delete")
            .with_status(200)
            .create_async()
            .await;

        let tmp_dir = tempfile::tempdir()?;
        let src_dir = tmp_dir.path().join("src");
        let dest_dir = tmp_dir.path().join("dest");
        let torrent_dir = src_dir.join("season_pack");
        fs::create_dir_all(&torrent_dir)?;
        fs::File::create(torrent_dir.join("episode.mkv"))?;

        let torrent = Torrent {
            save_path: src_dir.to_str().unwrap().to_string(),
            name: String::from("season_pack"),
            category: String::from("tv"),
            hash: String::from("dir_hash"),
            state: TorrentState::Uploading,
            content_path: Some(torrent_dir.to_str().unwrap().to_string()),
            ..Default::default()
        };

        let mut server_config = bypass_auth_config(server.url());
        server_config
            .categories
            .insert("tv".to_string(), dest_dir.to_str().unwrap().to_string());
        let client = TorrentClient::new(server_config)?;

        client.move_and_clean_torrent_files(&torrent).await?;

        assert!(!torrent_dir.exists());
        assert!(dest_dir.join("season_pack").join("episode.mkv").exists());
        // The historical bug produced dest/season_pack/season_pack.
        assert!(!dest_dir.join("season_pack").join("season_pack").exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_unmapped_category_is_skipped() -> Result<()> {
        let client = TorrentClient::new(bypass_auth_config("http://127.0.0.1:1".to_string()))?;
        let torrent = Torrent {
            save_path: String::from("/nonexistent"),
            name: String::from("x"),
            category: String::from("unmapped"),
            hash: String::from("h"),
            state: TorrentState::Uploading,
            content_path: None,
            ..Default::default()
        };
        // No categories configured: must be a no-op, not an error.
        client.move_and_clean_torrent_files(&torrent).await?;
        Ok(())
    }

    #[test]
    fn test_resolve_source_path_with_prefix_mapping() -> Result<()> {
        let mut server_config = bypass_auth_config("http://127.0.0.1:1".to_string());
        server_config.path_prefix = Some("/downloads".to_string());
        server_config.root_path = Some("/mnt/seedbox".to_string());
        let client = TorrentClient::new(server_config)?;

        let torrent = Torrent {
            save_path: String::from("/downloads/movies"),
            name: String::from("film.mkv"),
            category: String::from("movies"),
            hash: String::from("h"),
            state: TorrentState::Uploading,
            content_path: Some(String::from("/downloads/movies/film.mkv")),
            ..Default::default()
        };
        let resolved = client.resolve_source_path(&torrent)?;
        assert_eq!(
            resolved,
            PathBuf::from("/mnt/seedbox")
                .join("movies")
                .join("film.mkv")
        );
        Ok(())
    }

    /// Regression test for the mixed Docker/host scenario: qBittorrent runs
    /// in a Linux container reporting paths like `/data/Anime/...`, while
    /// this tool runs on the Windows host where the bind mount lives at
    /// `G:\data\torrents`.
    #[test]
    fn test_resolve_source_path_docker_container_to_windows_host() -> Result<()> {
        let mut server_config = bypass_auth_config("http://127.0.0.1:1".to_string());
        server_config.path_prefix = Some("/data".to_string());
        server_config.root_path = Some(r"G:\data\torrents".to_string());
        let client = TorrentClient::new(server_config)?;

        let torrent = Torrent {
            save_path: String::from("/data/Anime"),
            name: String::from("Some Show S01"),
            category: String::from("anime"),
            hash: String::from("h"),
            state: TorrentState::Uploading,
            content_path: Some(String::from("/data/Anime/Some Show S01")),
            ..Default::default()
        };
        let resolved = client.resolve_source_path(&torrent)?;
        assert_eq!(
            resolved,
            PathBuf::from(r"G:\data\torrents")
                .join("Anime")
                .join("Some Show S01")
        );

        // Same mapping must hold when content_path is unavailable and the
        // path is derived from save_path + name instead.
        let torrent_no_content_path = Torrent {
            content_path: None,
            ..torrent
        };
        let resolved = client.resolve_source_path(&torrent_no_content_path)?;
        assert_eq!(
            resolved,
            PathBuf::from(r"G:\data\torrents")
                .join("Anime")
                .join("Some Show S01")
        );
        Ok(())
    }

    /// A trailing slash on `path_prefix` must not break the mapping.
    #[test]
    fn test_resolve_source_path_prefix_trailing_slash() -> Result<()> {
        let mut server_config = bypass_auth_config("http://127.0.0.1:1".to_string());
        server_config.path_prefix = Some("/data/".to_string());
        server_config.root_path = Some(r"G:\data\torrents".to_string());
        let client = TorrentClient::new(server_config)?;

        let torrent = Torrent {
            save_path: String::from("/data/Anime"),
            name: String::from("x"),
            category: String::from("anime"),
            hash: String::from("h"),
            state: TorrentState::Uploading,
            content_path: Some(String::from("/data/Anime/x")),
            ..Default::default()
        };
        let resolved = client.resolve_source_path(&torrent)?;
        assert_eq!(
            resolved,
            PathBuf::from(r"G:\data\torrents").join("Anime").join("x")
        );
        Ok(())
    }

    /// If a previous cycle moved the files but failed to remove the torrent,
    /// the next cycle must finish the job instead of erroring forever.
    #[tokio::test]
    async fn test_already_moved_torrent_is_cleaned_up() -> Result<()> {
        let mut server = Server::new_async().await;
        let _stop_mock = server
            .mock("POST", "/api/v2/torrents/stop")
            .with_status(200)
            .create_async()
            .await;
        let delete_mock = server
            .mock("POST", "/api/v2/torrents/delete")
            .with_status(200)
            .expect(1)
            .create_async()
            .await;

        let tmp_dir = tempfile::tempdir()?;
        let src_dir = tmp_dir.path().join("src");
        let dest_dir = tmp_dir.path().join("dest");
        fs::create_dir_all(&src_dir)?;
        fs::create_dir_all(&dest_dir)?;
        // The payload is already at the destination; the source is gone.
        fs::File::create(dest_dir.join("test_torrent"))?;

        let torrent = Torrent {
            save_path: src_dir.to_str().unwrap().to_string(),
            name: String::from("test_torrent"),
            category: String::from("test_category"),
            hash: String::from("test_hash"),
            state: TorrentState::StalledUpload,
            content_path: None,
            ..Default::default()
        };

        let mut server_config = bypass_auth_config(server.url());
        server_config.categories.insert(
            torrent.category.clone(),
            dest_dir.to_str().unwrap().to_string(),
        );
        let client = TorrentClient::new(server_config)?;

        client.move_and_clean_torrent_files(&torrent).await?;
        delete_mock.assert_async().await;
        Ok(())
    }

    #[test]
    fn test_move_path_reports_already_moved() -> Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let src = tmp_dir.path().join("gone").join("file.bin");
        let dest_dir = tmp_dir.path().join("dest");
        fs::create_dir_all(&dest_dir)?;
        fs::File::create(dest_dir.join("file.bin"))?;

        assert_eq!(move_path(&src, &dest_dir)?, MoveOutcome::AlreadyMoved);
        Ok(())
    }

    #[test]
    fn test_state_deserialization_covers_both_api_generations() {
        // qBittorrent 5.x spelling.
        let t: TorrentState = serde_json::from_str(r#""stoppedUP""#).unwrap();
        assert_eq!(t, TorrentState::StoppedUpload);
        // qBittorrent 4.x spelling maps to the same variant.
        let t: TorrentState = serde_json::from_str(r#""pausedUP""#).unwrap();
        assert_eq!(t, TorrentState::StoppedUpload);
        let t: TorrentState = serde_json::from_str(r#""pausedDL""#).unwrap();
        assert_eq!(t, TorrentState::StoppedDownload);
        let t: TorrentState = serde_json::from_str(r#""moving""#).unwrap();
        assert_eq!(t, TorrentState::Moving);
        // Unrecognized states must not fail deserialization.
        let t: TorrentState = serde_json::from_str(r#""brandNewState""#).unwrap();
        assert_eq!(t, TorrentState::Unknown);
    }

    #[test]
    fn test_state_classification() {
        // Download finished, safe to move.
        for state in [
            TorrentState::Uploading,
            TorrentState::StoppedUpload,
            TorrentState::QueuedUpload,
            TorrentState::StalledUpload,
            TorrentState::ForcedUpload,
        ] {
            assert!(state.download_finished(), "{state} should be finished");
            assert!(state.safe_to_move(), "{state} should be safe to move");
        }
        // Download finished, but NOT safe to touch the payload.
        for state in [TorrentState::CheckingUpload, TorrentState::Moving] {
            assert!(state.download_finished(), "{state} should be finished");
            assert!(!state.safe_to_move(), "{state} must not be safe to move");
        }
        // Not finished and never safe.
        for state in [
            TorrentState::Downloading,
            TorrentState::QueuedDownload,
            TorrentState::StalledDownload,
            TorrentState::StoppedDownload,
            TorrentState::CheckingDownload,
            TorrentState::ForcedDownload,
            TorrentState::FetchingMetadata,
            TorrentState::Allocating,
            TorrentState::CheckingResumeData,
            TorrentState::Error,
            TorrentState::MissingFiles,
            TorrentState::Unknown,
        ] {
            assert!(!state.safe_to_move(), "{state} must not be safe to move");
        }
        assert!(TorrentState::Error.is_errored());
        assert!(TorrentState::MissingFiles.is_errored());
        assert!(!TorrentState::Uploading.is_errored());
    }

    /// Torrents in unsafe states (qBittorrent relocating them, rechecking,
    /// or errored) must not be touched even when their category is mapped
    /// and the source exists.
    #[tokio::test]
    async fn test_unsafe_states_are_not_moved() -> Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let src_dir = tmp_dir.path().join("src");
        let dest_dir = tmp_dir.path().join("dest");
        fs::create_dir_all(&src_dir)?;
        let src_file = src_dir.join("test_torrent");
        fs::File::create(&src_file)?;

        // Any API call would fail (no server); the point is none is made.
        let mut server_config = bypass_auth_config("http://127.0.0.1:1".to_string());
        server_config.categories.insert(
            "test_category".to_string(),
            dest_dir.to_str().unwrap().to_string(),
        );
        let client = TorrentClient::new(server_config)?;

        for state in [
            TorrentState::Moving,
            TorrentState::CheckingUpload,
            TorrentState::Error,
            TorrentState::MissingFiles,
            TorrentState::Downloading,
            TorrentState::Unknown,
        ] {
            let torrent = Torrent {
                save_path: src_dir.to_str().unwrap().to_string(),
                name: String::from("test_torrent"),
                category: String::from("test_category"),
                hash: String::from("test_hash"),
                state,
                content_path: None,
                ..Default::default()
            };
            client.move_and_clean_torrent_files(&torrent).await?;
            assert!(src_file.exists(), "{state}: payload must be untouched");
            assert!(
                !dest_dir.join("test_torrent").exists(),
                "{state}: nothing must be moved"
            );
        }
        Ok(())
    }

    #[test]
    fn test_state_tracker_detects_transitions() {
        let make = |hash: &str, state| Torrent {
            save_path: String::from("/data"),
            name: format!("torrent_{hash}"),
            category: String::from("anime"),
            hash: hash.to_string(),
            state,
            content_path: None,
            ..Default::default()
        };

        let mut tracker = StateTracker::default();

        // Cycle 1: one downloading torrent.
        tracker.observe(&[make("a", TorrentState::Downloading)]);
        assert_eq!(tracker.states.get("a"), Some(&TorrentState::Downloading));

        // Cycle 2: it finished and started seeding; a new one appeared.
        tracker.observe(&[
            make("a", TorrentState::Uploading),
            make("b", TorrentState::QueuedDownload),
        ]);
        assert_eq!(tracker.states.get("a"), Some(&TorrentState::Uploading));
        assert_eq!(tracker.states.get("b"), Some(&TorrentState::QueuedDownload));

        // Cycle 3: "a" was removed (moved out); "b" errored.
        tracker.observe(&[make("b", TorrentState::Error)]);
        assert_eq!(tracker.states.get("a"), None, "removed torrents are pruned");
        assert_eq!(tracker.states.get("b"), Some(&TorrentState::Error));
    }

    #[test]
    fn test_move_path_refuses_overwrite() -> Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let src = tmp_dir.path().join("file.bin");
        let dest_dir = tmp_dir.path().join("dest");
        fs::create_dir_all(&dest_dir)?;
        fs::File::create(&src)?;
        fs::File::create(dest_dir.join("file.bin"))?;

        let result = move_path(&src, &dest_dir);
        assert!(result.is_err());
        assert!(src.exists(), "source must be left intact");
        Ok(())
    }

    /// The torrent list must be fetched page by page so a client with
    /// thousands of torrents cannot produce one enormous response.
    #[tokio::test]
    async fn test_get_torrents_paginates() {
        let mut server = Server::new_async().await;

        // First page: exactly TORRENT_PAGE_SIZE entries -> a next page is
        // requested. Second page: a short page -> fetching stops.
        let full_page: Vec<serde_json::Value> = (0..TORRENT_PAGE_SIZE)
            .map(|i| {
                serde_json::json!({
                    "save_path": "/downloads",
                    "name": format!("t{i}"),
                    "category": "c",
                    "hash": format!("h{i}"),
                    "state": "uploading"
                })
            })
            .collect();
        let page1 = server
            .mock("GET", "/api/v2/torrents/info")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("limit".into(), TORRENT_PAGE_SIZE.to_string()),
                mockito::Matcher::UrlEncoded("offset".into(), "0".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&full_page).unwrap())
            .expect(1)
            .create_async()
            .await;
        let page2 = server
            .mock("GET", "/api/v2/torrents/info")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("limit".into(), TORRENT_PAGE_SIZE.to_string()),
                mockito::Matcher::UrlEncoded("offset".into(), TORRENT_PAGE_SIZE.to_string()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"[{"save_path":"/downloads","name":"last","category":"c","hash":"hl","state":"uploading"}]"#)
            .expect(1)
            .create_async()
            .await;

        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        let torrents = client.get_torrents().await.unwrap();
        assert_eq!(torrents.len(), TORRENT_PAGE_SIZE + 1);
        assert_eq!(torrents.last().unwrap().name, "last");
        page1.assert_async().await;
        page2.assert_async().await;
    }

    /// Transfer data (progress/amount_left/completion_on) disambiguates
    /// states that occur both before and after a download completes.
    #[test]
    fn test_lifecycle_disambiguation() {
        let t = |state, progress: f64, amount_left: i64, completion_on: i64| Torrent {
            state,
            progress,
            amount_left,
            completion_on,
            ..Default::default()
        };

        // Errored before/during download: incomplete, not movable.
        assert!(!t(TorrentState::Error, 0.0, 1000, 0).is_download_complete());
        assert!(!t(TorrentState::Error, 0.42, 500, 0).is_download_complete());
        // Errored after a completed download (e.g. missingFiles after an
        // external move): complete, but still never movable.
        let errored_done = t(TorrentState::MissingFiles, 1.0, 0, 1_700_000_000);
        assert!(errored_done.is_download_complete());
        assert!(!errored_done.eligible_for_move());

        // Stopped before completion vs stopped after completion.
        assert!(!t(TorrentState::StoppedDownload, 0.6, 400, 0).eligible_for_move());
        assert!(t(TorrentState::StoppedDownload, 1.0, 0, 1_700_000_000).eligible_for_move());

        // Queued before completion: untouched. Queued after: movable.
        assert!(!t(TorrentState::QueuedDownload, 0.0, 1000, 0).eligible_for_move());
        assert!(t(TorrentState::QueuedDownload, 1.0, 0, 1_700_000_000).eligible_for_move());
        assert!(t(TorrentState::QueuedUpload, 1.0, 0, 1_700_000_000).eligible_for_move());

        // amount_left == 0 alone (e.g. metadata not yet fetched) must not
        // count as complete without a completion timestamp.
        assert!(!t(TorrentState::FetchingMetadata, 0.0, 0, 0).is_download_complete());

        // Upload-side states imply completion even without transfer data.
        assert!(t(TorrentState::Uploading, 0.0, 0, 0).is_download_complete());

        // Unsafe transitional states stay unmovable regardless of data.
        assert!(!t(TorrentState::Moving, 1.0, 0, 1_700_000_000).eligible_for_move());
        assert!(!t(TorrentState::CheckingUpload, 1.0, 0, 1_700_000_000).eligible_for_move());
    }

    #[test]
    fn test_recovery_backoff_schedule() {
        assert_eq!(recovery_backoff_delay(1), Duration::from_secs(60));
        assert_eq!(recovery_backoff_delay(2), Duration::from_secs(120));
        assert_eq!(recovery_backoff_delay(3), Duration::from_secs(240));
        // Capped at one hour, even for absurd attempt counts.
        assert_eq!(recovery_backoff_delay(10), Duration::from_secs(3600));
        assert_eq!(recovery_backoff_delay(u32::MAX), Duration::from_secs(3600));
    }

    #[test]
    fn test_state_tracker_recovery_backoff() {
        let mut tracker = StateTracker::default();
        let now = Instant::now();

        // Never attempted: due immediately.
        assert!(tracker.recovery_due("h", now));
        assert_eq!(tracker.record_recovery_attempt("h", now), 1);
        // Right after the first attempt: not due again yet.
        assert!(!tracker.recovery_due("h", now));
        assert!(!tracker.recovery_due("h", now + Duration::from_secs(59)));
        // After the first backoff delay: due again.
        assert!(tracker.recovery_due("h", now + Duration::from_secs(60)));
        assert_eq!(
            tracker.record_recovery_attempt("h", now + Duration::from_secs(60)),
            2
        );
        // Second delay is doubled.
        assert!(!tracker.recovery_due("h", now + Duration::from_secs(60 + 119)));
        assert!(tracker.recovery_due("h", now + Duration::from_secs(60 + 120)));

        // Other torrents are unaffected.
        assert!(tracker.recovery_due("other", now));
    }

    /// Retry bookkeeping must be dropped once a torrent recovers (or is
    /// removed), so a later unrelated error starts a fresh backoff.
    #[test]
    fn test_state_tracker_prunes_recovered_retries() {
        let make = |hash: &str, state| Torrent {
            hash: hash.to_string(),
            name: format!("torrent_{hash}"),
            state,
            ..Default::default()
        };
        let mut tracker = StateTracker::default();
        let now = Instant::now();

        tracker.observe(&[make("a", TorrentState::Error)]);
        tracker.record_recovery_attempt("a", now);
        assert!(!tracker.recovery_due("a", now));

        // The torrent recovered: its backoff schedule is forgotten.
        tracker.observe(&[make("a", TorrentState::Downloading)]);
        assert!(tracker.recovery_due("a", now));
    }

    #[tokio::test]
    async fn test_start_torrent_falls_back_to_resume() {
        // qBittorrent 4.x path: torrents/start is 404, torrents/resume works.
        let mut server = Server::new_async().await;
        let start_mock = server
            .mock("POST", "/api/v2/torrents/start")
            .with_status(404)
            .create_async()
            .await;
        let resume_mock = server
            .mock("POST", "/api/v2/torrents/resume")
            .match_body(mockito::Matcher::UrlEncoded(
                "hashes".into(),
                "test_hash".into(),
            ))
            .with_status(200)
            .create_async()
            .await;

        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        assert!(client.start_torrent("test_hash").await.is_ok());
        start_mock.assert_async().await;
        resume_mock.assert_async().await;
    }

    /// A torrent that errors before its download completes gets a
    /// recheck+start recovery attempt, and immediate re-observations are
    /// suppressed by the backoff schedule.
    #[tokio::test]
    async fn test_errored_incomplete_torrent_is_recovered_with_backoff() {
        let mut server = Server::new_async().await;
        let recheck_mock = server
            .mock("POST", "/api/v2/torrents/recheck")
            .match_body(mockito::Matcher::UrlEncoded("hashes".into(), "eh".into()))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;
        let start_mock = server
            .mock("POST", "/api/v2/torrents/start")
            .match_body(mockito::Matcher::UrlEncoded("hashes".into(), "eh".into()))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;
        let delete_mock = server
            .mock("POST", "/api/v2/torrents/delete")
            .expect(0)
            .create_async()
            .await;

        let torrent = Torrent {
            name: String::from("broken"),
            category: String::from("anime"),
            hash: String::from("eh"),
            state: TorrentState::Error,
            progress: 0.3,
            amount_left: 700,
            ..Default::default()
        };
        let mut server_config = bypass_auth_config(server.url());
        server_config
            .categories
            .insert("anime".to_string(), "/dest".to_string());
        let client = TorrentClient::new(server_config).unwrap();
        let mut tracker = StateTracker::default();

        client
            .handle_errored_torrent(&torrent, &mut tracker, ErroredCompletedAction::Remove)
            .await;
        // Second observation in quick succession: backoff suppresses it,
        // so the recheck/start mocks stay at exactly one hit each.
        client
            .handle_errored_torrent(&torrent, &mut tracker, ErroredCompletedAction::Remove)
            .await;

        recheck_mock.assert_async().await;
        start_mock.assert_async().await;
        delete_mock.assert_async().await;
    }

    /// A torrent that errored *after* completing its download is handled
    /// according to the configured action.
    #[tokio::test]
    async fn test_errored_completed_torrent_actions() {
        let torrent = Torrent {
            name: String::from("gone"),
            category: String::from("anime"),
            hash: String::from("ch"),
            state: TorrentState::MissingFiles,
            progress: 1.0,
            completion_on: 1_700_000_000,
            ..Default::default()
        };

        // remove: delete the torrent entry, keep files.
        let mut server = Server::new_async().await;
        let delete_mock = server
            .mock("POST", "/api/v2/torrents/delete")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("hashes".into(), "ch".into()),
                mockito::Matcher::UrlEncoded("deleteFiles".into(), "false".into()),
            ]))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;
        let mut server_config = bypass_auth_config(server.url());
        server_config
            .categories
            .insert("anime".to_string(), "/dest".to_string());
        let client = TorrentClient::new(server_config).unwrap();
        let mut tracker = StateTracker::default();
        client
            .handle_errored_torrent(&torrent, &mut tracker, ErroredCompletedAction::Remove)
            .await;
        delete_mock.assert_async().await;

        // remove_with_data: qBittorrent deletes the payload too (honoring
        // its own trash preference).
        let mut server = Server::new_async().await;
        let delete_mock = server
            .mock("POST", "/api/v2/torrents/delete")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("hashes".into(), "ch".into()),
                mockito::Matcher::UrlEncoded("deleteFiles".into(), "true".into()),
            ]))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;
        let mut server_config = bypass_auth_config(server.url());
        server_config
            .categories
            .insert("anime".to_string(), "/dest".to_string());
        let client = TorrentClient::new(server_config).unwrap();
        client
            .handle_errored_torrent(
                &torrent,
                &mut tracker,
                ErroredCompletedAction::RemoveWithData,
            )
            .await;
        delete_mock.assert_async().await;

        // keep: no API call at all (unreachable server would fail loudly).
        let mut server_config = bypass_auth_config("http://127.0.0.1:1".to_string());
        server_config
            .categories
            .insert("anime".to_string(), "/dest".to_string());
        let client = TorrentClient::new(server_config).unwrap();
        client
            .handle_errored_torrent(&torrent, &mut tracker, ErroredCompletedAction::Keep)
            .await;

        // Unmapped category: never touched, regardless of action.
        let client =
            TorrentClient::new(bypass_auth_config("http://127.0.0.1:1".to_string())).unwrap();
        client
            .handle_errored_torrent(
                &torrent,
                &mut tracker,
                ErroredCompletedAction::RemoveWithData,
            )
            .await;
    }

    #[test]
    fn test_move_path_handles_unicode_media_names() {
        // Names representative of TV / movie / music / anime releases,
        // including non-ASCII characters and awkward punctuation.
        let names = [
            "Some.Show.S01E01.1080p.WEB-DL",
            "A Movie (2024) [Blu-ray]",
            "Artist – Álbum Déluxe (FLAC)",
            "アニメ作品 第01話 「はじまり」",
        ];
        let temp = tempfile::tempdir().unwrap();
        for name in names {
            let src_dir = temp.path().join("src");
            let dest_dir = temp.path().join("dest");
            fs::create_dir_all(&src_dir).unwrap();
            let src = src_dir.join(name);
            fs::write(&src, b"payload").unwrap();
            let outcome = move_path(&src, &dest_dir).unwrap();
            assert_eq!(outcome, MoveOutcome::Moved);
            assert!(
                dest_dir.join(name).exists(),
                "missing moved file for {name}"
            );
            assert!(!src.exists());
        }
    }

    #[test]
    fn test_tag_list_parses_comma_separated_tags() {
        let mut torrent = Torrent::default();
        assert!(torrent.tag_list().is_empty());
        torrent.tags = String::from("music, flac ,processed");
        assert_eq!(torrent.tag_list(), vec!["music", "flac", "processed"]);
        torrent.tags = String::from(" , ");
        assert!(torrent.tag_list().is_empty());
    }

    #[tokio::test]
    async fn test_set_category() {
        let mut server = Server::new_async().await;
        let m = server
            .mock("POST", "/api/v2/torrents/setCategory")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("hashes".into(), "h1".into()),
                mockito::Matcher::UrlEncoded("category".into(), "Seeding".into()),
            ]))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;
        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        assert!(client.set_category("h1", "Seeding", false).await.is_ok());
        m.assert_async().await;
    }

    #[tokio::test]
    async fn test_set_category_creates_missing_category() {
        // qBittorrent answers 409 when the category doesn't exist; with
        // create_if_missing the category must be created before retrying.
        let mut server = Server::new_async().await;
        let set_mock = server
            .mock("POST", "/api/v2/torrents/setCategory")
            .with_status(409)
            .expect_at_least(1)
            .create_async()
            .await;
        let create_mock = server
            .mock("POST", "/api/v2/torrents/createCategory")
            .match_body(mockito::Matcher::UrlEncoded(
                "category".into(),
                "Seeding".into(),
            ))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;

        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        // The mock keeps answering 409 on the retry, so the overall call
        // fails — but the create branch must have fired exactly once.
        let result = client.set_category("h1", "Seeding", true).await;
        assert!(result.is_err());
        create_mock.assert_async().await;
        set_mock.assert_async().await;

        // Without create_if_missing, a 409 is a plain error and the
        // category is never created.
        let mut server = Server::new_async().await;
        let _set = server
            .mock("POST", "/api/v2/torrents/setCategory")
            .with_status(409)
            .create_async()
            .await;
        let create_mock = server
            .mock("POST", "/api/v2/torrents/createCategory")
            .expect(0)
            .create_async()
            .await;
        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        assert!(client.set_category("h1", "Seeding", false).await.is_err());
        create_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_create_category_conflict_is_ok() {
        // 409 means the category already exists; that's success here.
        let mut server = Server::new_async().await;
        let _m = server
            .mock("POST", "/api/v2/torrents/createCategory")
            .with_status(409)
            .create_async()
            .await;
        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        assert!(client.create_category("Seeding").await.is_ok());
    }

    #[tokio::test]
    async fn test_add_and_remove_tags() {
        let mut server = Server::new_async().await;
        let add_mock = server
            .mock("POST", "/api/v2/torrents/addTags")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("hashes".into(), "h1".into()),
                mockito::Matcher::UrlEncoded("tags".into(), "a,b".into()),
            ]))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;
        let remove_mock = server
            .mock("POST", "/api/v2/torrents/removeTags")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("hashes".into(), "h1".into()),
                mockito::Matcher::UrlEncoded("tags".into(), "a,b".into()),
            ]))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;
        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        assert!(client.add_tags("h1", "a,b").await.is_ok());
        assert!(client.remove_tags("h1", "a,b").await.is_ok());
        add_mock.assert_async().await;
        remove_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_clear_tags_passes_current_tags_and_skips_untagged() {
        let mut server = Server::new_async().await;
        let remove_mock = server
            .mock("POST", "/api/v2/torrents/removeTags")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("hashes".into(), "h1".into()),
                mockito::Matcher::UrlEncoded("tags".into(), "x,y".into()),
            ]))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;
        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();

        let mut torrent = Torrent {
            hash: String::from("h1"),
            tags: String::from("x, y"),
            ..Default::default()
        };
        assert!(client.clear_tags(&torrent).await.is_ok());
        remove_mock.assert_async().await;

        // No tags: no API call (an unreachable server would error).
        torrent.tags = String::new();
        let offline = TorrentClient::new(bypass_auth_config("http://127.0.0.1:1".into())).unwrap();
        assert!(offline.clear_tags(&torrent).await.is_ok());
    }

    #[tokio::test]
    async fn test_delete_to_trash_with_missing_payload_removes_entry() {
        // The payload is already gone (deleted manually or by an earlier
        // cycle): the torrent entry must still be cleaned up, without
        // asking qBittorrent to delete files.
        let mut server = Server::new_async().await;
        let stop_mock = server
            .mock("POST", "/api/v2/torrents/stop")
            .with_status(200)
            .create_async()
            .await;
        let delete_mock = server
            .mock("POST", "/api/v2/torrents/delete")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("hashes".into(), "h1".into()),
                mockito::Matcher::UrlEncoded("deleteFiles".into(), "false".into()),
            ]))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;

        let temp = tempfile::tempdir().unwrap();
        let torrent = Torrent {
            hash: String::from("h1"),
            name: String::from("gone_torrent"),
            save_path: temp.path().to_string_lossy().into_owned(),
            state: TorrentState::StoppedUpload,
            progress: 1.0,
            ..Default::default()
        };
        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        assert!(client.delete_torrent_to_trash(&torrent).await.is_ok());
        stop_mock.assert_async().await;
        delete_mock.assert_async().await;
    }
}
