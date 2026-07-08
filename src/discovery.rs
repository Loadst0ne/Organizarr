/*
Organizarr - A qBittorrent companion that organizes torrents with state-aware rules, complementing the *arr suite.
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

//! Opt-in discovery of directory configuration that already exists in the
//! surrounding ecosystem, so users with mixed environments (e.g. qBittorrent
//! in a Docker container, this tool on the host) don't have to duplicate
//! path translation tables they have already maintained elsewhere.
//!
//! What is imported, and why only this:
//! - **\*arr remote path mappings** (Sonarr/Radarr via `/api/v3`,
//!   Lidarr/Readarr via `/api/v1`): these are exactly the
//!   "path-as-the-download-client-reports-it" -> "path-as-this-host-sees-it"
//!   pairs this tool needs for its own translation. Detection is *verified*:
//!   a mapping whose local side does not exist on this host is skipped, so a
//!   mapping meant for a different machine can never mis-route a move.
//! - **qBittorrent category save paths** are fetched in `torrent.rs` (they
//!   need the authenticated WebUI session) and power `auto` destinations.
//!
//! Deliberately *not* detected, after assessing each app: unpackerr
//! (file-based config only, no API to read), Overseerr/Jellyseerr/requestrr
//! (request layers with no filesystem paths), slskd/Nicotine+ (separate
//! download pipelines whose paths never appear in qBittorrent). Importing
//! nothing from these is the safe choice.
//!
//! Precedence: imported values are re-fetched on an interval (default 60s),
//! so the *latest* change made inside the client application always wins
//! over anything previously imported. On fetch failure the last good value
//! is reused (stale-ok) so a briefly unreachable *arr instance cannot stall
//! the pipeline.

use crate::config::ArrConfig;
use anyhow::{bail, Context, Result};
use log::{info, warn};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

/// One "remote view" -> "local view" directory translation. `remote` is a
/// path as qBittorrent reports it (e.g. `/data/torrents` inside a
/// container); `local` is the same directory as this host sees it (e.g.
/// `G:\data\torrents`).
#[derive(Debug, Clone, PartialEq)]
pub struct PathMapping {
    pub remote: String,
    pub local: String,
}

/// A remote path mapping as returned by the *arr `remotepathmapping` API.
#[derive(Debug, Deserialize)]
struct ArrRemotePathMapping {
    #[serde(default)]
    host: String,
    #[serde(rename = "remotePath")]
    remote_path: String,
    #[serde(rename = "localPath")]
    local_path: String,
}

/// Fetches the remote path mappings from one *arr instance. Tries the v3
/// API (Sonarr, Radarr) first and falls back to v1 (Lidarr, Readarr) on
/// 404, so no per-app `kind` configuration is needed.
pub async fn fetch_arr_mappings(client: &Client, arr: &ArrConfig) -> Result<Vec<PathMapping>> {
    let base = arr.url.trim_end_matches('/');
    let mut mappings = None;
    for api in ["v3", "v1"] {
        let url = format!("{}/api/{}/remotepathmapping", base, api);
        let response = client
            .get(&url)
            .header("X-Api-Key", &arr.api_key)
            .send()
            .await
            .with_context(|| format!("Request to {} failed", url))?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            // This API version doesn't exist on this app; try the next.
            continue;
        }
        let response = response
            .error_for_status()
            .with_context(|| format!("Request to {} returned an error status", url))?;
        mappings = Some(
            response
                .json::<Vec<ArrRemotePathMapping>>()
                .await
                .with_context(|| format!("Failed to parse remote path mappings from {}", url))?,
        );
        break;
    }
    let Some(mappings) = mappings else {
        bail!(
            "{}: no remotepathmapping endpoint found (tried /api/v3 and /api/v1)",
            arr.label()
        );
    };

    let mut verified = Vec::new();
    for m in mappings {
        // Optional download-client host filter, for *arr instances that
        // manage several download clients.
        if let Some(host) = &arr.host {
            if !m.host.eq_ignore_ascii_case(host) {
                continue;
            }
        }
        // Verification gate: only adopt mappings whose local side actually
        // exists on *this* host. A mapping meant for another machine (or a
        // container-internal path) is skipped instead of silently
        // mis-routing file moves.
        if !Path::new(&m.local_path).is_dir() {
            warn!(
                "{}: skipping remote path mapping {:?} -> {:?}: local path does not exist on this host",
                arr.label(),
                m.remote_path,
                m.local_path
            );
            continue;
        }
        verified.push(PathMapping {
            remote: m.remote_path,
            local: m.local_path,
        });
    }
    Ok(verified)
}

/// Cached mapping import state for one *arr instance.
#[derive(Debug, Clone)]
struct CachedMappings {
    fetched_at: Instant,
    mappings: Vec<PathMapping>,
}

/// Per-server cache of imported *arr path mappings, persisted across
/// polling cycles. Re-fetches each instance on its configured `refresh`
/// interval so client-side changes take precedence; keeps serving the last
/// good value when an instance is temporarily unreachable.
#[derive(Debug, Default, Clone)]
pub struct DiscoveryCache {
    arr: HashMap<String, CachedMappings>,
}

impl DiscoveryCache {
    /// Returns the current set of imported mappings for `arrs`, refreshing
    /// any instance whose cache entry is older than its refresh interval.
    pub async fn arr_mappings(&mut self, client: &Client, arrs: &[ArrConfig]) -> Vec<PathMapping> {
        let mut all = Vec::new();
        for arr in arrs {
            let key = arr.url.clone();
            let stale = self
                .arr
                .get(&key)
                .is_none_or(|c| c.fetched_at.elapsed() >= arr.refresh_interval());
            if stale {
                match fetch_arr_mappings(client, arr).await {
                    Ok(mappings) => {
                        let changed = self.arr.get(&key).is_none_or(|c| c.mappings != mappings);
                        if changed {
                            info!(
                                "{}: imported {} verified remote path mapping(s)",
                                arr.label(),
                                mappings.len()
                            );
                        }
                        self.arr.insert(
                            key.clone(),
                            CachedMappings {
                                fetched_at: Instant::now(),
                                mappings,
                            },
                        );
                    }
                    Err(e) => {
                        // Stale-ok: reuse the last good import (if any) and
                        // retry on the next cycle rather than waiting a full
                        // refresh interval.
                        warn!(
                            "{}: could not refresh remote path mappings (reusing last import): {:#}",
                            arr.label(),
                            e
                        );
                    }
                }
            }
            if let Some(cached) = self.arr.get(&key) {
                all.extend(cached.mappings.iter().cloned());
            }
        }
        all
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::Server;
    use tempfile::tempdir;

    fn arr(url: &str) -> ArrConfig {
        ArrConfig {
            name: String::from("test-arr"),
            url: url.to_string(),
            api_key: String::from("key"),
            host: None,
            refresh: String::from("60s"),
        }
    }

    /// Sonarr/Radarr-style v3 endpoint: mappings are parsed and verified
    /// against the local filesystem; nonexistent local paths are skipped.
    #[tokio::test]
    async fn test_fetch_v3_mappings_verifies_local_paths() -> Result<()> {
        let dir = tempdir()?;
        let local = dir.path().to_string_lossy().to_string();
        let mut server = Server::new_async().await;
        let body = serde_json::json!([
            {"id": 1, "host": "gluetun", "remotePath": "/data/torrents", "localPath": local},
            {"id": 2, "host": "gluetun", "remotePath": "/other", "localPath": "/definitely/not/here"}
        ])
        .to_string();
        let mock = server
            .mock("GET", "/api/v3/remotepathmapping")
            .match_header("X-Api-Key", "key")
            .with_status(200)
            .with_body(&body)
            .create_async()
            .await;

        let client = Client::new();
        let mappings = fetch_arr_mappings(&client, &arr(&server.url())).await?;
        mock.assert_async().await;
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].remote, "/data/torrents");
        assert_eq!(mappings[0].local, local);
        Ok(())
    }

    /// Lidarr/Readarr expose the same resource under /api/v1; a 404 on v3
    /// must fall through to it.
    #[tokio::test]
    async fn test_fetch_falls_back_to_v1() -> Result<()> {
        let dir = tempdir()?;
        let local = dir.path().to_string_lossy().to_string();
        let mut server = Server::new_async().await;
        let v3 = server
            .mock("GET", "/api/v3/remotepathmapping")
            .with_status(404)
            .create_async()
            .await;
        let body = serde_json::json!([
            {"id": 1, "host": "qbit", "remotePath": "/music", "localPath": local}
        ])
        .to_string();
        let v1 = server
            .mock("GET", "/api/v1/remotepathmapping")
            .match_header("X-Api-Key", "key")
            .with_status(200)
            .with_body(&body)
            .create_async()
            .await;

        let client = Client::new();
        let mappings = fetch_arr_mappings(&client, &arr(&server.url())).await?;
        v3.assert_async().await;
        v1.assert_async().await;
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].remote, "/music");
        Ok(())
    }

    /// The optional `host` filter keeps mappings meant for other download
    /// clients out of the translation table.
    #[tokio::test]
    async fn test_host_filter() -> Result<()> {
        let dir = tempdir()?;
        let local = dir.path().to_string_lossy().to_string();
        let mut server = Server::new_async().await;
        let body = serde_json::json!([
            {"id": 1, "host": "gluetun", "remotePath": "/data/torrents", "localPath": local},
            {"id": 2, "host": "sabnzbd", "remotePath": "/usenet", "localPath": local}
        ])
        .to_string();
        server
            .mock("GET", "/api/v3/remotepathmapping")
            .with_status(200)
            .with_body(&body)
            .create_async()
            .await;

        let mut cfg = arr(&server.url());
        cfg.host = Some(String::from("Gluetun")); // case-insensitive
        let client = Client::new();
        let mappings = fetch_arr_mappings(&client, &cfg).await?;
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].remote, "/data/torrents");
        Ok(())
    }

    /// Within the refresh interval the cache must not hit the *arr API
    /// again; after a failure the last good import is reused.
    #[tokio::test]
    async fn test_cache_refresh_and_stale_reuse() -> Result<()> {
        let dir = tempdir()?;
        let local = dir.path().to_string_lossy().to_string();
        let mut server = Server::new_async().await;
        let body = serde_json::json!([
            {"id": 1, "host": "qbit", "remotePath": "/data", "localPath": local}
        ])
        .to_string();
        let mock = server
            .mock("GET", "/api/v3/remotepathmapping")
            .with_status(200)
            .with_body(&body)
            .expect(1) // second call must be served from cache
            .create_async()
            .await;

        let mut cfg = arr(&server.url());
        cfg.refresh = String::from("1h");
        let client = Client::new();
        let mut cache = DiscoveryCache::default();
        let first = cache
            .arr_mappings(&client, std::slice::from_ref(&cfg))
            .await;
        let second = cache
            .arr_mappings(&client, std::slice::from_ref(&cfg))
            .await;
        mock.assert_async().await;
        assert_eq!(first, second);
        assert_eq!(first.len(), 1);

        // Force a refresh against a now-failing endpoint: the cached
        // mappings must survive.
        mock.remove_async().await;
        let failing = server
            .mock("GET", "/api/v3/remotepathmapping")
            .with_status(500)
            .create_async()
            .await;
        cfg.refresh = String::from("0s");
        let third = cache
            .arr_mappings(&client, std::slice::from_ref(&cfg))
            .await;
        failing.assert_async().await;
        assert_eq!(third, first);
        Ok(())
    }
}
