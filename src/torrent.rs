/*
qBittorrent Mover - A tool to automatically move torrents to different categories based on their state.
Copyright (C) 2023 Harrison Chin

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

use super::config::ServerConfig;
use anyhow::{bail, Context, Result};
use log::{info, warn};
use reqwest::{Client, Response};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::sleep;

const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize, Clone)]
pub struct Torrent {
    pub save_path: String,
    pub name: String,
    pub category: String,
    pub hash: String,
    /// Absolute path of the torrent's content on the qBittorrent host
    /// (root path for multi-file torrents, file path for single-file ones).
    /// Available since qBittorrent 4.2; preferred over `save_path` + `name`
    /// because it stays correct when a torrent's content is renamed.
    #[serde(default)]
    pub content_path: Option<String>,
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

    pub async fn get_completed_torrents(&self) -> Result<Vec<Torrent>> {
        let response = self
            .get("/api/v2/torrents/info", &[("filter", "completed")])
            .await?;
        let torrents = response
            .json::<Vec<Torrent>>()
            .await
            .context("Failed to parse torrent list from qBittorrent")?;
        Ok(torrents)
    }

    /// Removes the torrent from qBittorrent without deleting its files
    /// (the files have already been moved by this tool).
    pub async fn remove_torrent(&self, hash: &str) -> Result<()> {
        self.post_form(
            "/api/v2/torrents/delete",
            &[("hashes", hash), ("deleteFiles", "false")],
        )
        .await?;
        Ok(())
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
        for endpoint in ["/api/v2/torrents/stop", "/api/v2/torrents/pause"] {
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
        bail!("Neither torrents/stop nor torrents/pause is available on this qBittorrent version");
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
    /// moved files). Torrents in unmapped categories are left untouched.
    ///
    /// The torrent is stopped first so its file handles are released, and
    /// the operation is retry-safe: if a previous cycle moved the files but
    /// failed to remove the torrent, the removal is completed this cycle.
    pub async fn move_and_clean_torrent_files(&self, torrent: &Torrent) -> Result<()> {
        let Some(dest_dir) = self.server.categories.get(&torrent.category) else {
            return Ok(());
        };
        let src = self.resolve_source_path(torrent)?;
        let dest_dir = PathBuf::from(dest_dir);

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

        self.remove_torrent(&torrent.hash).await.with_context(|| {
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
    async fn test_get_completed_torrents() {
        let mut server = Server::new_async().await;
        let m = server
            .mock("GET", "/api/v2/torrents/info?filter=completed")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"save_path":"/downloads","name":"ubuntu.iso","category":"distros","hash":"abc123","content_path":"/downloads/ubuntu.iso"}]"#,
            )
            .create_async()
            .await;

        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        let torrents = client.get_completed_torrents().await.unwrap();
        assert_eq!(torrents.len(), 1);
        assert_eq!(torrents[0].name, "ubuntu.iso");
        assert_eq!(
            torrents[0].content_path.as_deref(),
            Some("/downloads/ubuntu.iso")
        );
        m.assert_async().await;
    }

    #[tokio::test]
    async fn test_get_completed_torrents_without_content_path() {
        // Older qBittorrent versions may not report content_path.
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/api/v2/torrents/info?filter=completed")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"save_path":"/downloads","name":"ubuntu.iso","category":"distros","hash":"abc123"}]"#,
            )
            .create_async()
            .await;

        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        let torrents = client.get_completed_torrents().await.unwrap();
        assert_eq!(torrents[0].content_path, None);
    }

    #[tokio::test]
    async fn test_http_error_status_is_reported() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/api/v2/torrents/info?filter=completed")
            .with_status(403)
            .create_async()
            .await;

        let client = TorrentClient::new(bypass_auth_config(server.url())).unwrap();
        assert!(client.get_completed_torrents().await.is_err());
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
        assert!(client.remove_torrent("test_hash").await.is_ok());
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
            content_path: None,
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
            content_path: Some(torrent_dir.to_str().unwrap().to_string()),
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
            content_path: None,
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
            content_path: Some(String::from("/downloads/movies/film.mkv")),
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
            content_path: Some(String::from("/data/Anime/Some Show S01")),
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
            content_path: Some(String::from("/data/Anime/x")),
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
            content_path: None,
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
}
