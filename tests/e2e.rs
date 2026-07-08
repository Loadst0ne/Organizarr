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

//! End-to-end test that exercises the workflow the way a user would run it:
//! the real compiled binary is started in a scratch directory with a real
//! `config.yaml`, pointed at a mock qBittorrent WebUI API that reports a
//! completed torrent using container-style paths (`/data/...`), with the
//! payload as real files on disk. The test then observes the daemon stop
//! the torrent, remap the path, move the files into the configured library
//! directory, and remove the torrent from qBittorrent.

use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// YAML-friendly rendering of a path (forward slashes work fine on Windows
/// and avoid YAML escape-sequence pitfalls with backslashes).
fn yaml_path(path: &Path) -> String {
    path.to_str()
        .expect("path is valid UTF-8")
        .replace('\\', "/")
}

#[tokio::test]
async fn e2e_completed_torrent_is_moved_and_removed() {
    let mut server = mockito::Server::new_async().await;
    let tmp = tempfile::tempdir().expect("create temp dir");
    let work = tmp.path();

    // Host-side view of the qBittorrent download area (what a container
    // would see as /data).
    let torrents_root = work.join("torrents");
    let src_dir = torrents_root.join("Anime").join("Some Show S01");
    fs::create_dir_all(&src_dir).expect("create source dir");
    fs::write(src_dir.join("Episode 01.mkv"), b"payload").expect("write payload");

    let library = work.join("library").join("Anime");

    // The mock qBittorrent reports one completed, seeding (stalled) torrent
    // with container-style paths, exactly as in a gluetun/Docker setup.
    let info_body = serde_json::json!([{
        "save_path": "/data/Anime",
        "name": "Some Show S01",
        "category": "anime",
        "hash": "e2ehash",
        "state": "stalledUP",
        "content_path": "/data/Anime/Some Show S01",
        "progress": 1.0,
        "amount_left": 0,
        "completion_on": 1_700_000_000
    }])
    .to_string();
    let info_mock = server
        .mock("GET", "/api/v2/torrents/info")
        .match_query(mockito::Matcher::Any)
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(info_body)
        .expect_at_least(1)
        .create_async()
        .await;
    let stop_mock = server
        .mock("POST", "/api/v2/torrents/stop")
        .match_body(mockito::Matcher::UrlEncoded(
            "hashes".into(),
            "e2ehash".into(),
        ))
        .with_status(200)
        .expect_at_least(1)
        .create_async()
        .await;
    let delete_mock = server
        .mock("POST", "/api/v2/torrents/delete")
        .match_body(mockito::Matcher::AllOf(vec![
            mockito::Matcher::UrlEncoded("hashes".into(), "e2ehash".into()),
            mockito::Matcher::UrlEncoded("deleteFiles".into(), "false".into()),
        ]))
        .with_status(200)
        .expect_at_least(1)
        .create_async()
        .await;

    // A real config.yaml, as a user would write it (empty username =
    // qBittorrent WebUI auth bypass). `after_move: remove` selects the
    // host-side move + remove flow this test observes end to end; the
    // default (`keep_seeding`) delegates the move to qBittorrent itself
    // and is covered by unit tests against the setLocation endpoint.
    let config = format!(
        r#"servers:
  - qbit_url: "{qbit_url}"
    username: ""
    password: ""
    after_move: remove
    categories:
      anime: "{library}"
    root_path: "{root}"
    path_prefix: "/data"
rate_limit_delay: 1
"#,
        qbit_url = server.url(),
        library = yaml_path(&library),
        root = yaml_path(&torrents_root),
    );
    fs::write(work.join("config.yaml"), config).expect("write config.yaml");

    // Start the real binary the way a user would, from the scratch dir.
    let mut child = Command::new(env!("CARGO_BIN_EXE_organizarr"))
        .current_dir(work)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn organizarr binary");

    // Wait for the daemon to complete the move (with a generous timeout).
    let dest_file = library.join("Some Show S01").join("Episode 01.mkv");
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && !dest_file.exists() {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let moved = dest_file.exists();
    let source_gone = !src_dir.exists();

    // Always terminate the daemon before asserting so a failure can't leak
    // a stray process.
    child.kill().ok();
    child.wait().ok();

    assert!(
        moved,
        "payload was not moved into the library directory within the timeout"
    );
    assert!(source_gone, "source payload was not removed after the move");
    info_mock.assert_async().await;
    stop_mock.assert_async().await;
    delete_mock.assert_async().await;
}

/// End-to-end discovery workflow, run the way a user would: the real
/// binary with a config that sets a category destination to `auto` and
/// imports remote path mappings from a (mock) Sonarr. The daemon must
/// fetch qBittorrent's category save paths, import and verify the *arr
/// mapping, and relocate the completed torrent via setLocation to the
/// save path qBittorrent reports — keeping it seeding (default
/// after_move), so no host-side move and no removal happen.
#[tokio::test]
async fn e2e_auto_destination_with_arr_import() {
    let mut server = mockito::Server::new_async().await;
    let tmp = tempfile::tempdir().expect("create temp dir");
    let work = tmp.path();

    // Host-side view of the qBittorrent download area, for the imported
    // *arr mapping to verify against.
    let torrents_root = work.join("torrents");
    fs::create_dir_all(&torrents_root).expect("create torrents root");

    let info_body = serde_json::json!([{
        "save_path": "/data/Anime",
        "name": "Some Show S01",
        "category": "anime",
        "hash": "autohash",
        "state": "stalledUP",
        "content_path": "/data/Anime/Some Show S01",
        "progress": 1.0,
        "amount_left": 0,
        "completion_on": 1_700_000_000
    }])
    .to_string();
    let info_mock = server
        .mock("GET", "/api/v2/torrents/info")
        .match_query(mockito::Matcher::Any)
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(info_body)
        .expect_at_least(1)
        .create_async()
        .await;
    // qBittorrent's own category configuration: "anime" saves to
    // /media/anime (the client's view of the filesystem).
    let categories_mock = server
        .mock("GET", "/api/v2/torrents/categories")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"anime":{"name":"anime","savePath":"/media/anime"}}"#)
        .expect_at_least(1)
        .create_async()
        .await;
    // The mock Sonarr (same mock server, Sonarr-style path) publishes a
    // remote path mapping whose local side really exists here.
    let arr_body = serde_json::json!([{
        "id": 1,
        "host": "qbittorrent",
        "remotePath": "/data",
        "localPath": yaml_path(&torrents_root)
    }])
    .to_string();
    let arr_mock = server
        .mock("GET", "/api/v3/remotepathmapping")
        .match_header("X-Api-Key", "e2e-arr-key")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(arr_body)
        .expect_at_least(1)
        .create_async()
        .await;
    // The relocation must target the save path detected from qBittorrent.
    let set_location_mock = server
        .mock("POST", "/api/v2/torrents/setLocation")
        .match_body(mockito::Matcher::AllOf(vec![
            mockito::Matcher::UrlEncoded("hashes".into(), "autohash".into()),
            mockito::Matcher::UrlEncoded("location".into(), "/media/anime".into()),
        ]))
        .with_status(200)
        .expect_at_least(1)
        .create_async()
        .await;
    // keep_seeding must never remove the torrent.
    let delete_mock = server
        .mock("POST", "/api/v2/torrents/delete")
        .expect(0)
        .create_async()
        .await;

    let config = format!(
        r#"servers:
  - qbit_url: "{qbit_url}"
    username: ""
    password: ""
    categories:
      anime: auto
    arr:
      - name: sonarr
        url: "{arr_url}"
        api_key: "e2e-arr-key"
rate_limit_delay: 1
"#,
        qbit_url = server.url(),
        arr_url = server.url(),
    );
    fs::write(work.join("config.yaml"), config).expect("write config.yaml");

    let mut child = Command::new(env!("CARGO_BIN_EXE_organizarr"))
        .current_dir(work)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn organizarr binary");

    // Wait for the daemon to issue the relocation.
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && !set_location_mock.matched_async().await {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    child.kill().ok();
    child.wait().ok();

    info_mock.assert_async().await;
    categories_mock.assert_async().await;
    arr_mock.assert_async().await;
    set_location_mock.assert_async().await;
    delete_mock.assert_async().await;
}
