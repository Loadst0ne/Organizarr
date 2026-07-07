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

//! Decision engine: for each torrent on each cycle, decide what (if
//! anything) to do with it. User-defined rules always take precedence
//! over the built-in default behaviors, and `ignore_categories` shields
//! torrents from everything.

use crate::config::{
    parse_duration, DeleteConfig, ForcedSeedingLimitConfig, Rule, RuleAction, RuleMatch,
    SendToSeedingConfig, ServerConfig,
};
use crate::torrent::{Torrent, TorrentClient, TorrentState};
use anyhow::Result;
use log::{info, warn};
use std::path::Path;

/// What the pipeline decided to do with one torrent this cycle.
/// Exactly one decision applies per torrent per cycle.
#[derive(Debug)]
pub enum Decision<'a> {
    /// Category is listed in `ignore_categories`: hands off entirely.
    Ignore,
    /// A user rule matched; apply its actions.
    UserRule(&'a Rule),
    /// Torrent is errored: lifecycle-aware recovery / removal (handled
    /// sequentially because it mutates the tracker's backoff schedule).
    Errored,
    /// Torrent is parked in the delete fallback category: retry deletion.
    RetryDelete,
    /// Category is mapped and the torrent is eligible: move the payload.
    MoveMapped,
    /// Forced-seeding torrent exceeded its configured limits: delete it.
    ForcedLimitExceeded,
    /// Torrent finished downloading this cycle: send it to the seeding
    /// category.
    SendToSeeding,
    /// Nothing applies this cycle.
    None,
}

/// Decides what to do with `torrent` this cycle. `previous` is the state
/// the torrent was in on the previous cycle (from the [`StateTracker`]),
/// used to match state *transitions*.
///
/// Precedence: `ignore_categories` > user rules (in order, first match
/// wins) > delete-retry > errored handling > mapped-category move >
/// forced-seeding limit > send-to-seeding.
///
/// [`StateTracker`]: crate::torrent::StateTracker
pub fn decide<'a>(
    torrent: &Torrent,
    previous: Option<TorrentState>,
    server: &'a ServerConfig,
) -> Decision<'a> {
    if server
        .ignore_categories
        .iter()
        .any(|c| c == &torrent.category)
    {
        return Decision::Ignore;
    }
    for rule in &server.rules {
        if rule_matches(&rule.when, torrent, previous) {
            return Decision::UserRule(rule);
        }
    }
    let behaviors = &server.behaviors;
    // Torrents parked in the fallback category are pending deletion.
    if torrent.category == behaviors.delete.fallback_category {
        return Decision::RetryDelete;
    }
    if torrent.state.is_errored() {
        return Decision::Errored;
    }
    if server.categories.contains_key(&torrent.category) {
        // Mapped categories keep the classic move flow; the eligibility
        // check avoids re-deciding (and re-logging) incomplete torrents.
        return if torrent.eligible_for_move() {
            Decision::MoveMapped
        } else {
            Decision::None
        };
    }
    if behaviors.forced_seeding_limit.enabled
        && torrent.state == TorrentState::ForcedUpload
        && forced_limit_exceeded(torrent, &behaviors.forced_seeding_limit)
    {
        return Decision::ForcedLimitExceeded;
    }
    if behaviors.send_to_seeding.enabled
        && torrent.category != behaviors.send_to_seeding.category
        && just_finished(torrent, previous)
    {
        return Decision::SendToSeeding;
    }
    Decision::None
}

/// True when the torrent transitioned from a not-finished state to a
/// finished one between the previous cycle and now — i.e. its download
/// finished while this tool was watching. Torrents seen for the first
/// time never qualify, so a restart of this tool cannot re-categorize
/// every existing torrent.
fn just_finished(torrent: &Torrent, previous: Option<TorrentState>) -> bool {
    previous.is_some_and(|prev| !prev.download_finished())
        && torrent.state.download_finished()
        && torrent.is_download_complete()
}

/// True when the forced-seeding torrent exceeds any configured limit.
fn forced_limit_exceeded(torrent: &Torrent, cfg: &ForcedSeedingLimitConfig) -> bool {
    if let Some(max) = &cfg.max_time_active {
        match parse_duration(max) {
            Ok(limit) => {
                if torrent.active_duration() > limit {
                    return true;
                }
            }
            Err(e) => warn!(
                "Invalid forced_seeding_limit.max_time_active {:?}: {:#}",
                max, e
            ),
        }
    }
    if let Some(max_ratio) = cfg.max_ratio {
        if torrent.ratio >= max_ratio {
            return true;
        }
    }
    false
}

/// True when every condition in `m` holds for the torrent (logical AND).
/// Omitted conditions match anything; an empty `when` matches everything.
pub fn rule_matches(m: &RuleMatch, torrent: &Torrent, previous: Option<TorrentState>) -> bool {
    if let Some(states) = &m.states {
        if !states.contains(&torrent.state) {
            return false;
        }
    }
    if let Some(from) = &m.from_states {
        // Matches an actual *transition*: the previous state must be
        // known, listed, and different from the current one.
        let transitioned =
            previous.is_some_and(|prev| from.contains(&prev) && prev != torrent.state);
        if !transitioned {
            return false;
        }
    }
    if let Some(cats) = &m.categories {
        if !cats.iter().any(|c| c == &torrent.category) {
            return false;
        }
    }
    if let Some(tags) = &m.tags {
        let torrent_tags = torrent.tag_list();
        if !tags.iter().any(|t| torrent_tags.contains(&t.as_str())) {
            return false;
        }
    }
    if let Some(complete) = m.complete {
        if torrent.is_download_complete() != complete {
            return false;
        }
    }
    if let Some(min) = &m.min_time_active {
        match parse_duration(min) {
            Ok(limit) => {
                if torrent.active_duration() < limit {
                    return false;
                }
            }
            Err(e) => {
                warn!("Invalid rule min_time_active {:?}: {:#}", min, e);
                return false;
            }
        }
    }
    if let Some(min_ratio) = m.min_ratio {
        if torrent.ratio < min_ratio {
            return false;
        }
    }
    true
}

/// Applies a matched rule's actions to the torrent, in order. Terminal
/// actions (`remove`, `remove_with_data`, `delete`) stop the sequence
/// since the torrent no longer exists afterwards.
pub async fn execute_rule(
    client: &TorrentClient,
    torrent: &Torrent,
    rule: &Rule,
    delete_cfg: &DeleteConfig,
) -> Result<()> {
    let label = rule.name.as_deref().unwrap_or("unnamed rule");
    info!("Rule '{}' matched torrent '{}'", label, torrent.name);
    for action in &rule.then {
        match action {
            RuleAction::SetCategory(category) => {
                client.set_category(&torrent.hash, category, true).await?;
            }
            RuleAction::AddTags(tags) => {
                client.add_tags(&torrent.hash, &tags.join(",")).await?;
            }
            RuleAction::RemoveTags(tags) => {
                client.remove_tags(&torrent.hash, &tags.join(",")).await?;
            }
            RuleAction::ClearTags => {
                client.clear_tags(torrent).await?;
            }
            RuleAction::Stop => {
                client.stop_torrent(&torrent.hash).await?;
            }
            RuleAction::Start => {
                client.start_torrent(&torrent.hash).await?;
            }
            RuleAction::Recheck => {
                client.recheck_torrent(&torrent.hash).await?;
            }
            RuleAction::MoveFiles(dest_dir) => {
                client
                    .move_torrent_files_to(torrent, Path::new(dest_dir))
                    .await?;
                // The move flow removes the torrent from qBittorrent.
                return Ok(());
            }
            RuleAction::Remove => {
                client.remove_torrent(&torrent.hash, false).await?;
                info!(
                    "Rule '{}' removed torrent '{}', files kept",
                    label, torrent.name
                );
                return Ok(());
            }
            RuleAction::RemoveWithData => {
                client.remove_torrent(&torrent.hash, true).await?;
                info!(
                    "Rule '{}' removed torrent '{}' with its files",
                    label, torrent.name
                );
                return Ok(());
            }
            RuleAction::Delete => {
                delete_with_fallback(client, torrent, delete_cfg).await?;
                return Ok(());
            }
            RuleAction::Nothing => {}
        }
    }
    Ok(())
}

/// Deletes a torrent's payload according to the configured delete flow:
/// host-side trash when `use_trash` is set (parking the torrent in the
/// fallback category for retry if the trash is unavailable), otherwise a
/// straight remove-with-data, which honors qBittorrent's own trash
/// preference.
pub async fn delete_with_fallback(
    client: &TorrentClient,
    torrent: &Torrent,
    cfg: &DeleteConfig,
) -> Result<()> {
    if !cfg.use_trash {
        client.remove_torrent(&torrent.hash, true).await?;
        info!(
            "Removed torrent '{}' and asked qBittorrent to delete its files",
            torrent.name
        );
        return Ok(());
    }
    match client.delete_torrent_to_trash(torrent).await {
        Ok(()) => Ok(()),
        Err(e) => {
            warn!(
                "Could not trash payload of '{}' ({:#}); parking it in category {:?} to retry later",
                torrent.name, e, cfg.fallback_category
            );
            client
                .set_category(&torrent.hash, &cfg.fallback_category, true)
                .await
        }
    }
}

/// Applies the "Send to Seeding" default behavior: overwrite the torrent's
/// category with the seeding category (created on demand when configured)
/// and clear its tags.
pub async fn send_to_seeding(
    client: &TorrentClient,
    torrent: &Torrent,
    cfg: &SendToSeedingConfig,
) -> Result<()> {
    client
        .set_category(&torrent.hash, &cfg.category, cfg.create_category)
        .await?;
    if cfg.clear_tags {
        client.clear_tags(torrent).await?;
    }
    info!(
        "Sent '{}' to seeding category {:?} (finished downloading)",
        torrent.name, cfg.category
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BehaviorConfig;

    fn torrent(state: TorrentState, category: &str) -> Torrent {
        Torrent {
            state,
            category: category.to_string(),
            name: String::from("t"),
            hash: String::from("h"),
            progress: if state.download_finished() { 1.0 } else { 0.5 },
            ..Default::default()
        }
    }

    fn server() -> ServerConfig {
        ServerConfig {
            username: String::new(),
            behaviors: BehaviorConfig::default(),
            ..Default::default()
        }
    }

    #[test]
    fn test_ignored_category_shields_torrent() {
        let mut server = server();
        server.ignore_categories = vec![String::from("tv-sonarr")];
        let t = torrent(TorrentState::Uploading, "tv-sonarr");
        assert!(matches!(
            decide(&t, Some(TorrentState::Downloading), &server),
            Decision::Ignore
        ));
    }

    #[test]
    fn test_send_to_seeding_fires_on_finish_transition() {
        let server = server();
        let t = torrent(TorrentState::Uploading, "");
        // Transition downloading -> uploading fires the behavior.
        assert!(matches!(
            decide(&t, Some(TorrentState::Downloading), &server),
            Decision::SendToSeeding
        ));
        // First sighting (no previous state) must NOT fire.
        assert!(matches!(decide(&t, None, &server), Decision::None));
        // Already-finished torrents must NOT fire again.
        assert!(matches!(
            decide(&t, Some(TorrentState::Uploading), &server),
            Decision::None
        ));
    }

    #[test]
    fn test_send_to_seeding_skips_seeding_and_mapped_categories() {
        let mut server = server();
        server
            .categories
            .insert(String::from("movies"), String::from("/dest"));
        let prev = Some(TorrentState::Downloading);

        let seeding = torrent(TorrentState::Uploading, "Seeding");
        assert!(matches!(decide(&seeding, prev, &server), Decision::None));

        // Mapped category goes down the move path instead.
        let mapped = torrent(TorrentState::Uploading, "movies");
        assert!(matches!(
            decide(&mapped, prev, &server),
            Decision::MoveMapped
        ));
    }

    #[test]
    fn test_send_to_seeding_disabled() {
        let mut server = server();
        server.behaviors.send_to_seeding.enabled = false;
        let t = torrent(TorrentState::Uploading, "");
        assert!(matches!(
            decide(&t, Some(TorrentState::Downloading), &server),
            Decision::None
        ));
    }

    #[test]
    fn test_delete_fallback_category_is_retried() {
        let server = server();
        let t = torrent(TorrentState::StoppedUpload, "Delete");
        assert!(matches!(decide(&t, None, &server), Decision::RetryDelete));
    }

    #[test]
    fn test_errored_torrent_is_flagged() {
        let server = server();
        let t = torrent(TorrentState::Error, "");
        assert!(matches!(decide(&t, None, &server), Decision::Errored));
    }

    #[test]
    fn test_forced_seeding_limit_time() {
        let server = server(); // default: 7d limit
        let mut t = torrent(TorrentState::ForcedUpload, "");
        t.time_active = 8 * 86_400; // 8 days
        assert!(matches!(
            decide(&t, None, &server),
            Decision::ForcedLimitExceeded
        ));
        t.time_active = 6 * 86_400; // under the limit
        assert!(matches!(decide(&t, None, &server), Decision::None));
    }

    #[test]
    fn test_forced_seeding_limit_ratio() {
        let mut server = server();
        server.behaviors.forced_seeding_limit.max_time_active = None;
        server.behaviors.forced_seeding_limit.max_ratio = Some(2.0);
        let mut t = torrent(TorrentState::ForcedUpload, "");
        t.ratio = 2.5;
        assert!(matches!(
            decide(&t, None, &server),
            Decision::ForcedLimitExceeded
        ));
        t.ratio = 1.5;
        assert!(matches!(decide(&t, None, &server), Decision::None));
    }

    #[test]
    fn test_forced_seeding_limit_disabled() {
        let mut server = server();
        server.behaviors.forced_seeding_limit.enabled = false;
        let mut t = torrent(TorrentState::ForcedUpload, "");
        t.time_active = 30 * 86_400;
        assert!(matches!(decide(&t, None, &server), Decision::None));
    }

    #[test]
    fn test_user_rule_takes_precedence_over_defaults() {
        let mut server = server();
        server.rules = vec![Rule {
            name: Some(String::from("keep errored")),
            when: RuleMatch {
                states: Some(vec![TorrentState::Error]),
                ..Default::default()
            },
            then: vec![RuleAction::Nothing],
        }];
        let t = torrent(TorrentState::Error, "");
        assert!(matches!(decide(&t, None, &server), Decision::UserRule(_)));
    }

    #[test]
    fn test_rule_match_states_and_categories() {
        let m = RuleMatch {
            states: Some(vec![TorrentState::Uploading]),
            categories: Some(vec![String::from("music")]),
            ..Default::default()
        };
        assert!(rule_matches(
            &m,
            &torrent(TorrentState::Uploading, "music"),
            None
        ));
        assert!(!rule_matches(
            &m,
            &torrent(TorrentState::Uploading, "tv"),
            None
        ));
        assert!(!rule_matches(
            &m,
            &torrent(TorrentState::Downloading, "music"),
            None
        ));
    }

    #[test]
    fn test_rule_match_from_states_requires_transition() {
        let m = RuleMatch {
            from_states: Some(vec![TorrentState::Downloading]),
            states: Some(vec![TorrentState::Uploading]),
            ..Default::default()
        };
        let t = torrent(TorrentState::Uploading, "");
        assert!(rule_matches(&m, &t, Some(TorrentState::Downloading)));
        assert!(!rule_matches(&m, &t, None));
        assert!(!rule_matches(&m, &t, Some(TorrentState::Uploading)));
    }

    #[test]
    fn test_rule_match_tags_any_of() {
        let m = RuleMatch {
            tags: Some(vec![String::from("keep"), String::from("archive")]),
            ..Default::default()
        };
        let mut t = torrent(TorrentState::Uploading, "");
        t.tags = String::from("archive, other");
        assert!(rule_matches(&m, &t, None));
        t.tags = String::from("other");
        assert!(!rule_matches(&m, &t, None));
        t.tags = String::new();
        assert!(!rule_matches(&m, &t, None));
    }

    #[test]
    fn test_rule_match_complete_and_thresholds() {
        let m = RuleMatch {
            complete: Some(true),
            min_time_active: Some(String::from("1d")),
            min_ratio: Some(1.0),
            ..Default::default()
        };
        let mut t = torrent(TorrentState::Uploading, "");
        t.time_active = 2 * 86_400;
        t.ratio = 1.5;
        assert!(rule_matches(&m, &t, None));
        t.ratio = 0.5;
        assert!(!rule_matches(&m, &t, None));
        t.ratio = 1.5;
        t.time_active = 3600;
        assert!(!rule_matches(&m, &t, None));
        let mut incomplete = torrent(TorrentState::Downloading, "");
        incomplete.time_active = 2 * 86_400;
        incomplete.ratio = 1.5;
        assert!(!rule_matches(&m, &incomplete, None));
    }

    #[test]
    fn test_empty_when_matches_everything() {
        let m = RuleMatch::default();
        assert!(rule_matches(
            &m,
            &torrent(TorrentState::Downloading, "x"),
            None
        ));
    }

    #[tokio::test]
    async fn test_delete_with_fallback_parks_torrent_when_trash_fails() {
        // The torrent's path can't be resolved/trashed, so it must be
        // parked in the fallback category for a retry on a later cycle.
        let mut mock_server = mockito::Server::new_async().await;
        let park_mock = mock_server
            .mock("POST", "/api/v2/torrents/setCategory")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("hashes".into(), "h1".into()),
                mockito::Matcher::UrlEncoded("category".into(), "Delete".into()),
            ]))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;

        let mut server_cfg = server();
        server_cfg.qbit_url = mock_server.url();
        // A path_prefix that doesn't match the torrent's path makes the
        // host-side resolution (and hence the trash attempt) fail.
        server_cfg.path_prefix = Some(String::from("/data"));
        server_cfg.root_path = Some(String::from("Z:/nonexistent"));
        let delete_cfg = server_cfg.behaviors.delete.clone();
        let client = TorrentClient::new(server_cfg).unwrap();

        let mut t = torrent(TorrentState::StoppedUpload, "");
        t.hash = String::from("h1");
        t.save_path = String::from("/other/place");
        assert!(delete_with_fallback(&client, &t, &delete_cfg).await.is_ok());
        park_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_delete_without_trash_removes_with_data() {
        // use_trash: false delegates deletion to qBittorrent itself
        // (deleteFiles=true honors its own trash preference).
        let mut mock_server = mockito::Server::new_async().await;
        let delete_mock = mock_server
            .mock("POST", "/api/v2/torrents/delete")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("hashes".into(), "h1".into()),
                mockito::Matcher::UrlEncoded("deleteFiles".into(), "true".into()),
            ]))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;

        let mut server_cfg = server();
        server_cfg.qbit_url = mock_server.url();
        let mut delete_cfg = server_cfg.behaviors.delete.clone();
        delete_cfg.use_trash = false;
        let client = TorrentClient::new(server_cfg).unwrap();

        let mut t = torrent(TorrentState::StoppedUpload, "");
        t.hash = String::from("h1");
        assert!(delete_with_fallback(&client, &t, &delete_cfg).await.is_ok());
        delete_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_send_to_seeding_sets_category_and_clears_tags() {
        let mut mock_server = mockito::Server::new_async().await;
        let set_mock = mock_server
            .mock("POST", "/api/v2/torrents/setCategory")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("hashes".into(), "h1".into()),
                mockito::Matcher::UrlEncoded("category".into(), "Seeding".into()),
            ]))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;
        let clear_mock = mock_server
            .mock("POST", "/api/v2/torrents/removeTags")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("hashes".into(), "h1".into()),
                mockito::Matcher::UrlEncoded("tags".into(), "sonarr,old".into()),
            ]))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;

        let mut server_cfg = server();
        server_cfg.qbit_url = mock_server.url();
        let seeding_cfg = server_cfg.behaviors.send_to_seeding.clone();
        let client = TorrentClient::new(server_cfg).unwrap();

        let mut t = torrent(TorrentState::Uploading, "");
        t.hash = String::from("h1");
        t.tags = String::from("sonarr, old");
        assert!(send_to_seeding(&client, &t, &seeding_cfg).await.is_ok());
        set_mock.assert_async().await;
        clear_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_execute_rule_applies_actions_in_order() {
        let mut mock_server = mockito::Server::new_async().await;
        let set_mock = mock_server
            .mock("POST", "/api/v2/torrents/setCategory")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("hashes".into(), "h1".into()),
                mockito::Matcher::UrlEncoded("category".into(), "archive".into()),
            ]))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;
        let tag_mock = mock_server
            .mock("POST", "/api/v2/torrents/addTags")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("hashes".into(), "h1".into()),
                mockito::Matcher::UrlEncoded("tags".into(), "processed".into()),
            ]))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;
        let stop_mock = mock_server
            .mock("POST", "/api/v2/torrents/stop")
            .match_body(mockito::Matcher::UrlEncoded("hashes".into(), "h1".into()))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;

        let mut server_cfg = server();
        server_cfg.qbit_url = mock_server.url();
        let delete_cfg = server_cfg.behaviors.delete.clone();
        let client = TorrentClient::new(server_cfg).unwrap();

        let rule = Rule {
            name: Some(String::from("archive")),
            when: RuleMatch::default(),
            then: vec![
                RuleAction::SetCategory(String::from("archive")),
                RuleAction::AddTags(vec![String::from("processed")]),
                RuleAction::Stop,
            ],
        };
        let mut t = torrent(TorrentState::Uploading, "music");
        t.hash = String::from("h1");
        assert!(execute_rule(&client, &t, &rule, &delete_cfg).await.is_ok());
        set_mock.assert_async().await;
        tag_mock.assert_async().await;
        stop_mock.assert_async().await;
    }
}
