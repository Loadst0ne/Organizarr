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

use crate::torrent::TorrentState;
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::time::Duration;

pub const CONFIG_FILE: &str = "config.yaml";

/// Parses a human-friendly duration string: a number followed by an
/// optional unit (`s`, `m`, `h`, `d`, `w`). A bare number means seconds.
/// Examples: `"7d"`, `"12h"`, `"90m"`, `"3600"`, `"1w"`.
pub fn parse_duration(input: &str) -> Result<Duration> {
    let s = input.trim();
    if s.is_empty() {
        bail!("Empty duration");
    }
    let (number, unit) = match s.find(|c: char| !c.is_ascii_digit()) {
        Some(idx) => s.split_at(idx),
        None => (s, "s"),
    };
    let value: u64 = number
        .parse()
        .with_context(|| format!("Invalid duration {:?}", input))?;
    let seconds = match unit.trim().to_ascii_lowercase().as_str() {
        "s" | "sec" | "secs" => value,
        "m" | "min" | "mins" => value * 60,
        "h" | "hr" | "hrs" => value * 3600,
        "d" | "day" | "days" => value * 86_400,
        "w" | "week" | "weeks" => value * 604_800,
        other => bail!("Unknown duration unit {:?} in {:?}", other, input),
    };
    Ok(Duration::from_secs(seconds))
}

#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct Config {
    pub servers: Vec<ServerConfig>,
    /// Seconds to wait between polling cycles.
    #[serde(default = "default_rate_limit_delay")]
    pub rate_limit_delay: u64,
    #[serde(default = "default_log_file")]
    pub log_file: String,
    /// Maximum log file size, e.g. "500K", "10M", "10MB", "1GB".
    #[serde(default = "default_max_log_file_size")]
    pub max_log_file_size: String,
    /// Maximum number of torrents whose files are moved concurrently per
    /// server. Keeps disk and network I/O bounded even when thousands of
    /// torrents become eligible at once.
    #[serde(default = "default_max_concurrent_moves")]
    pub max_concurrent_moves: usize,
    /// What to do with a torrent that errored *after* its download had
    /// already completed (e.g. `missingFiles`), when its category is mapped:
    /// - `keep`: leave it alone (only log a warning).
    /// - `remove`: remove the torrent from qBittorrent, keep any files.
    /// - `remove_with_data`: remove the torrent and ask qBittorrent to
    ///   delete its files too (honors qBittorrent's own "move to trash"
    ///   preference).
    #[serde(default)]
    pub errored_completed_action: ErroredCompletedAction,
}

/// Action taken for torrents that error out after completing their download.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ErroredCompletedAction {
    Keep,
    #[default]
    Remove,
    RemoveWithData,
}

fn default_rate_limit_delay() -> u64 {
    5
}

fn default_max_concurrent_moves() -> usize {
    2
}

fn default_log_file() -> String {
    String::from("qbittorrent-mover.log")
}

fn default_max_log_file_size() -> String {
    String::from("10M")
}

impl Default for Config {
    fn default() -> Self {
        Self {
            servers: Vec::new(),
            rate_limit_delay: default_rate_limit_delay(),
            log_file: default_log_file(),
            max_log_file_size: default_max_log_file_size(),
            max_concurrent_moves: default_max_concurrent_moves(),
            errored_completed_action: ErroredCompletedAction::default(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct ServerConfig {
    pub qbit_url: String,
    /// WebUI username. Leave empty to skip authentication (for WebUI
    /// setups with authentication bypass enabled).
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    /// Maps a qBittorrent category to the local directory completed
    /// torrents in that category should be moved to.
    #[serde(default)]
    pub categories: HashMap<String, String>,
    /// Local path that replaces `path_prefix` when mapping remote
    /// qBittorrent paths onto the local filesystem.
    #[serde(default)]
    pub root_path: Option<String>,
    /// Prefix of paths as reported by qBittorrent (e.g. a container or
    /// remote mount path) to strip before prepending `root_path`.
    #[serde(default)]
    pub path_prefix: Option<String>,
    /// Categories this tool must never touch in any way (no re-categorizing,
    /// no tagging, no moves, no deletes). Use this to protect the *active*
    /// download categories of other tools (e.g. `tv-sonarr`, `radarr`,
    /// `lidarr`, unpackerr-watched categories) so they keep full control
    /// until they hand the torrent off.
    #[serde(default)]
    pub ignore_categories: Vec<String>,
    /// Built-in default behaviors. Each can be disabled or tuned; user
    /// `rules` always take precedence over all of them.
    #[serde(default)]
    pub behaviors: BehaviorConfig,
    /// User-defined rules, evaluated in order for every torrent on every
    /// polling cycle; the first rule whose `when` conditions all match has
    /// its `then` actions applied (first match wins), and default behaviors
    /// are skipped for that torrent on that cycle.
    #[serde(default)]
    pub rules: Vec<Rule>,
}

/// The built-in default behaviors. Only the behaviors listed here exist;
/// everything else must be configured explicitly through `rules`.
#[derive(Debug, Deserialize, Clone, Serialize, PartialEq, Default)]
pub struct BehaviorConfig {
    /// "Send to Seeding": when a torrent finishes downloading, overwrite
    /// its category with the seeding category and clear its tags.
    #[serde(default)]
    pub send_to_seeding: SendToSeedingConfig,
    /// Time/ratio limits for torrents in *forced seeding* (`forcedUP`).
    #[serde(default)]
    pub forced_seeding_limit: ForcedSeedingLimitConfig,
    /// How this tool deletes torrent payloads when a rule or behavior
    /// calls for deletion.
    #[serde(default)]
    pub delete: DeleteConfig,
}

/// Configuration for the "Send to Seeding" default behavior.
#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct SendToSeedingConfig {
    /// Enabled by default.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Category assigned to torrents that finish downloading. This
    /// *overwrites* any existing category.
    #[serde(default = "default_seeding_category")]
    pub category: String,
    /// Create the seeding category via the qBittorrent API if it does not
    /// exist yet.
    #[serde(default = "default_true")]
    pub create_category: bool,
    /// Remove all tags from the torrent when sending it to seeding.
    #[serde(default = "default_true")]
    pub clear_tags: bool,
}

impl Default for SendToSeedingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            category: default_seeding_category(),
            create_category: true,
            clear_tags: true,
        }
    }
}

/// Configuration for the forced-seeding limit default behavior.
#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct ForcedSeedingLimitConfig {
    /// Enabled by default.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Maximum time a forced-seeding torrent may have been active, e.g.
    /// "7d", "12h", "90m". Torrents exceeding it are deleted. `null`
    /// disables the time criterion.
    #[serde(default = "default_forced_time_limit")]
    pub max_time_active: Option<String>,
    /// Maximum share ratio for forced-seeding torrents. Exceeding it
    /// deletes the torrent. `null` (default) disables the ratio criterion.
    #[serde(default)]
    pub max_ratio: Option<f64>,
}

impl Default for ForcedSeedingLimitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_time_active: default_forced_time_limit(),
            max_ratio: None,
        }
    }
}

/// How payload deletion is performed.
#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct DeleteConfig {
    /// Prefer moving deleted payloads to the user's trash / recycle bin
    /// (performed host-side by this tool). Enabled by default.
    #[serde(default = "default_true")]
    pub use_trash: bool,
    /// When trashing is not currently possible, the torrent is parked in
    /// this category instead and retried on every cycle until the trash
    /// becomes available.
    #[serde(default = "default_delete_category")]
    pub fallback_category: String,
}

impl Default for DeleteConfig {
    fn default() -> Self {
        Self {
            use_trash: true,
            fallback_category: default_delete_category(),
        }
    }
}

/// A user-defined rule: `when` all listed conditions match a torrent,
/// apply the `then` actions in order.
#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct Rule {
    /// Optional human-readable name used in logs.
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub when: RuleMatch,
    #[serde(default)]
    pub then: Vec<RuleAction>,
}

/// Conditions of a rule. Every specified condition must hold (logical
/// AND); an omitted condition matches anything. An empty `when` matches
/// every torrent.
#[derive(Debug, Deserialize, Clone, Serialize, PartialEq, Default)]
pub struct RuleMatch {
    /// Current state(s), using qBittorrent API spellings (`downloading`,
    /// `stalledUP`, `stoppedDL`/`pausedDL`, `queuedUP`, `forcedUP`,
    /// `error`, `missingFiles`, `moving`, ...).
    #[serde(default)]
    pub states: Option<Vec<TorrentState>>,
    /// Previous state(s) observed on the last cycle: matches *state
    /// changes*, e.g. `from_states: [downloading]` + `states: [uploading]`
    /// fires exactly when a download finishes. Torrents seen for the
    /// first time have no previous state and never match this condition.
    #[serde(default)]
    pub from_states: Option<Vec<TorrentState>>,
    /// Torrent category must equal one of these (empty string matches
    /// uncategorized torrents).
    #[serde(default)]
    pub categories: Option<Vec<String>>,
    /// Torrent must carry at least one of these tags.
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    /// Whether the download must be complete (`true`) or incomplete
    /// (`false`), judged from transfer data.
    #[serde(default)]
    pub complete: Option<bool>,
    /// Minimum time the torrent has been active, e.g. "7d", "12h".
    #[serde(default)]
    pub min_time_active: Option<String>,
    /// Minimum share ratio.
    #[serde(default)]
    pub min_ratio: Option<f64>,
}

/// An action a rule can apply to a matching torrent. In YAML each action
/// is written as `action: <name>` plus an optional `with:` payload, e.g.
///
/// ```yaml
/// then:
///   - action: set_category
///     with: "archive"
///   - action: add_tags
///     with: ["processed"]
///   - action: delete
/// ```
#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
#[serde(tag = "action", content = "with", rename_all = "snake_case")]
pub enum RuleAction {
    /// Overwrite the torrent's category (created on demand if missing).
    SetCategory(String),
    /// Add tags to the torrent.
    AddTags(Vec<String>),
    /// Remove specific tags from the torrent.
    RemoveTags(Vec<String>),
    /// Remove all tags from the torrent.
    ClearTags,
    /// Stop (pause) the torrent.
    Stop,
    /// Start (resume) the torrent.
    Start,
    /// Re-verify the torrent's payload.
    Recheck,
    /// Move the payload to the given directory, then remove the torrent
    /// from qBittorrent keeping the moved files (the standard move flow,
    /// including stop-before-move and path remapping).
    MoveFiles(String),
    /// Remove the torrent from qBittorrent, keeping its files.
    Remove,
    /// Remove the torrent and ask qBittorrent to delete its files
    /// (honoring qBittorrent's own trash preference).
    RemoveWithData,
    /// Delete the payload via this tool's delete flow: host-side trash
    /// when possible, otherwise park in the delete fallback category.
    Delete,
    /// Explicitly do nothing (useful to shield torrents from default
    /// behaviors and later rules).
    Nothing,
}

fn default_true() -> bool {
    true
}

fn default_seeding_category() -> String {
    String::from("Seeding")
}

fn default_delete_category() -> String {
    String::from("Delete")
}

fn default_forced_time_limit() -> Option<String> {
    Some(String::from("7d"))
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            qbit_url: String::from("http://localhost:8080"),
            username: String::from("admin"),
            password: String::from("adminadmin"),
            categories: HashMap::new(),
            root_path: None,
            path_prefix: None,
            ignore_categories: Vec::new(),
            behaviors: BehaviorConfig::default(),
            rules: Vec::new(),
        }
    }
}

pub fn load_config(filename: &str) -> Result<Config> {
    match File::open(filename) {
        Ok(file) => serde_yaml_ng::from_reader(&file)
            .with_context(|| format!("Failed to parse configuration file {}", filename)),
        Err(_) => {
            // No config yet: write out a commented default so the user has
            // a template to fill in.
            let default_config = Config::default();
            let file = File::create(filename)
                .with_context(|| format!("Failed to create configuration file {}", filename))?;
            serde_yaml_ng::to_writer(&file, &default_config)?;
            Ok(default_config)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(config.servers.is_empty());
        assert_eq!(config.rate_limit_delay, 5);
        assert_eq!(config.log_file, "qbittorrent-mover.log");
        assert_eq!(config.max_log_file_size, "10M");
    }

    #[test]
    fn test_default_server_config() {
        let server_config = ServerConfig::default();
        assert_eq!(server_config.qbit_url, "http://localhost:8080");
        assert_eq!(server_config.username, "admin");
        assert_eq!(server_config.password, "adminadmin");
        assert_eq!(server_config.categories, HashMap::new());
    }
    #[test]
    fn test_load_config() {
        let mut test_config = Config::default();
        test_config.servers.push(ServerConfig::default());
        let filename = "test_config.yaml";
        let file = File::create(filename).expect("Failed to create file");
        serde_yaml_ng::to_writer(file, &test_config).expect("Failed to write to file");

        let config = load_config(filename);
        assert!(config.is_ok());
        let config = config.expect("Failed to load config");
        assert_eq!(config.servers.len(), 1);
        assert_eq!(config.rate_limit_delay, 5);
        assert_eq!(config.log_file, "qbittorrent-mover.log");
        assert_eq!(config.max_log_file_size, "10M");

        fs::remove_file(filename).expect("Failed to remove file");
    }
    #[test]
    fn test_load_config_creates_file() {
        let filename = "test_config_create.yaml";
        let _ = fs::remove_file(filename); // Ensure the file does not exist before the test

        let config = load_config(filename);
        assert!(config.is_ok(), "Failed to load config");
        assert!(fs::metadata(filename).is_ok(), "File was not created");

        fs::remove_file(filename).expect("Failed to remove file");
    }

    #[test]
    fn test_minimal_config_uses_defaults() {
        let yaml = r#"
servers:
  - qbit_url: "http://localhost:8080"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse");
        assert_eq!(config.servers.len(), 1);
        assert_eq!(config.servers[0].username, "");
        assert_eq!(config.servers[0].root_path, None);
        assert_eq!(config.rate_limit_delay, 5);
        assert_eq!(config.max_log_file_size, "10M");
        assert_eq!(config.max_concurrent_moves, 2);
        assert_eq!(
            config.errored_completed_action,
            ErroredCompletedAction::Remove
        );
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("30").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("45s").unwrap(), Duration::from_secs(45));
        assert_eq!(parse_duration("90m").unwrap(), Duration::from_secs(5400));
        assert_eq!(parse_duration("12h").unwrap(), Duration::from_secs(43_200));
        assert_eq!(parse_duration("7d").unwrap(), Duration::from_secs(604_800));
        assert_eq!(parse_duration("1w").unwrap(), Duration::from_secs(604_800));
        assert_eq!(
            parse_duration(" 2D ").unwrap(),
            Duration::from_secs(172_800)
        );
        assert!(parse_duration("").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("7y").is_err());
    }

    #[test]
    fn test_behavior_defaults() {
        let yaml = "servers:\n  - qbit_url: \"http://localhost:8080\"\n";
        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse");
        let behaviors = &config.servers[0].behaviors;
        assert!(behaviors.send_to_seeding.enabled);
        assert_eq!(behaviors.send_to_seeding.category, "Seeding");
        assert!(behaviors.send_to_seeding.create_category);
        assert!(behaviors.send_to_seeding.clear_tags);
        assert!(behaviors.forced_seeding_limit.enabled);
        assert_eq!(
            behaviors.forced_seeding_limit.max_time_active.as_deref(),
            Some("7d")
        );
        assert_eq!(behaviors.forced_seeding_limit.max_ratio, None);
        assert!(behaviors.delete.use_trash);
        assert_eq!(behaviors.delete.fallback_category, "Delete");
        assert!(config.servers[0].ignore_categories.is_empty());
        assert!(config.servers[0].rules.is_empty());
    }

    #[test]
    fn test_behaviors_can_be_tuned_and_disabled() {
        let yaml = r#"
servers:
  - qbit_url: "http://localhost:8080"
    ignore_categories: ["tv-sonarr", "radarr"]
    behaviors:
      send_to_seeding:
        enabled: false
      forced_seeding_limit:
        max_time_active: "14d"
        max_ratio: 3.0
      delete:
        use_trash: false
        fallback_category: "Trashcan"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse");
        let server = &config.servers[0];
        assert_eq!(server.ignore_categories, vec!["tv-sonarr", "radarr"]);
        let behaviors = &server.behaviors;
        assert!(!behaviors.send_to_seeding.enabled);
        // Untouched fields keep their defaults.
        assert_eq!(behaviors.send_to_seeding.category, "Seeding");
        assert_eq!(
            behaviors.forced_seeding_limit.max_time_active.as_deref(),
            Some("14d")
        );
        assert_eq!(behaviors.forced_seeding_limit.max_ratio, Some(3.0));
        assert!(!behaviors.delete.use_trash);
        assert_eq!(behaviors.delete.fallback_category, "Trashcan");
    }

    #[test]
    fn test_rules_parsing() {
        let yaml = r#"
servers:
  - qbit_url: "http://localhost:8080"
    rules:
      - name: "archive finished music"
        when:
          from_states: [downloading]
          states: [uploading, stalledUP]
          categories: [music]
          complete: true
        then:
          - action: set_category
            with: "archive"
          - action: add_tags
            with: ["processed"]
      - name: "prune old seeds"
        when:
          min_time_active: "30d"
          min_ratio: 2.0
        then:
          - action: delete
      - name: "leave everything else alone"
        then:
          - action: nothing
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse");
        let rules = &config.servers[0].rules;
        assert_eq!(rules.len(), 3);
        assert_eq!(rules[0].name.as_deref(), Some("archive finished music"));
        assert_eq!(
            rules[0].when.states.as_deref(),
            Some(&[TorrentState::Uploading, TorrentState::StalledUpload][..])
        );
        assert_eq!(
            rules[0].when.from_states.as_deref(),
            Some(&[TorrentState::Downloading][..])
        );
        assert_eq!(rules[0].when.complete, Some(true));
        assert_eq!(
            rules[0].then,
            vec![
                RuleAction::SetCategory(String::from("archive")),
                RuleAction::AddTags(vec![String::from("processed")]),
            ]
        );
        assert_eq!(rules[1].when.min_time_active.as_deref(), Some("30d"));
        assert_eq!(rules[1].when.min_ratio, Some(2.0));
        assert_eq!(rules[1].then, vec![RuleAction::Delete]);
        assert_eq!(rules[2].when, RuleMatch::default());
        assert_eq!(rules[2].then, vec![RuleAction::Nothing]);
    }

    #[test]
    fn test_errored_completed_action_parsing() {
        let yaml = r#"
servers: []
errored_completed_action: remove_with_data
max_concurrent_moves: 8
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse");
        assert_eq!(
            config.errored_completed_action,
            ErroredCompletedAction::RemoveWithData
        );
        assert_eq!(config.max_concurrent_moves, 8);

        let yaml_keep = "servers: []\nerrored_completed_action: keep\n";
        let config: Config = serde_yaml_ng::from_str(yaml_keep).expect("Failed to parse");
        assert_eq!(
            config.errored_completed_action,
            ErroredCompletedAction::Keep
        );
    }
}
