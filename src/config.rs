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

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;

pub const CONFIG_FILE: &str = "config.yaml";

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
}

fn default_rate_limit_delay() -> u64 {
    5
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
    }
}
