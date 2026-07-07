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

use anyhow::{anyhow, Context, Result};
use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::rolling_file::policy::compound::roll::fixed_window::FixedWindowRoller;
use log4rs::append::rolling_file::policy::compound::trigger::size::SizeTrigger;
use log4rs::append::rolling_file::policy::compound::CompoundPolicy;
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::config::{Appender, Config as LogConfig, Root};
use log4rs::encode::pattern::PatternEncoder;

const MAX_ARCHIVED_LOGS: u32 = 1;
const LOG_PATTERN: &str = "{d(%Y-%m-%d %H:%M:%S)} {l} - {m}\n";

/// Parses a human-readable size like "500K", "10M", "10MB" or "1GB"
/// (case-insensitive) into a number of bytes.
fn parse_size(size: &str) -> Result<u64> {
    let normalized = size.trim().to_ascii_uppercase();
    let (number, multiplier) = if let Some(n) = normalized
        .strip_suffix("GB")
        .or_else(|| normalized.strip_suffix('G'))
    {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = normalized
        .strip_suffix("MB")
        .or_else(|| normalized.strip_suffix('M'))
    {
        (n, 1024 * 1024)
    } else if let Some(n) = normalized
        .strip_suffix("KB")
        .or_else(|| normalized.strip_suffix('K'))
    {
        (n, 1024)
    } else {
        (normalized.strip_suffix('B').unwrap_or(&normalized), 1)
    };
    let number = number
        .trim()
        .parse::<u64>()
        .map_err(|e| anyhow!("Invalid size '{}': {}", size, e))?;
    Ok(number * multiplier)
}

pub fn setup_logger(log_file: &str, max_log_size: &str) -> Result<()> {
    if log::log_enabled!(log::Level::Info) {
        // A logger is already installed (e.g. across tests); nothing to do.
        return Ok(());
    }

    // Set up rolling file appender
    let roller = FixedWindowRoller::builder()
        .build(&format!("{}.{{}}", log_file), MAX_ARCHIVED_LOGS)
        .map_err(|e| anyhow!("Failed to build log roller: {}", e))?;
    let max_log_size = parse_size(max_log_size).context("Invalid max_log_file_size")?;
    let trigger = SizeTrigger::new(max_log_size);
    let policy = CompoundPolicy::new(Box::new(trigger), Box::new(roller));

    let file_appender = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(LOG_PATTERN)))
        .build(log_file, Box::new(policy))?;

    // Also log to the console so interactive runs give immediate feedback.
    let console_appender = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(LOG_PATTERN)))
        .build();

    let config = LogConfig::builder()
        .appender(Appender::builder().build("file_appender", Box::new(file_appender)))
        .appender(Appender::builder().build("console_appender", Box::new(console_appender)))
        .build(
            Root::builder()
                .appender("file_appender")
                .appender("console_appender")
                .build(LevelFilter::Info),
        )?;

    log4rs::init_config(config)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("512").unwrap(), 512);
        assert_eq!(parse_size("512B").unwrap(), 512);
        assert_eq!(parse_size("2K").unwrap(), 2 * 1024);
        assert_eq!(parse_size("10M").unwrap(), 10 * 1024 * 1024);
        assert_eq!(parse_size("10MB").unwrap(), 10 * 1024 * 1024);
        assert_eq!(parse_size("1g").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size(" 1 GB ").unwrap(), 1024 * 1024 * 1024);
        assert!(parse_size("abc").is_err());
        assert!(parse_size("").is_err());
    }

    #[test]
    fn test_setup_logger() -> Result<()> {
        let log_file = "test_logger.log";
        let max_log_size = "10M";
        setup_logger(log_file, max_log_size)?;

        // Check if the log file was created
        assert!(fs::metadata(log_file).is_ok());

        Ok(())
    }
}
