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

mod config;
mod logger;
mod torrent;

use anyhow::{Error, Result};
use config::{ServerConfig, CONFIG_FILE};
use futures::future::join_all;
use log::{error, info};
use logger::setup_logger;
use std::time::Duration;
use tokio::sync::oneshot::channel as oneshot_channel;
use tokio::sync::oneshot::Receiver as OneshotReceiver;
use tokio::time::sleep;

use crate::torrent::{StateTracker, TorrentClient};

#[tokio::main]
async fn main() -> Result<()> {
    // The logger needs settings from the config, so config errors can only
    // go to stderr at this point.
    let config = config::load_config(CONFIG_FILE).map_err(|e| {
        eprintln!("Failed to load configuration: {}", e);
        e
    })?;

    setup_logger(&config.log_file, &config.max_log_file_size)?;
    info!("Starting qBittorrent Mover");

    let (shutdown_sender, shutdown_receiver) = oneshot_channel();

    // Spawn a task to listen for the ctrl+c signal
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        let _ = shutdown_sender.send(());
    });

    main_loop(config, shutdown_receiver).await?;

    info!("Shutting down qBittorrent Mover");
    Ok(())
}

async fn process_single_server(
    server: ServerConfig,
    tracker: &mut StateTracker,
) -> Result<(), Error> {
    let torrent_client = TorrentClient::new(server)?;
    torrent_client.login().await?;

    let torrents = torrent_client.get_torrents().await?;
    // Record every torrent's state and log transitions (e.g. downloading
    // -> seeding, or anything -> errored) before deciding what to act on.
    tracker.observe(&torrents);

    let tasks: Vec<_> = torrents
        .into_iter()
        .filter(|torrent| torrent.state.download_finished())
        .map(|torrent| {
            let torrent_client = torrent_client.clone();
            tokio::spawn(async move {
                if let Err(e) = torrent_client.move_and_clean_torrent_files(&torrent).await {
                    error!("Error moving torrent '{}': {:#}", torrent.name, e);
                }
            })
        })
        .collect();
    // Wait for all moves to finish before returning, so the next polling
    // cycle can't pick up the same torrents while they are still in flight.
    join_all(tasks).await;

    // Best-effort session cleanup.
    let _ = torrent_client.logout().await;
    Ok(())
}

async fn process_all_servers(
    servers: &[ServerConfig],
    trackers: &mut [StateTracker],
) -> Result<(), Error> {
    let tasks = servers
        .iter()
        .zip(trackers.iter_mut())
        .map(|(server, tracker)| process_single_server(server.clone(), tracker));
    let results: Vec<_> = join_all(tasks).await;

    let errors: Vec<String> = servers
        .iter()
        .zip(results)
        .filter_map(|(server, res)| res.err().map(|e| format!("{}: {:#}", server.qbit_url, e)))
        .collect();
    if !errors.is_empty() {
        return Err(anyhow::anyhow!(
            "Encountered {} error(s): {}",
            errors.len(),
            errors.join("; ")
        ));
    }

    Ok(())
}

async fn main_loop(config: config::Config, mut shutdown_signal: OneshotReceiver<()>) -> Result<()> {
    // Guard against a zero delay, which would busy-loop against the servers.
    let poll_delay = Duration::from_secs(config.rate_limit_delay.max(1));
    // One state tracker per server, persisted across polling cycles so
    // torrent state transitions can be detected and logged.
    let mut trackers = vec![StateTracker::default(); config.servers.len()];
    loop {
        if let Err(e) = process_all_servers(&config.servers, &mut trackers).await {
            error!("Error processing servers: {:#}", e);
        }

        tokio::select! {
            Ok(_) = &mut shutdown_signal => {
                info!("Received shutdown signal. Exiting...");
                break;
            }
            _ = sleep(poll_delay) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use mockito::Server;

    #[tokio::test]
    async fn test_main_loop() -> Result<()> {
        // Setup
        let (shutdown_sender, shutdown_receiver) = oneshot_channel();

        // Start the mock server
        let mut server = Server::new_async().await;
        let login_mock = server
            .mock("POST", "/api/v2/auth/login")
            .with_status(200)
            .with_body("Ok.")
            .expect(1)
            .create_async()
            .await;
        let info_mock = server
            .mock("GET", "/api/v2/torrents/info")
            .with_status(200)
            .with_body("[]")
            .expect(1)
            .create_async()
            .await;
        let logout_mock = server
            .mock("POST", "/api/v2/auth/logout")
            .with_status(200)
            .expect(1)
            .create_async()
            .await;

        // Update the config to use the mock server
        let mut config = config::Config::default();
        config.servers = vec![config::ServerConfig {
            qbit_url: server.url(),
            ..Default::default()
        }];

        // Run the main_loop
        let main_loop_future = tokio::spawn(main_loop(config, shutdown_receiver));

        // Wait for a while and then send the shutdown signal
        sleep(Duration::from_secs(1)).await;
        let _ = shutdown_sender.send(());

        // Wait for the main_loop to finish
        let _ = main_loop_future.await?;

        // Verify the mock expectations
        login_mock.assert_async().await;
        info_mock.assert_async().await;
        logout_mock.assert_async().await;

        Ok(())
    }
}
