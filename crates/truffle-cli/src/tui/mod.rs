//! TUI mode — the interactive terminal interface.
//!
//! Launched by bare `truffle` (no subcommand). Starts the daemon in-process,
//! renders a ratatui UI, and processes events until the user exits.

use std::io;
use std::panic;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::ExecutableCommand;
use ratatui::prelude::*;

use crate::config::TruffleConfig;
use crate::daemon::server::DaemonServer;

pub mod app;
pub mod commands;
pub mod event;
pub mod onboarding;
pub mod ui;

use app::AppState;
use event::AppEvent;

/// Run the TUI. This is the main entry point for `truffle` (bare command).
pub async fn run(config: &TruffleConfig) -> Result<(), String> {
    // Redirect stderr to a log file BEFORE starting the daemon or TUI.
    redirect_stderr_to_log();

    // Check for first-run (no config file exists)
    let is_first_run = !crate::config::TruffleConfig::config_exists();

    let (server, config) = if is_first_run {
        // Run the onboarding wizard — returns started server + updated config
        onboarding::run(config).await?
    } else {
        // Normal startup: kill existing daemon, start in-process
        kill_existing_daemon().await;
        let server = DaemonServer::start(config).await?;
        (server, config.clone())
    };

    let node = server.node().clone();

    // Get the offer channel for interactive accept/reject instead of auto_accept.
    // Create the output directory ahead of time so accepted files have a home.
    let output_dir = dirs::download_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp"))
        .join("truffle")
        .to_string_lossy()
        .to_string();
    if let Err(e) = std::fs::create_dir_all(&output_dir) {
        tracing::error!(
            dir = output_dir.as_str(),
            "Failed to create output dir: {e}"
        );
    }
    // Restrict PULL_REQUEST serving to the download directory (deny-by-default).
    if let Err(e) = node.file_transfer().add_pull_root(&output_dir) {
        tracing::warn!(dir = output_dir.as_str(), "Pull serving disabled: {e}");
    }
    let offer_rx = node.file_transfer().offer_channel(node.clone()).await;

    // Subscribe to peer events, chat messages, and file transfer events
    let peer_rx = server.subscribe_peer_events();
    let chat_rx = node.subscribe("chat");
    let ft_rx = node.file_transfer().subscribe();

    // Create app state and load command history
    let mut app = AppState::new(node.clone(), config.clone());
    app.load_history();

    // Populate initial peer list from current Node state.
    populate_peers(&node, &mut app).await;

    // Spawn event collectors — also get the sender for /cp to push transfer progress
    let (event_tx, mut event_rx) =
        event::spawn_event_collectors(peer_rx, chat_rx, ft_rx, Some(offer_rx));

    // Schedule a delayed re-poll of peers. The sidecar's WatchIPNBus may not
    // have delivered its first PeersReceived event yet at startup, and the
    // broadcast subscription may have missed early Joined events (race between
    // DaemonServer::start and subscribe_peer_events). Re-polling after a few
    // seconds catches peers discovered during this window.
    {
        let node_clone = node.clone();
        let tx = event_tx.clone();
        tokio::spawn(async move {
            for delay_secs in [3, 8, 15] {
                tokio::time::sleep(tokio::time::Duration::from_secs(delay_secs)).await;
                let peers = node_clone.peers().await;
                for peer in peers {
                    if peer.online {
                        // Re-emit as a synthetic Joined event so the TUI updates.
                        // Reconstruct a hello-envelope identity from the Peer
                        // so downstream UI sees the user-facing device_name.
                        let identity = peer.device_id.as_ref().map(|uid| {
                            truffle_core::session::PeerIdentity {
                                app_id: String::new(),
                                device_id: uid.clone(),
                                device_name: peer
                                    .device_name
                                    .clone()
                                    .unwrap_or_else(|| peer.display_name.clone()),
                                os: peer.os.clone().unwrap_or_default(),
                                tailscale_id: peer.tailscale_id.clone(),
                            }
                        });
                        let state = truffle_core::session::PeerState {
                            id: peer.tailscale_id.clone(),
                            generation: peer.generation,
                            name: peer.hostname.clone(),
                            ip: peer.ip,
                            online: peer.online,
                            ws_connected: peer.ws_connected,
                            connection_type: peer.connection_type.clone(),
                            os: peer.os.clone(),
                            last_seen: peer.last_seen.clone(),
                            identity,
                            identity_suppressed: false,
                            login_name: peer.login_name.clone(),
                        };
                        let _ = tx.send(event::AppEvent::PeerEvent(
                            truffle_core::session::PeerEvent::Updated(state),
                        ));
                    }
                }
            }
        });
    }

    // Initialize terminal
    let mut terminal = init_terminal().map_err(|e| format!("Failed to init terminal: {e}"))?;

    // Set up panic hook to restore terminal.
    // Only disable raw mode — don't try to LeaveAlternateScreen because
    // that locks stdout which may deadlock if the panic occurred during draw().
    // The terminal emulator will restore the primary screen when the process exits.
    let original_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        original_hook(info);
    }));

    // Spawn the IPC accept loop as a background task.
    // Note: server.run() internally sets up Ctrl+C / SIGTERM handlers that
    // trigger its shutdown. In TUI mode, those signals will also cause the
    // IPC loop to exit gracefully, which is fine — our TUI event loop will
    // also see the Ctrl+C via crossterm and set should_quit.
    let ipc_handle = tokio::spawn(async move {
        if let Err(e) = server.run().await {
            tracing::error!("IPC server error: {e}");
        }
        // server.run() calls cleanup() internally on shutdown
    });

    // Main render loop
    loop {
        terminal
            .draw(|frame| ui::render(frame, &app))
            .map_err(|e| format!("Draw error: {e}"))?;

        match event_rx.recv().await {
            Some(AppEvent::Key(key)) => {
                // File picker, autocomplete, and transfer dialog handle Enter themselves,
                // so route to handle_key first when they're active.
                if key.code == KeyCode::Enter
                    && app.file_picker.is_none()
                    && app.autocomplete.is_none()
                    && app.transfer_dialog.is_none()
                {
                    // Normal submit — no overlay is open
                    let input = app.input.trim().to_string();
                    if !input.is_empty() {
                        app.history.push(input.clone());
                        app.save_history_entry(&input);
                        app.history_index = None;
                        app.input.clear();
                        app.cursor_pos = 0;
                        handle_input_async(&mut app, &input, event_tx.clone()).await;
                    }
                } else {
                    handle_key(&mut app, key);
                }
            }
            Some(event) => handle_event(&mut app, event),
            None => break,
        }

        if app.should_quit {
            break;
        }
    }

    // Cleanup: restore terminal first (so error messages are visible)
    let _ = restore_terminal();

    // Abort the IPC server task (it handles its own cleanup via Drop/shutdown)
    ipc_handle.abort();
    let _ = ipc_handle.await;

    Ok(())
}

/// Handle an incoming event.
fn handle_event(app: &mut AppState, event: AppEvent) {
    match event {
        AppEvent::Key(key) => handle_key(app, key),
        AppEvent::Resize(_, _) => {
            // ratatui handles resize automatically on next draw
        }
        AppEvent::PeerEvent(peer_event) => handle_peer_event(app, peer_event),
        AppEvent::IncomingMessage {
            from_id,
            from_name: _,
            text,
        } => {
            // msg.from is the WhoIs Tailscale id (RFC 022 §7.5) — match cache
            // by tailscale_id (or ULID if ever passed).
            let display_name = app
                .peers
                .iter()
                .find(|p| p.matches_id(&from_id))
                .map(|p| p.device_name.clone())
                .unwrap_or_else(|| from_id.clone());

            // Toast if scrolled up (user would miss the message)
            if !app.auto_scroll {
                app.notifications.push_back(app::Toast {
                    text: format!("{display_name}: {text}"),
                    created_at: std::time::Instant::now(),
                });
            }

            app.push_item(app::DisplayItem::ChatIncoming {
                time: chrono::Local::now(),
                from: display_name,
                text,
            });
        }
        AppEvent::TransferProgress {
            file_name,
            percent,
            speed_bps,
        } => {
            // Update the last matching FileTransfer item in-place
            for item in app.items.iter_mut().rev() {
                if let app::DisplayItem::FileTransfer {
                    file_name: ref name,
                    ref mut status,
                    ..
                } = item
                {
                    if name == &file_name {
                        *status = app::TransferStatus::InProgress { percent, speed_bps };
                        break;
                    }
                }
            }
        }
        AppEvent::TransferComplete {
            file_name,
            size: _,
            sha256,
        } => {
            for item in app.items.iter_mut().rev() {
                if let app::DisplayItem::FileTransfer {
                    file_name: ref name,
                    ref mut status,
                    ..
                } = item
                {
                    if name == &file_name {
                        *status = app::TransferStatus::Complete {
                            sha256: sha256.clone(),
                        };
                        break;
                    }
                }
            }
        }
        AppEvent::TransferFailed { file_name, reason } => {
            for item in app.items.iter_mut().rev() {
                if let app::DisplayItem::FileTransfer {
                    file_name: ref name,
                    ref mut status,
                    ..
                } = item
                {
                    if name == &file_name {
                        *status = app::TransferStatus::Failed {
                            reason: reason.clone(),
                        };
                        break;
                    }
                }
            }
        }
        AppEvent::FileOfferReceived { offer, responder } => {
            handle_file_offer_received(app, offer, responder);
        }
        AppEvent::Tick => {
            // Expire old toasts (4 second lifetime)
            let now = std::time::Instant::now();
            while let Some(front) = app.notifications.front() {
                if now.duration_since(front.created_at).as_secs() >= 4 {
                    app.notifications.pop_front();
                } else {
                    break;
                }
            }

            // Timeout file transfer dialog after 60 seconds
            if let Some(dialog) = &app.transfer_dialog {
                if dialog.created_at.elapsed() > std::time::Duration::from_secs(60) {
                    // Auto-reject and close
                    let Some(mut dialog) = app.transfer_dialog.take() else {
                        return;
                    };
                    if let Some(responder) = dialog.responder.take() {
                        responder.reject("timed out (60s)");
                    }
                    app.push_item(app::DisplayItem::System {
                        time: chrono::Local::now(),
                        text: format!(
                            "  File offer from {} timed out ({})",
                            dialog.offer.from_name, dialog.offer.file_name,
                        ),
                        level: app::SystemLevel::Warning,
                    });
                    // Show next queued offer if any
                    show_next_pending_offer(app);
                }
            }
        }
    }
}

/// Handle a key press.
fn handle_key(app: &mut AppState, key: KeyEvent) {
    // If transfer dialog is open, capture ALL keys
    if app.transfer_dialog.is_some() {
        handle_transfer_dialog_key(app, key);
        return;
    }

    // If file picker is open, route all keys to it
    if app.file_picker.is_some() {
        match key.code {
            KeyCode::Esc => {
                app.file_picker = None;
                // Reset input to "/" so the command list shows again
                app.input = "/".to_string();
                app.cursor_pos = 1;
                app.update_autocomplete();
            }
            KeyCode::Enter => {
                // Check if current selection is a file or directory
                let is_file = app
                    .file_picker
                    .as_ref()
                    .map(|e| !e.current().is_dir)
                    .unwrap_or(false);

                if is_file {
                    if let Some(path) = app.close_file_picker_with_selection() {
                        // Check if we're in a transfer dialog SaveAs mode
                        if let Some(ref mut dialog) = app.transfer_dialog {
                            if dialog.phase == app::TransferDialogPhase::SaveAs {
                                dialog.save_path_input = path;
                                dialog.save_path_cursor = dialog.save_path_input.chars().count();
                                return;
                            }
                        }
                        // Normal /cp mode — insert path into input
                        if !app.input.starts_with("/cp ") {
                            app.input = "/cp ".to_string();
                            app.cursor_pos = 4;
                        }
                        let byte_pos = char_to_byte_pos(&app.input, app.cursor_pos);
                        app.input.insert_str(byte_pos, &path);
                        app.cursor_pos += path.chars().count();
                    }
                } else {
                    // Directory — in SaveAs mode, use as save directory; otherwise navigate
                    if let Some(ref mut dialog) = app.transfer_dialog {
                        if dialog.phase == app::TransferDialogPhase::SaveAs {
                            if let Some(ref explorer) = app.file_picker {
                                let dir_path =
                                    explorer.current().path.to_string_lossy().to_string();
                                dialog.save_path_input =
                                    format!("{}/{}", dir_path, dialog.offer.file_name);
                                dialog.save_path_cursor = dialog.save_path_input.chars().count();
                            }
                            app.file_picker = None;
                            return;
                        }
                    }
                    // Normal mode — let explorer navigate into it
                    let event = crossterm::event::Event::Key(key);
                    if let Some(ref mut explorer) = app.file_picker {
                        let _ = explorer.handle(&event);
                    }
                }
            }
            _ => {
                // Forward the event to ratatui-explorer
                let event = crossterm::event::Event::Key(key);
                if let Some(ref mut explorer) = app.file_picker {
                    let _ = explorer.handle(&event);
                }
            }
        }
        return;
    }

    // If autocomplete is visible, intercept Enter/Tab/Esc/Up/Down
    if app.autocomplete.is_some() {
        match key.code {
            KeyCode::Enter | KeyCode::Tab => {
                let was_command = matches!(
                    app.autocomplete.as_ref().map(|a| &a.kind),
                    Some(app::AutocompleteKind::Command)
                );
                app.accept_autocomplete();
                if was_command {
                    // Re-evaluate — command completion may lead to further input
                    app.update_autocomplete();
                    // Auto-open file picker when /cp is selected
                    if app.input.starts_with("/cp ") || app.input == "/cp" {
                        app.open_file_picker();
                    }
                } else {
                    // Device completion — dismiss the list
                    app.autocomplete = None;
                }
                return;
            }
            KeyCode::Esc => {
                app.autocomplete = None;
                return;
            }
            KeyCode::Up => {
                app.autocomplete_prev();
                return;
            }
            KeyCode::Down => {
                app.autocomplete_next();
                return;
            }
            _ => {
                // Fall through to normal key handling, then update autocomplete
            }
        }
    }

    match key.code {
        // Ctrl+C: quit
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.should_quit = true;
        }
        // Esc: dismiss autocomplete or do nothing
        KeyCode::Esc => {
            app.autocomplete = None;
        }
        // Basic text input
        // NOTE: cursor_pos is a *char* index (not byte index).
        KeyCode::Char(c) => {
            let byte_pos = char_to_byte_pos(&app.input, app.cursor_pos);
            app.input.insert(byte_pos, c);
            app.cursor_pos += 1;
        }
        KeyCode::Backspace if app.cursor_pos > 0 => {
            app.cursor_pos -= 1;
            let byte_pos = char_to_byte_pos(&app.input, app.cursor_pos);
            app.input.remove(byte_pos);
        }
        KeyCode::Delete => {
            let char_count = app.input.chars().count();
            if app.cursor_pos < char_count {
                let byte_pos = char_to_byte_pos(&app.input, app.cursor_pos);
                app.input.remove(byte_pos);
            }
        }
        KeyCode::Left => {
            app.cursor_pos = app.cursor_pos.saturating_sub(1);
        }
        KeyCode::Right => {
            let char_count = app.input.chars().count();
            if app.cursor_pos < char_count {
                app.cursor_pos += 1;
            }
        }
        KeyCode::Home => {
            app.cursor_pos = 0;
        }
        KeyCode::End => {
            app.cursor_pos = app.input.chars().count();
            // Also scroll to bottom if in feed
            app.scroll_to_bottom();
        }
        KeyCode::Enter => {
            // Enter is handled in the main loop (async command dispatch)
        }
        KeyCode::Tab => {
            // Tab without autocomplete visible — try to trigger it
            app.update_autocomplete();
            if app.autocomplete.is_some() {
                app.accept_autocomplete();
            }
        }
        KeyCode::Up if !app.history.is_empty() => {
            let idx = match app.history_index {
                None => app.history.len() - 1,
                Some(i) => i.saturating_sub(1),
            };
            app.history_index = Some(idx);
            app.input = app.history[idx].clone();
            app.cursor_pos = app.input.chars().count();
        }
        KeyCode::Down => {
            if let Some(idx) = app.history_index {
                if idx + 1 < app.history.len() {
                    let next = idx + 1;
                    app.history_index = Some(next);
                    app.input = app.history[next].clone();
                    app.cursor_pos = app.input.chars().count();
                } else {
                    app.history_index = None;
                    app.input.clear();
                    app.cursor_pos = 0;
                }
            }
        }
        KeyCode::PageUp => {
            app.auto_scroll = false;
            app.scroll_offset += 10;
            let max_offset = app.items.len().saturating_sub(1);
            if app.scroll_offset > max_offset {
                app.scroll_offset = max_offset;
            }
        }
        KeyCode::PageDown => {
            if app.scroll_offset > 10 {
                app.scroll_offset -= 10;
            } else {
                app.scroll_to_bottom();
            }
        }
        _ => {}
    }

    // Update autocomplete after every keystroke that modifies input
    match key.code {
        KeyCode::Char(_) | KeyCode::Backspace | KeyCode::Delete => {
            app.update_autocomplete();
            // Auto-open file picker when input is exactly "/cp "
            if app.input == "/cp " && app.file_picker.is_none() {
                app.open_file_picker();
            }
        }
        _ => {}
    }
}

/// Handle text submitted from the input bar (async — commands may do I/O).
async fn handle_input_async(
    app: &mut AppState,
    input: &str,
    event_tx: tokio::sync::mpsc::UnboundedSender<event::AppEvent>,
) {
    if input.starts_with('/') {
        match commands::dispatch(input, app, event_tx).await {
            commands::CommandResult::Quit => {
                app.should_quit = true;
            }
            commands::CommandResult::Items(items) => {
                for item in items {
                    app.push_item(item);
                }
            }
            commands::CommandResult::Handled => {
                // Command already pushed its own items to the feed
            }
        }
    } else {
        // Plain text without / prefix
        app.push_item(app::DisplayItem::System {
            time: chrono::Local::now(),
            text: "  Type / for commands. Available: /send /broadcast /cp /exit".to_string(),
            level: app::SystemLevel::Info,
        });
    }
}

/// Handle a peer event from the mesh.
fn handle_peer_event(app: &mut AppState, event: truffle_core::session::PeerEvent) {
    use truffle_core::session::PeerEvent;

    match event {
        PeerEvent::Joined(state) | PeerEvent::Identity(state) | PeerEvent::Updated(state) => {
            let peer_view: truffle_core::Peer = state.clone().into();
            let name = peer_view.display_name.clone();
            // Honest Option — never stuff the Tailscale id into device_id.
            let device_id = peer_view.device_id.clone();
            let tailscale_id = peer_view.tailscale_id.clone();
            let was_online = app
                .peers
                .iter()
                .find(|p| p.tailscale_id == tailscale_id)
                .map(|p| p.online);
            if let Some(peer) = app
                .peers
                .iter_mut()
                .find(|p| p.tailscale_id == tailscale_id)
            {
                // Only overwrite device_id when we learned a real ULID; keep a
                // previously known ULID if this event has no identity yet.
                if device_id.is_some() {
                    peer.device_id = device_id;
                }
                peer.online = state.online;
                peer.ip = state.ip.to_string();
                peer.device_name = name.clone();
                if !state.connection_type.is_empty() {
                    peer.connection = Some(state.connection_type.clone());
                }
            } else {
                app.peers.push(app::PeerInfo {
                    device_id,
                    device_name: name.clone(),
                    tailscale_id,
                    ip: state.ip.to_string(),
                    online: state.online,
                    connection: if state.connection_type.is_empty() {
                        None
                    } else {
                        Some(state.connection_type.clone())
                    },
                });
            }
            // Feed line only for true joins (new online peer appearance).
            if was_online.is_none() && state.online {
                app.push_item(app::DisplayItem::PeerEvent {
                    time: chrono::Local::now(),
                    kind: app::PeerEventKind::Joined,
                    peer_name: name,
                    detail: state.ip.to_string(),
                });
            } else if was_online == Some(true) && !state.online {
                app.push_item(app::DisplayItem::PeerEvent {
                    time: chrono::Local::now(),
                    kind: app::PeerEventKind::Disconnected,
                    peer_name: name,
                    detail: String::new(),
                });
            } else if was_online == Some(false) && state.online {
                app.push_item(app::DisplayItem::PeerEvent {
                    time: chrono::Local::now(),
                    kind: app::PeerEventKind::Connected,
                    peer_name: name,
                    detail: state.ip.to_string(),
                });
            }
        }
        PeerEvent::Left(state) => {
            // Carries the departed entry's final state; key rows by the
            // Tailscale stable id, same as WsConnected/WsDisconnected.
            let id = state.id;
            let name = app
                .peers
                .iter()
                .find(|p| p.tailscale_id == id)
                .map(|p| p.device_name.clone())
                .unwrap_or_else(|| id.clone());
            if let Some(peer) = app.peers.iter_mut().find(|p| p.tailscale_id == id) {
                peer.online = false;
            }
            app.push_item(app::DisplayItem::PeerEvent {
                time: chrono::Local::now(),
                kind: app::PeerEventKind::Left,
                peer_name: name,
                detail: String::new(),
            });
        }
        PeerEvent::WsConnected(id) => {
            // WsConnected/WsDisconnected carry the Tailscale stable ID
            // because the session layer only knows that at the moment of
            // the event — look up PeerInfo by its tailscale_id escape hatch.
            let name = app
                .peers
                .iter()
                .find(|p| p.tailscale_id == id)
                .map(|p| p.device_name.clone())
                .unwrap_or_else(|| id.clone());
            app.push_item(app::DisplayItem::PeerEvent {
                time: chrono::Local::now(),
                kind: app::PeerEventKind::Connected,
                peer_name: name,
                detail: String::new(),
            });
        }
        PeerEvent::WsDisconnected(id) => {
            let name = app
                .peers
                .iter()
                .find(|p| p.tailscale_id == id)
                .map(|p| p.device_name.clone())
                .unwrap_or_else(|| id.clone());
            app.push_item(app::DisplayItem::PeerEvent {
                time: chrono::Local::now(),
                kind: app::PeerEventKind::Disconnected,
                peer_name: name,
                detail: String::new(),
            });
        }
        PeerEvent::AuthRequired { url } => {
            // During normal TUI operation, auth shouldn't be needed.
            // Log it in case it happens (e.g., key rotation).
            tracing::info!("auth required during TUI session: {url}");
        }
    }
}

/// Whether this offer's sender is on the auto-accept list.
///
/// `offer.from_peer` is the WhoIs Tailscale id (RFC 022 §7.5). The list may
/// also contain legacy ULID entries from pre-022 configs — accept those too.
fn auto_accepts_offer(app: &AppState, from_peer: &str) -> bool {
    if app
        .config
        .auto_accept_peers
        .iter()
        .any(|id| id == from_peer)
    {
        return true;
    }
    app.peers.iter().any(|p| {
        p.tailscale_id == from_peer
            && p.device_id
                .as_ref()
                .is_some_and(|uid| app.config.auto_accept_peers.iter().any(|id| id == uid))
    })
}

/// Handle an incoming file offer: show dialog or queue it.
fn handle_file_offer_received(
    app: &mut AppState,
    offer: truffle_core::file_transfer::types::FileOffer,
    responder: truffle_core::file_transfer::types::OfferResponder,
) {
    if auto_accepts_offer(app, &offer.from_peer) {
        let save_path = if offer.suggested_path.is_empty() || offer.suggested_path == "." {
            let output_dir = dirs::download_dir()
                .unwrap_or_else(|| std::path::PathBuf::from("/tmp"))
                .join("truffle")
                .to_string_lossy()
                .to_string();
            format!("{}/{}", output_dir, offer.file_name)
        } else {
            offer.suggested_path.clone()
        };
        let _peer_name = app
            .peers
            .iter()
            .find(|p| p.matches_id(&offer.from_peer))
            .map(|p| p.device_name.clone())
            .unwrap_or_else(|| offer.from_name.clone());
        // Add FileTransfer item so receive progress can update it
        app.push_item(app::DisplayItem::FileTransfer {
            time: chrono::Local::now(),
            direction: app::TransferDirection::Receive,
            file_name: offer.file_name.clone(),
            size: offer.size,
            status: app::TransferStatus::InProgress {
                percent: 0.0,
                speed_bps: 0.0,
            },
        });
        responder.accept(&save_path);
        return;
    }

    // If no dialog is open, show it; otherwise queue it
    if app.transfer_dialog.is_none() {
        show_offer_dialog(app, offer, responder);
    } else {
        app.pending_offers.push_back((offer, responder));
    }
}

/// Show a file offer as a dialog.
fn show_offer_dialog(
    app: &mut AppState,
    offer: truffle_core::file_transfer::types::FileOffer,
    responder: truffle_core::file_transfer::types::OfferResponder,
) {
    let save_path = if offer.suggested_path.is_empty() || offer.suggested_path == "." {
        let output_dir = dirs::download_dir()
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp"))
            .join("truffle")
            .to_string_lossy()
            .to_string();
        format!("{}/{}", output_dir, offer.file_name)
    } else {
        offer.suggested_path.clone()
    };
    let cursor = save_path.chars().count();

    app.transfer_dialog = Some(app::TransferDialogState {
        offer,
        responder: Some(responder),
        phase: app::TransferDialogPhase::Prompt,
        save_path_input: save_path,
        save_path_cursor: cursor,
        created_at: std::time::Instant::now(),
    });
}

/// Show the next pending offer as a dialog, if any.
fn show_next_pending_offer(app: &mut AppState) {
    if let Some((offer, responder)) = app.pending_offers.pop_front() {
        show_offer_dialog(app, offer, responder);
    }
}

/// Handle key events when the transfer dialog is open.
fn handle_transfer_dialog_key(app: &mut AppState, key: KeyEvent) {
    let phase = app
        .transfer_dialog
        .as_ref()
        .map(|d| d.phase.clone())
        .unwrap_or(app::TransferDialogPhase::Prompt);

    match phase {
        app::TransferDialogPhase::Prompt => {
            match key.code {
                KeyCode::Char('a') | KeyCode::Char('A') => {
                    // Check if file already exists
                    let path_exists = app
                        .transfer_dialog
                        .as_ref()
                        .map(|d| std::path::Path::new(&d.save_path_input).exists())
                        .unwrap_or(false);

                    if path_exists {
                        // Ask for overwrite confirmation
                        if let Some(ref mut dialog) = app.transfer_dialog {
                            dialog.phase = app::TransferDialogPhase::OverwriteConfirm;
                        }
                    } else {
                        // No conflict — accept directly
                        accept_current_offer(app);
                    }
                }
                KeyCode::Char('s') | KeyCode::Char('S') => {
                    // Switch to save-as mode and open file picker immediately
                    if let Some(ref mut dialog) = app.transfer_dialog {
                        dialog.phase = app::TransferDialogPhase::SaveAs;
                    }
                    app.open_file_picker();
                }
                KeyCode::Char('r') | KeyCode::Char('R') => {
                    // Reject
                    let Some(mut dialog) = app.transfer_dialog.take() else {
                        return;
                    };
                    if let Some(responder) = dialog.responder.take() {
                        responder.reject("rejected by user");
                    }
                    app.push_item(app::DisplayItem::System {
                        time: chrono::Local::now(),
                        text: format!(
                            "  \u{274C} Rejected {} from {}",
                            dialog.offer.file_name, dialog.offer.from_name,
                        ),
                        level: app::SystemLevel::Warning,
                    });
                    show_next_pending_offer(app);
                }
                KeyCode::Char('d') | KeyCode::Char('D') => {
                    // Accept and add peer to auto-accept list
                    let Some(mut dialog) = app.transfer_dialog.take() else {
                        return;
                    };
                    let save_path = dialog.save_path_input.clone();
                    // Store the Tailscale routing key (offer attribution).
                    let peer_id = dialog.offer.from_peer.clone();
                    let peer_name = app
                        .peers
                        .iter()
                        .find(|p| p.matches_id(&peer_id))
                        .map(|p| p.device_name.clone())
                        .unwrap_or_else(|| dialog.offer.from_name.clone());

                    if let Some(responder) = dialog.responder.take() {
                        responder.accept(&save_path);
                    }

                    // Add to auto-accept list and save config
                    if !app.config.auto_accept_peers.contains(&peer_id) {
                        app.config.auto_accept_peers.push(peer_id);
                        if let Err(e) = app.config.save(None) {
                            tracing::error!("Failed to save config: {e}");
                        }
                    }

                    app.push_item(app::DisplayItem::System {
                        time: chrono::Local::now(),
                        text: format!(
                            "  \u{2705} Accepted {} \u{2192} {}",
                            dialog.offer.file_name, save_path,
                        ),
                        level: app::SystemLevel::Success,
                    });
                    app.push_item(app::DisplayItem::System {
                        time: chrono::Local::now(),
                        text: format!("  Future files from {} accepted automatically", peer_name,),
                        level: app::SystemLevel::Info,
                    });
                    show_next_pending_offer(app);
                }
                KeyCode::Esc => {
                    // Esc in prompt phase rejects (same as 'r')
                    let Some(mut dialog) = app.transfer_dialog.take() else {
                        return;
                    };
                    if let Some(responder) = dialog.responder.take() {
                        responder.reject("dismissed");
                    }
                    app.push_item(app::DisplayItem::System {
                        time: chrono::Local::now(),
                        text: format!(
                            "  \u{274C} Dismissed {} from {}",
                            dialog.offer.file_name, dialog.offer.from_name,
                        ),
                        level: app::SystemLevel::Warning,
                    });
                    show_next_pending_offer(app);
                }
                _ => {
                    // Ignore all other keys
                }
            }
        }
        app::TransferDialogPhase::SaveAs => {
            match key.code {
                KeyCode::Enter => {
                    // Check if file already exists at the edited path
                    let path_exists = app
                        .transfer_dialog
                        .as_ref()
                        .map(|d| std::path::Path::new(&d.save_path_input).exists())
                        .unwrap_or(false);

                    if path_exists {
                        if let Some(ref mut dialog) = app.transfer_dialog {
                            dialog.phase = app::TransferDialogPhase::OverwriteConfirm;
                        }
                    } else {
                        accept_current_offer(app);
                    }
                }
                KeyCode::Esc => {
                    // Back to prompt phase
                    if let Some(ref mut dialog) = app.transfer_dialog {
                        dialog.phase = app::TransferDialogPhase::Prompt;
                    }
                }
                KeyCode::Char(c) => {
                    if let Some(ref mut dialog) = app.transfer_dialog {
                        let byte_pos =
                            char_to_byte_pos(&dialog.save_path_input, dialog.save_path_cursor);
                        dialog.save_path_input.insert(byte_pos, c);
                        dialog.save_path_cursor += 1;
                    }
                }
                KeyCode::Backspace => {
                    if let Some(ref mut dialog) = app.transfer_dialog {
                        if dialog.save_path_cursor > 0 {
                            dialog.save_path_cursor -= 1;
                            let byte_pos =
                                char_to_byte_pos(&dialog.save_path_input, dialog.save_path_cursor);
                            dialog.save_path_input.remove(byte_pos);
                        }
                    }
                }
                KeyCode::Delete => {
                    if let Some(ref mut dialog) = app.transfer_dialog {
                        let char_count = dialog.save_path_input.chars().count();
                        if dialog.save_path_cursor < char_count {
                            let byte_pos =
                                char_to_byte_pos(&dialog.save_path_input, dialog.save_path_cursor);
                            dialog.save_path_input.remove(byte_pos);
                        }
                    }
                }
                KeyCode::Left => {
                    if let Some(ref mut dialog) = app.transfer_dialog {
                        dialog.save_path_cursor = dialog.save_path_cursor.saturating_sub(1);
                    }
                }
                KeyCode::Right => {
                    if let Some(ref mut dialog) = app.transfer_dialog {
                        let char_count = dialog.save_path_input.chars().count();
                        if dialog.save_path_cursor < char_count {
                            dialog.save_path_cursor += 1;
                        }
                    }
                }
                KeyCode::Home => {
                    if let Some(ref mut dialog) = app.transfer_dialog {
                        dialog.save_path_cursor = 0;
                    }
                }
                KeyCode::End => {
                    if let Some(ref mut dialog) = app.transfer_dialog {
                        dialog.save_path_cursor = dialog.save_path_input.chars().count();
                    }
                }
                KeyCode::Tab => {
                    // Open file picker for directory selection
                    app.open_file_picker();
                }
                _ => {}
            }
        }
        app::TransferDialogPhase::OverwriteConfirm => {
            match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    // User confirmed overwrite
                    accept_current_offer(app);
                }
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    // Cancel — go back to prompt
                    if let Some(ref mut dialog) = app.transfer_dialog {
                        dialog.phase = app::TransferDialogPhase::Prompt;
                    }
                }
                _ => {}
            }
        }
    }
}

/// Accept the current offer and show next pending.
fn accept_current_offer(app: &mut AppState) {
    let Some(mut dialog) = app.transfer_dialog.take() else {
        return;
    };
    let save_path = dialog.save_path_input.clone();
    if let Some(responder) = dialog.responder.take() {
        responder.accept(&save_path);
    }
    // Add a FileTransfer item so receive progress can update it in-place
    app.push_item(app::DisplayItem::FileTransfer {
        time: chrono::Local::now(),
        direction: app::TransferDirection::Receive,
        file_name: dialog.offer.file_name.clone(),
        size: dialog.offer.size,
        status: app::TransferStatus::InProgress {
            percent: 0.0,
            speed_bps: 0.0,
        },
    });
    show_next_pending_offer(app);
}

/// Kill an existing background daemon so the TUI can take over.
async fn kill_existing_daemon() {
    let client = crate::daemon::client::DaemonClient::new();
    if client.is_daemon_running() {
        let _ = client
            .request(
                crate::daemon::protocol::method::SHUTDOWN,
                serde_json::json!({}),
            )
            .await;
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
}

/// Populate app.peers from the current Node peer list.
async fn populate_peers(
    node: &std::sync::Arc<
        truffle_core::node::Node<truffle_core::network::tailscale::TailscaleProvider>,
    >,
    app: &mut AppState,
) {
    let current_peers = node.peers().await;
    for peer in current_peers {
        // RFC 022: key by tailscale_id; device_id is honest Option.
        let name = peer.display_name.clone();
        let device_id = peer.device_id.clone();
        let tailscale_id = peer.tailscale_id.clone();
        if let Some(existing) = app
            .peers
            .iter_mut()
            .find(|p| p.tailscale_id == tailscale_id)
        {
            if device_id.is_some() {
                existing.device_id = device_id;
            }
            existing.online = peer.online;
            existing.ip = peer.ip.to_string();
            existing.device_name = name;
            if !peer.connection_type.is_empty() {
                existing.connection = Some(peer.connection_type.clone());
            }
        } else {
            app.peers.push(app::PeerInfo {
                device_id,
                device_name: name,
                tailscale_id,
                ip: peer.ip.to_string(),
                online: peer.online,
                connection: if peer.connection_type.is_empty() {
                    None
                } else {
                    Some(peer.connection_type.clone())
                },
            });
        }
    }
}

/// Redirect stderr to a log file so sidecar output doesn't corrupt the TUI.
///
/// Log goes to `~/.config/truffle/tui.log`. The file handle is leaked
/// intentionally so the fd/handle stays valid for the process lifetime.
#[allow(unsafe_code)]
fn redirect_stderr_to_log() {
    let log_path = dirs::config_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join("truffle")
        .join("tui.log");

    if let Some(parent) = log_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path);

    #[cfg(unix)]
    if let Ok(f) = file {
        use std::os::unix::io::IntoRawFd;
        let fd = f.into_raw_fd(); // Leak the fd so it stays valid
        unsafe {
            libc::dup2(fd, libc::STDERR_FILENO);
        }
    }

    #[cfg(windows)]
    if let Ok(f) = file {
        use std::os::windows::io::IntoRawHandle;
        let handle = f.into_raw_handle(); // Leak the handle so it stays valid
        unsafe {
            windows_sys::Win32::System::Console::SetStdHandle(
                windows_sys::Win32::System::Console::STD_ERROR_HANDLE,
                handle as _,
            );
        }
    }
}

/// Convert a char index to a byte offset in a string.
fn char_to_byte_pos(s: &str, char_pos: usize) -> usize {
    s.char_indices()
        .nth(char_pos)
        .map(|(byte, _)| byte)
        .unwrap_or(s.len())
}

/// Initialize the terminal for TUI rendering.
fn init_terminal() -> io::Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    io::stdout().execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(io::stdout());
    let terminal = Terminal::new(backend)?;
    Ok(terminal)
}

/// Restore the terminal to its original state.
fn restore_terminal() -> io::Result<()> {
    disable_raw_mode()?;
    io::stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}
