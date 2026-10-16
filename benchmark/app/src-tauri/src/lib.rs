//! `truffle-bench-app`: the Tauri v2 shell of "truffle bench" (RFC 027 §11).
//!
//! A thin shell over the [`truffle_bench`] engine. Everything that measures
//! lives in the engine; this crate owns the window, the §11.2 commands, the
//! persisted settings and the process lifecycle:
//!
//! - [`commands`]: every §11.2 command and the `bench://event` name;
//! - [`state`]: the profile, settings, the hub and the run trackers;
//! - [`window`]: the main window, its theme and navigation guard;
//! - `macos`: the `NSProcessInfo` activity held while a run is active.
//!
//! Closing the window hides it (macOS); the hub keeps answering peers until
//! the app quits, and quitting cancels runs and stops the hub within 5 s.

use std::sync::Arc;
use std::time::Duration;

use tauri::{App, AppHandle, Emitter, Manager, RunEvent, Runtime, Window, WindowEvent};

pub mod commands;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(debug_assertions)]
mod selftest;
pub mod state;
pub mod window;

use state::{AppState, Emit, Launch, StateInit};

/// Quit waits at most this long for the hub to stop (§11.1).
const QUIT_BUDGET: Duration = Duration::from_secs(5);
/// If the UI never calls `ui_ready`, the window is shown anyway after this.
const SHOW_FALLBACK: Duration = Duration::from_secs(10);

/// Run the app (the binary's `main`).
pub fn run() {
    init_logging();
    let launch = match Launch::from_env() {
        Ok(l) => l,
        Err(e) => {
            eprintln!("truffle bench: {e}");
            std::process::exit(2);
        }
    };
    let app = tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .invoke_handler(commands::handler())
        .setup(move |app| setup(app, launch))
        .on_window_event(on_window_event)
        .build(tauri::generate_context!())
        .unwrap_or_else(|e| {
            eprintln!("truffle bench: could not start: {e}");
            std::process::exit(1);
        });
    app.run(on_run_event);
}

fn init_logging() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn,truffle_bench_app=info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

fn setup<R: Runtime>(app: &mut App<R>, launch: Launch) -> Result<(), Box<dyn std::error::Error>> {
    let base = app.path().app_data_dir()?;
    let data_dir = state::profile_data_dir(&base, launch.profile.as_deref());
    std::fs::create_dir_all(&data_dir)?;
    let handle = app.handle().clone();
    let emit: Emit = Arc::new(move |event| {
        if let Err(e) = handle.emit(commands::EVENT, event) {
            tracing::debug!(error = %e, "could not emit a hub event");
        }
    });
    let state = AppState::new(StateInit {
        profile: launch.profile.clone(),
        data_dir,
        bundled: !cfg!(dev),
        env_auth_key: launch.auth_key.clone(),
        emit,
    });
    tracing::info!(
        profile = launch.profile.as_deref().unwrap_or("-"),
        data_dir = %state.data_dir().display(),
        device = %state.settings().device_name,
        bundled = !cfg!(dev),
        "truffle bench"
    );
    app.manage(state.clone());
    window::create_main(app.handle(), state.settings().theme)?;

    // Safety net: never leave an invisible app if the UI fails to load.
    let handle = app.handle().clone();
    let watch = state.clone();
    tauri::async_runtime::spawn(async move {
        tokio::time::sleep(SHOW_FALLBACK).await;
        if !watch.ui_is_ready() {
            tracing::warn!("the UI did not report ready; showing the window anyway");
            window::show_main(&handle);
        }
    });

    state.autostart();
    #[cfg(debug_assertions)]
    if let Some(peer) = launch.selftest {
        selftest::spawn(state, peer);
    }
    Ok(())
}

fn on_window_event<R: Runtime>(window: &Window<R>, event: &WindowEvent) {
    if window.label() != window::MAIN {
        return;
    }
    let Some(state) = window.try_state::<AppState>() else {
        return;
    };
    match event {
        // macOS: closing hides; the responder keeps answering peers until ⌘Q.
        #[cfg(target_os = "macos")]
        WindowEvent::CloseRequested { api, .. } if !state.is_exiting() => {
            api.prevent_close();
            let _ = window.hide();
        }
        WindowEvent::Focused(focused) => state.foreground().set_focused(*focused),
        // Following the OS: keep the ground behind the webview in step.
        WindowEvent::ThemeChanged(theme)
            if state.settings().theme == truffle_bench::Theme::System =>
        {
            if let Some(w) = window.app_handle().get_webview_window(window::MAIN) {
                window::paint_ground(&w, *theme);
            }
        }
        _ => {}
    }
}

fn on_run_event<R: Runtime>(app: &AppHandle<R>, event: RunEvent) {
    match event {
        RunEvent::ExitRequested { api, .. } => {
            let Some(state) = app.try_state::<AppState>() else {
                return;
            };
            // The second request (our own `exit(0)`) goes through.
            if !state.begin_exit() {
                return;
            }
            api.prevent_exit();
            let state = state.inner().clone();
            let app = app.clone();
            tauri::async_runtime::spawn(async move {
                state.shutdown(QUIT_BUDGET).await;
                app.exit(0);
            });
        }
        // macOS ⌘Q, the Dock's Quit and a quit Apple Event terminate the
        // application directly (tao sends no ExitRequested for them): stop the
        // hub here, blocking the exiting main thread for at most the budget.
        // After an ExitRequested quit this returns at once.
        RunEvent::Exit => {
            if let Some(state) = app.try_state::<AppState>() {
                let state = state.inner().clone();
                tauri::async_runtime::block_on(state.shutdown(QUIT_BUDGET));
            }
        }
        #[cfg(target_os = "macos")]
        RunEvent::Reopen { .. } => window::show_main(app),
        _ => {}
    }
}

#[cfg(test)]
mod tests;
