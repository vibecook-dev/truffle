//! The main window (RFC 027 §11.1): created in Rust, hidden until the UI has
//! painted with its theme, with a navigation guard and no new windows.
//!
//! The UI calls `ui_ready` after its first themed paint. WebKit suspends
//! `requestAnimationFrame` in a window that has never been shown, so a UI
//! that waits for a frame before calling it would wait forever; the window is
//! therefore also shown [`UI_READY_GRACE`] after the page finishes loading.

use std::time::Duration;

use tauri::webview::{NewWindowResponse, PageLoadEvent};
use tauri::window::Color;
use tauri::{AppHandle, Manager, Runtime, Url, WebviewUrl, WebviewWindow, WebviewWindowBuilder};
use truffle_bench::Theme;

use crate::state::AppState;

/// The one window's label (`capabilities/default.json` names it).
pub const MAIN: &str = "main";

/// `--ground` in light mode (`#fafaf7`).
const GROUND_LIGHT: Color = Color(0xfa, 0xfa, 0xf7, 0xff);
/// `--ground` in dark mode (`#0a0a0a`).
const GROUND_DARK: Color = Color(0x0a, 0x0a, 0x0a, 0xff);

/// After the page has loaded, the window is shown this long later if the UI
/// has not called `ui_ready` yet.
pub const UI_READY_GRACE: Duration = Duration::from_millis(600);

/// The native theme for a setting: `None` follows the OS.
pub fn native_theme(theme: Theme) -> Option<tauri::Theme> {
    match theme {
        Theme::System => None,
        Theme::Light => Some(tauri::Theme::Light),
        Theme::Dark => Some(tauri::Theme::Dark),
    }
}

/// The ground colour for a native theme.
pub fn ground(theme: tauri::Theme) -> Color {
    match theme {
        tauri::Theme::Dark => GROUND_DARK,
        _ => GROUND_LIGHT,
    }
}

/// Whether a URL is the app itself: `tauri://localhost` (macOS, Linux),
/// `http(s)://tauri.localhost` (Windows), or in development the dev server's
/// origin.
pub fn is_app_url(url: &Url, dev_url: Option<&Url>) -> bool {
    match url.scheme() {
        "tauri" => url.host_str() == Some("localhost"),
        "http" | "https" if url.host_str() == Some("tauri.localhost") => true,
        _ => dev_url.is_some_and(|dev| dev.origin() == url.origin()),
    }
}

/// Open an `https` URL in the default browser; anything else is refused.
pub fn open_external(url: &Url) -> std::io::Result<()> {
    if url.scheme() != "https" {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "only https links open",
        ));
    }
    open::that_detached(url.as_str())
}

/// Create the main window, hidden, in the theme of `theme`.
pub fn create_main<R: Runtime>(
    app: &AppHandle<R>,
    theme: Theme,
) -> tauri::Result<WebviewWindow<R>> {
    // In development the dev server is the app; a bundle serves its own assets.
    let dev_url = if cfg!(dev) {
        app.config().build.dev_url.clone()
    } else {
        None
    };
    let builder = WebviewWindowBuilder::new(app, MAIN, WebviewUrl::default())
        .title("truffle bench")
        .inner_size(1360.0, 900.0)
        .min_inner_size(960.0, 640.0)
        .visible(false)
        .theme(native_theme(theme))
        .background_color(ground(native_theme(theme).unwrap_or(tauri::Theme::Light)))
        .on_navigation(move |url| {
            let ok = is_app_url(url, dev_url.as_ref());
            if !ok {
                tracing::warn!(%url, "blocked a navigation away from the app");
            }
            ok
        })
        .on_new_window(|url, _features| {
            if let Err(e) = open_external(&url) {
                tracing::warn!(%url, error = %e, "did not open a new-window link");
            }
            NewWindowResponse::Deny
        })
        .on_page_load(|window, payload| {
            tracing::debug!(url = %payload.url(), event = ?payload.event(), "page load");
            if !matches!(payload.event(), PageLoadEvent::Finished) {
                return;
            }
            let app = window.app_handle().clone();
            tauri::async_runtime::spawn(async move {
                tokio::time::sleep(UI_READY_GRACE).await;
                let waiting = app
                    .try_state::<AppState>()
                    .is_some_and(|s| !s.ui_is_ready());
                if waiting {
                    tracing::debug!("the page loaded but the UI has not called ui_ready; showing");
                    show_main(&app);
                }
            });
        });
    #[cfg(target_os = "macos")]
    let builder = builder
        .title_bar_style(tauri::TitleBarStyle::Overlay)
        .hidden_title(true)
        .traffic_light_position(tauri::LogicalPosition::new(16.0, 13.0));
    let window = builder.build()?;
    paint_ground(&window, window.theme().unwrap_or(tauri::Theme::Light));
    Ok(window)
}

/// Set the window background to the ground of `theme`.
pub fn paint_ground<R: Runtime>(window: &WebviewWindow<R>, theme: tauri::Theme) {
    if let Err(e) = window.set_background_color(Some(ground(theme))) {
        tracing::debug!(error = %e, "could not set the window background");
    }
}

/// Apply a theme setting to the native window (§11.1 Theme).
pub fn apply_theme<R: Runtime>(app: &AppHandle<R>, theme: Theme) {
    let Some(window) = app.get_webview_window(MAIN) else {
        return;
    };
    if let Err(e) = window.set_theme(native_theme(theme)) {
        tracing::debug!(error = %e, "could not set the window theme");
    }
    let actual = native_theme(theme)
        .or_else(|| window.theme().ok())
        .unwrap_or(tauri::Theme::Light);
    paint_ground(&window, actual);
}

/// Show and focus the main window.
pub fn show_main<R: Runtime>(app: &AppHandle<R>) {
    if let Some(window) = app.get_webview_window(MAIN) {
        let _ = window.unminimize();
        let _ = window.show();
        let _ = window.set_focus();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_app_origin_is_navigable() {
        let u = |s: &str| Url::parse(s).unwrap();
        let dev = u("http://127.0.0.1:47183/");
        assert!(is_app_url(&u("tauri://localhost/index.html"), None));
        assert!(is_app_url(&u("http://tauri.localhost/"), None));
        assert!(is_app_url(&u("http://127.0.0.1:47183/#/tcp"), Some(&dev)));
        assert!(!is_app_url(&u("http://127.0.0.1:47183/"), None));
        assert!(!is_app_url(&u("http://127.0.0.1:5173/"), Some(&dev)));
        assert!(!is_app_url(&u("https://example.com/"), Some(&dev)));
        assert!(!is_app_url(&u("tauri://evil/"), None));
    }

    #[test]
    fn external_links_must_be_https() {
        let u = |s: &str| Url::parse(s).unwrap();
        assert!(open_external(&u("http://example.com/")).is_err());
        assert!(open_external(&u("file:///etc/passwd")).is_err());
    }

    #[test]
    fn theme_settings_map_to_native_themes() {
        assert_eq!(native_theme(Theme::System), None);
        assert_eq!(native_theme(Theme::Dark), Some(tauri::Theme::Dark));
        assert_eq!(ground(tauri::Theme::Dark), GROUND_DARK);
        assert_eq!(ground(tauri::Theme::Light), GROUND_LIGHT);
    }
}
