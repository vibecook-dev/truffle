//! macOS process policy while a run is active (RFC 027 §11.1).
//!
//! App Nap and idle sleep would throttle a backgrounded app mid-measurement,
//! in either role. While any run is active the app holds an `NSProcessInfo`
//! activity (user-initiated, latency-critical, idle system sleep disabled).
//!
//! The activity is held with `performActivityWithOptions:reason:usingBlock:`
//! on a dedicated thread whose block waits until the hold is dropped. That
//! API is safe to call, unlike the `beginActivity`/`endActivity` pair, so
//! the crate needs no `unsafe`.

use std::sync::mpsc;
use std::thread;

use block2::RcBlock;
use objc2_foundation::{NSActivityOptions, NSProcessInfo, NSString};

/// A held process activity; dropping it ends the activity.
pub struct Activity {
    stop: Option<mpsc::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Activity {
    /// Begin an activity with the given reason (shown by `pmset -g assertions`).
    pub fn begin(reason: &str) -> Self {
        let (stop, wait) = mpsc::channel::<()>();
        let reason = reason.to_string();
        let thread = thread::Builder::new()
            .name("bench-activity".into())
            .spawn(move || {
                let options = NSActivityOptions::UserInitiated
                    | NSActivityOptions::LatencyCritical
                    | NSActivityOptions::IdleSystemSleepDisabled;
                // Returns when the sender is dropped (or sends).
                let block = RcBlock::new(move || {
                    let _ = wait.recv();
                });
                NSProcessInfo::processInfo().performActivityWithOptions_reason_usingBlock(
                    options,
                    &NSString::from_str(&reason),
                    &block,
                );
            });
        match thread {
            Ok(t) => Self {
                stop: Some(stop),
                thread: Some(t),
            },
            Err(e) => {
                tracing::warn!(error = %e, "could not start the process activity thread");
                Self {
                    stop: None,
                    thread: None,
                }
            }
        }
    }
}

impl Drop for Activity {
    fn drop(&mut self) {
        drop(self.stop.take());
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_begins_and_ends() {
        let a = Activity::begin("truffle bench test");
        assert!(a.thread.is_some());
        drop(a);
    }
}
