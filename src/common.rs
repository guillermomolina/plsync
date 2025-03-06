use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;


/// Throttle access to an optional `io::Write` to the specified `Duration`
#[derive(Debug)]
pub struct Throttle {
    trigger: Arc<AtomicBool>,
}

impl Throttle {
    pub fn new(duration: Duration, initial_sleep: Option<Duration>) -> Self {
        let instance = Self {
            trigger: Default::default(),
        };

        let trigger = Arc::downgrade(&instance.trigger);
        std::thread::spawn(move || {
            if let Some(duration) = initial_sleep {
                std::thread::sleep(duration)
            }
            while let Some(t) = trigger.upgrade() {
                t.store(true, Ordering::Relaxed);
                std::thread::sleep(duration);
            }
        });

        instance
    }

    pub fn throttled<F>(&self, f: F)
    where
        F: FnOnce(),
    {
        if self.can_update() {
            f()
        }
    }

    /// Return `true` if we are not currently throttled.
    pub fn can_update(&self) -> bool {
        self.trigger.swap(false, Ordering::Relaxed)
    }
}
