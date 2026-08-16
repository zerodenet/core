use std::io;

#[cfg(target_os = "linux")]
#[path = "monitor/linux.rs"]
mod platform;
#[cfg(target_os = "macos")]
#[path = "monitor/macos.rs"]
mod platform;
#[cfg(target_os = "windows")]
#[path = "monitor/windows.rs"]
mod platform;

/// Event-driven notification that the host route topology may have changed.
///
/// Notifications are deliberately treated as invalidation hints. Consumers
/// must re-read the preferred route and compare desired state rather than
/// interpreting an individual platform event as authoritative.
#[derive(Debug)]
pub struct RouteChangeMonitor(platform::RouteChangeMonitor);

impl RouteChangeMonitor {
    pub fn new() -> io::Result<Self> {
        platform::RouteChangeMonitor::new().map(Self)
    }

    pub async fn changed(&mut self) -> io::Result<()> {
        self.0.changed().await
    }

    /// Discard notifications already queued after a debounce window. A later
    /// change remains observable, so callers can collapse bursts without an
    /// unbounded event backlog.
    pub fn coalesce(&mut self) -> io::Result<()> {
        self.0.coalesce()
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
mod platform {
    use std::io;

    #[derive(Debug)]
    pub(super) struct RouteChangeMonitor;

    impl RouteChangeMonitor {
        pub(super) fn new() -> io::Result<Self> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "route change monitoring is unsupported on this platform",
            ))
        }

        pub(super) async fn changed(&mut self) -> io::Result<()> {
            std::future::pending().await
        }

        pub(super) fn coalesce(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}

#[cfg(all(
    test,
    any(target_os = "linux", target_os = "macos", target_os = "windows")
))]
mod tests {
    use super::RouteChangeMonitor;

    #[test]
    fn platform_route_monitor_registration_is_releasable() {
        drop(RouteChangeMonitor::new().expect("register route change monitor"));
    }
}
