mod owned;
use owned::Active;
pub use owned::{ProviderFactory, ProviderOptions};
use std::{
    collections::HashMap,
    io,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use zero_transport::browser_dialer::{BrowserDialer, BrowserDialerOptions, BrowserDialerServer};

#[derive(Clone, Default)]
pub(super) struct Registry(Arc<State>);

#[derive(Default)]
struct State {
    entries: Mutex<HashMap<zero_traits::BrowserDialerSettings, Arc<Entry>>>,
}

struct Entry {
    settings: zero_traits::BrowserDialerSettings,
    server: tokio::sync::Mutex<Option<BrowserDialerServer>>,
    active: Mutex<Option<BrowserDialer>>,
    retired: AtomicBool,
    owned: Mutex<Option<zero_traits::packet_metadata::PacketMetadataWeak<Active>>>,
    hosted: AtomicBool,
    // Accepted immutable recipes keep their exact entry available for a real
    // recovery build; they retain no listener or compiled provider.
    recipes: AtomicUsize,
}

#[derive(Clone)]
pub(in crate::transport) struct Access(Arc<Entry>, std::sync::Weak<State>);

impl std::fmt::Debug for Registry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("BrowserDialerRegistry").finish()
    }
}

impl std::fmt::Debug for Access {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("BrowserDialerAccess")
            .field(&self.0.settings)
            .finish()
    }
}

impl Registry {
    pub(super) fn access(&self, settings: zero_traits::BrowserDialerSettings) -> Access {
        let mut entries = self
            .0
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        Access(
            entries
                .entry(settings.clone())
                .or_insert_with(|| {
                    Arc::new(Entry {
                        settings,
                        server: tokio::sync::Mutex::new(None),
                        active: Mutex::new(None),
                        retired: AtomicBool::new(false),
                        owned: Mutex::new(None),
                        hosted: AtomicBool::new(false),
                        recipes: AtomicUsize::new(0),
                    })
                })
                .clone(),
            Arc::downgrade(&self.0),
        )
    }

    pub(super) fn retire(&self) {
        // A Kernel provider is owned/revoked by its actual Host component.
        // This library registry neither closes that provider nor replaces its
        // real Native/user/Scope/ACK receipts during another pool refresh.
        loop {
            let removed = {
                let mut entries = self
                    .0
                    .entries
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                entries
                    .extract_if(|_, entry| {
                        !entry.hosted.load(Ordering::Acquire)
                            && entry.recipes.load(Ordering::Acquire) == 0
                    })
                    .next()
            };
            let Some((_, entry)) = removed else { break };
            entry.retire();
        }
    }
}

impl Drop for State {
    fn drop(&mut self) {
        for entry in self
            .entries
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .drain()
            .map(|(_, entry)| entry)
        {
            if !entry.hosted.load(Ordering::Acquire) {
                entry.retire();
            }
        }
    }
}

impl Access {
    pub(in crate::transport) async fn dialer(&self) -> io::Result<BrowserDialer> {
        if self.0.retired.load(Ordering::Acquire) {
            return Err(retired_error());
        }
        // An explicit library view can share the actual installed provider,
        // but cannot create another listener after the Host takes ownership.
        if self.0.hosted.load(Ordering::Acquire) {
            let original = {
                self.0
                    .owned
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
            };
            return original
                .and_then(|original| original.upgrade())
                .ok_or_else(retired_error)?
                .dialer()
                .map_err(io::Error::other);
        }
        let mut server = self.0.server.lock().await;
        if self.0.hosted.load(Ordering::Acquire) {
            drop(server);
            let original = {
                self.0
                    .owned
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
            };
            return original
                .and_then(|original| original.upgrade())
                .ok_or_else(retired_error)?
                .dialer()
                .map_err(io::Error::other);
        }
        if self.0.retired.load(Ordering::Acquire) {
            return Err(retired_error());
        }
        if let Some(server) = server.as_ref() {
            return Ok(server.dialer());
        }
        let address = self.0.settings.listen.parse().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid Browser Dialer listen address: {error}"),
            )
        })?;
        let listener = BrowserDialerServer::bind(
            address,
            BrowserDialerOptions {
                idle_capacity: self.0.settings.idle_capacity as usize,
                task_timeout: Duration::from_millis(self.0.settings.task_timeout_ms),
                max_task_bytes: self.0.settings.max_task_bytes as usize,
                max_payload_bytes: self.0.settings.max_payload_bytes as usize,
            },
        )
        .await?;
        if self.0.retired.load(Ordering::Acquire) {
            return Err(retired_error());
        }
        tracing::info!(page_url = listener.page_url(), "Browser Dialer ready");
        let dialer = listener.dialer();
        *self
            .0
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(dialer.clone());
        *server = Some(listener);
        Ok(dialer)
    }

    #[cfg(test)]
    async fn page_url(&self) -> io::Result<String> {
        let _ = self.dialer().await?;
        let server = self.0.server.lock().await;
        Ok(server
            .as_ref()
            .expect("dialer initialization stores its listener")
            .page_url()
            .to_owned())
    }
}

impl Entry {
    fn retire(&self) {
        self.retired.store(true, Ordering::Release);
        if let Some(dialer) = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            dialer.close();
        }
        if let Ok(mut server) = self.server.try_lock() {
            server.take();
        }
    }
}

impl Drop for Entry {
    fn drop(&mut self) {
        self.retired.store(true, Ordering::Release);
        if let Some(dialer) = self
            .active
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            dialer.close();
        }
        self.server.get_mut().take();
    }
}

fn retired_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::Interrupted,
        "Browser Dialer retired by reload",
    )
}

#[cfg(test)]
#[path = "../../../tests/transport/browser_dialer.rs"]
mod tests;
