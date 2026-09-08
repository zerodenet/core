//! Authenticated TCP connection reuse; never replay application bytes on retry.
use super::{
    Hysteria2AuthenticatedConnection, Hysteria2OutboundOptionsRef, Hysteria2QuicProfile,
    Hysteria2Stream,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Mutex as AsyncMutex;
use zero_core::Session;
use zero_transport::{OutboundDatagramSocketFactory, RuntimeError, TcpRelayStream};

#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    tag: String,
    server: String,
    port: u16,
    password: String,
    server_name: Option<String>,
    fingerprint: Option<String>,
    insecure: bool,
    settings: crate::settings::Settings,
    egress_generation: u64,
}
struct Entry {
    connection: AsyncMutex<Option<Arc<Hysteria2AuthenticatedConnection>>>,
    touched: Mutex<Instant>,
}
#[derive(Clone, Default)]
pub struct Hysteria2ConnectionPool(Arc<Mutex<HashMap<Key, Arc<Entry>>>>);
impl std::fmt::Debug for Hysteria2ConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Hysteria2ConnectionPool")
    }
}
impl Hysteria2ConnectionPool {
    /// Retire cached connections on reload; live streams retain their guards.
    pub fn clear(&self) {
        self.0.lock().unwrap().clear();
    }
    fn entry(&self, key: Key) -> Arc<Entry> {
        let mut entries = self.0.lock().unwrap();
        if let Some(entry) = entries.get(&key) {
            *entry.touched.lock().unwrap() = Instant::now();
            return entry.clone();
        }
        if entries.len() >= 256 {
            if let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, e)| *e.touched.lock().unwrap())
                .map(|(k, _)| k.clone())
            {
                entries.remove(&oldest);
            }
        }
        let entry = Arc::new(Entry {
            connection: AsyncMutex::new(None),
            touched: Mutex::new(Instant::now()),
        });
        entries.insert(key.clone(), entry.clone());
        let weak = Arc::downgrade(&self.0);
        let weak_entry = Arc::downgrade(&entry);
        let idle = Duration::from_secs(key.settings.quic.max_idle_timeout_secs);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(idle).await;
                let (Some(pool), Some(entry)) = (weak.upgrade(), weak_entry.upgrade()) else {
                    return;
                };
                let mut entries = pool.lock().unwrap();
                if entry.touched.lock().unwrap().elapsed() >= idle {
                    if entries
                        .get(&key)
                        .is_some_and(|current| Arc::ptr_eq(current, &entry))
                    {
                        entries.remove(&key);
                    }
                    return;
                }
            }
        });
        entry
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn connect_pooled(
    pool: &Hysteria2ConnectionPool,
    tag: &str,
    session: &Session,
    server: &str,
    port: u16,
    options: Hysteria2OutboundOptionsRef<'_>,
    sockets: &OutboundDatagramSocketFactory,
) -> Result<TcpRelayStream, RuntimeError> {
    let entry = pool.entry(Key {
        tag: tag.into(),
        server: server.into(),
        port,
        password: options.password.into(),
        server_name: options.server_name.map(Into::into),
        fingerprint: options.client_fingerprint.map(Into::into),
        insecure: options.insecure,
        settings: options.settings,
        egress_generation: sockets.egress_generation(),
    });
    for attempt in 0..2 {
        let connection = {
            // Single-flight dialing/authentication. Cancellation leaves an empty slot.
            let mut slot = entry.connection.lock().await;
            if slot
                .as_ref()
                .is_some_and(|c| c.connection().close_reason().is_some())
            {
                *slot = None;
            }
            if slot.is_none() {
                let profile = crate::outbound_profile_from_config_password(
                    options.password,
                    options.client_fingerprint,
                );
                let quic = Hysteria2QuicProfile::from_parts(options.client_fingerprint)
                    .with_insecure(options.insecure)
                    .with_server_name(options.server_name)
                    .with_settings(options.settings);
                *slot = Some(
                    super::open_authenticated_hysteria2_quic_connection(
                        server, port, &profile, quic, sockets,
                    )
                    .await?,
                );
            }
            slot.as_ref().unwrap().clone()
        };
        match connection.connection().open_bi().await {
            Ok((send, recv)) => {
                let mut stream = Hysteria2Stream::with_connection_guard(send, recv, connection);
                crate::Hysteria2Outbound
                    .establish_tcp_connect(&mut stream, session)
                    .await
                    .map_err(RuntimeError::Core)?;
                return Ok(TcpRelayStream::new(stream));
            }
            Err(error) => {
                let mut slot = entry.connection.lock().await;
                if slot
                    .as_ref()
                    .is_some_and(|cached| Arc::ptr_eq(cached, &connection))
                {
                    *slot = None;
                }
                if attempt == 1 {
                    return Err(
                        std::io::Error::other(format!("hysteria2 open stream: {error}")).into(),
                    );
                }
            }
        }
    }
    unreachable!()
}

#[cfg(test)]
#[path = "tests/pool.rs"]
mod tests;
