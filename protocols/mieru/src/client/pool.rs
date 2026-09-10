use super::ClientConnection;
use crate::inbound::multiplex::stream::MieruLogicalStream;
use std::{
    collections::BTreeMap,
    future::Future,
    io,
    sync::{Arc, Mutex},
};

mod policy;
mod slot;

pub use policy::{ClientPoolPolicy, ClientPoolSnapshot};
use slot::{DialGuard, DialPlan, Slot};

const MAX_KEYS: usize = 512;

/// Endpoint, credential identity, carrier and egress generation are isolated by
/// the adapter's pool lifetime and this opaque protocol-owned key.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PoolKey {
    generation: u64,
    profile: String,
    pub(crate) tag: String,
    pub(crate) server: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) udp: bool,
}
impl PoolKey {
    pub fn with_profile(mut self, profile: String) -> Self {
        self.profile = profile;
        self
    }
    pub fn with_generation(mut self, generation: u64) -> Self {
        self.generation = generation;
        self
    }
    pub fn new(
        tag: &str,
        server: &str,
        port: u16,
        username: &str,
        password: &str,
        udp: bool,
    ) -> Self {
        Self {
            generation: 0,
            profile: String::new(),
            tag: tag.into(),
            server: server.into(),
            port,
            username: username.into(),
            password: password.into(),
            udp,
        }
    }
}
impl std::fmt::Debug for PoolKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolKey")
            .field("tag", &self.tag)
            .field("server", &self.server)
            .field("port", &self.port)
            .field("udp", &self.udp)
            .finish_non_exhaustive()
    }
}
pub struct ClientPool {
    entries: Mutex<BTreeMap<PoolKey, Arc<Slot>>>,
    policy: ClientPoolPolicy,
}

impl Default for ClientPool {
    fn default() -> Self {
        Self::with_policy(ClientPoolPolicy::default())
            .expect("default mieru client pool policy must be valid")
    }
}
impl std::fmt::Debug for ClientPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientPool").finish_non_exhaustive()
    }
}
impl ClientPool {
    pub fn with_policy(policy: ClientPoolPolicy) -> io::Result<Self> {
        policy.validate()?;
        Ok(Self {
            entries: Mutex::new(BTreeMap::new()),
            policy,
        })
    }

    pub fn snapshot(&self) -> ClientPoolSnapshot {
        let entries = self.entries.lock().unwrap();
        let mut snapshot = ClientPoolSnapshot {
            identities: entries.len(),
            ..ClientPoolSnapshot::default()
        };
        for slot in entries.values() {
            slot.update_snapshot(&self.policy, &mut snapshot);
        }
        snapshot
    }

    /// Retire cached connections on reload. Existing streams retain their own
    /// connection; new requests cannot reuse the previous generation.
    pub fn clear(&self) {
        let mut entries = self.entries.lock().unwrap();
        for slot in entries.values() {
            slot.retire();
        }
        entries.clear();
    }
    pub async fn open<F, Fut>(&self, key: PoolKey, mut connect: F) -> io::Result<MieruLogicalStream>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = io::Result<Arc<ClientConnection>>>,
    {
        let slot = self.slot(key)?;
        let mut changed = slot.changed.subscribe();
        loop {
            let Some(connections) = slot.connections(&self.policy) else {
                return open_uncached(connect()).await;
            };
            if matches!(slot.plan_growth(&self.policy), DialPlan::Start) {
                let guard = DialGuard::new(slot.clone());
                match connect().await {
                    Ok(connection) => match connection.open().await {
                        Ok(stream) => {
                            guard.finish(Some(connection));
                            return Ok(stream);
                        }
                        Err(_) => guard.finish(None),
                    },
                    Err(_) => guard.finish(None),
                }
            }
            for connection in &connections {
                match connection.open().await {
                    Ok(stream) => return Ok(stream),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) if retryable_open_error(&error) => {
                        // Only OPEN has been attempted: no application bytes are replayed.
                        // Pointer identity prevents an old failure from removing a replacement.
                        slot.remove(connection);
                    }
                    Err(error) => return Err(error),
                }
            }
            match slot.plan_dial(&connections, &self.policy) {
                DialPlan::Start => {
                    let guard = DialGuard::new(slot.clone());
                    let connection = connect().await?;
                    let stream = connection.open().await?;
                    guard.finish(Some(connection));
                    return Ok(stream);
                }
                DialPlan::Wait => {
                    let _ = changed.changed().await;
                }
                DialPlan::Retry => {}
                DialPlan::Retired => return open_uncached(connect()).await,
                DialPlan::Capacity => return Err(io::ErrorKind::WouldBlock.into()),
            }
        }
    }

    fn slot(&self, key: PoolKey) -> io::Result<Arc<Slot>> {
        let mut entries = self.entries.lock().unwrap();
        entries.retain(|_, slot| Arc::strong_count(slot) > 1 || slot.has_live_work());
        if entries.len() >= MAX_KEYS && !entries.contains_key(&key) {
            return Err(io::Error::other("mieru pool endpoint capacity exceeded"));
        }
        Ok(entries
            .entry(key)
            .or_insert_with(|| Arc::new(Slot::new()))
            .clone())
    }
}

fn retryable_open_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset | io::ErrorKind::TimedOut
    )
}

async fn open_uncached<Fut>(connect: Fut) -> io::Result<MieruLogicalStream>
where
    Fut: Future<Output = io::Result<Arc<ClientConnection>>>,
{
    connect.await?.open().await
}
