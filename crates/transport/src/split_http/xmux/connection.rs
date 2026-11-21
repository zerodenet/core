use super::super::client::carrier;
use super::*;
use hyper_util::rt::TokioExecutor;
mod idle;
mod selection;
use idle::IdleConnections;
pub(super) use selection::ConnectionClaim;
#[derive(Default)]
pub(super) struct Connections {
    state: tokio::sync::Mutex<Option<Connection>>,
    idle: Mutex<IdleConnections>,
    opening: std::sync::atomic::AtomicBool,
    pub(super) changed: tokio::sync::Notify,
}
pub(super) struct Connection {
    pub(super) sender: carrier::Sender,
    _driver: Driver,
}
pub(in crate::split_http) struct DriverLease {
    _driver: Driver,
    // A logical stream still needs its original H2 dispatcher after its XMUX
    // request group rotates. The original lease alone retains no H2 sender.
    _sender: Option<hyper::client::conn::http2::SendRequest<crate::split_http::body::Body>>,
}
enum Driver {
    Legacy(Arc<LegacyDriver>),
    Owned(super::owned::OwnedXhttp3),
    OwnedTcp(zero_traits::packet_metadata::PacketMetadataArc<owned::TcpOriginal>),
}
impl Clone for Driver {
    fn clone(&self) -> Self {
        match self {
            Self::Legacy(original) => Self::Legacy(original.clone()),
            Self::Owned(original) => Self::Owned(original.clone()),
            Self::OwnedTcp(original) => Self::OwnedTcp(original.clone()),
        }
    }
}
struct LegacyDriver(tokio::task::AbortHandle);
impl Drop for LegacyDriver {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl Connection {
    async fn return_idle_original(self) -> io::Result<()> {
        // A closed cached sender can precede its original native/Scope ACK.
        // Existing logical work may still use newer physical connections;
        // returning that cache reference must not stop or await its own driver.
        let retirement = if self.has_logical_work() {
            None
        } else {
            match &self._driver {
                Driver::Owned(original) => Some(original.retirement()),
                Driver::OwnedTcp(original) => Some(original.retirement()),
                Driver::Legacy(_) => None,
            }
        };
        if let Some(original) = &retirement {
            original.request_retirement();
        }
        drop(self);
        if let Some(original) = retirement {
            original
                .wait_settled_original()
                .await
                .map_err(io::Error::other)?;
        }
        Ok(())
    }
    pub(super) fn retain_driver(&self) -> DriverLease {
        DriverLease {
            _driver: self._driver.clone(),
            _sender: match &self.sender {
                carrier::Sender::Http2(sender) => Some(sender.clone()),
                _ => None,
            },
        }
    }
    pub(super) fn owned_origin(
        &self,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierOrigin> {
        match &self._driver {
            Driver::Owned(original) => Some(original.origin().clone()),
            Driver::OwnedTcp(original) => Some(original.origin()),
            Driver::Legacy(_) => None,
        }
    }
    pub(super) fn owned_runtime(
        &self,
    ) -> Option<zero_traits::packet_metadata::PacketMetadataArc<owned::Runtime>> {
        match &self._driver {
            Driver::Owned(original) => Some(original.runtime()),
            Driver::OwnedTcp(original) => Some(original.runtime()),
            _ => None,
        }
    }
    fn has_logical_work(&self) -> bool {
        self.owned_runtime()
            .is_some_and(|runtime| runtime.has_logical_work())
    }
    fn is_owned(&self) -> bool {
        !matches!(self._driver, Driver::Legacy(_))
    }
    fn idle_memory(
        &self,
    ) -> Option<&zero_platform_tokio::contract::component::PacketDriverMetadataView> {
        match &self._driver {
            Driver::OwnedTcp(original) => Some(original.memory()),
            _ => None,
        }
    }
    pub(super) fn bind_logical(&mut self, original: owned::OwnedLogicalReturn) -> io::Result<()> {
        match &self._driver {
            // H1/H2 session work and Hyper delegates share this TCP Runtime.
            // Its actual session WorkReturn already keeps logical custody.
            Driver::OwnedTcp(_) => Ok(()),
            Driver::Owned(_) => self.sender.bind_logical(original),
            Driver::Legacy(_) => Err(io::ErrorKind::PermissionDenied.into()),
        }
    }
    fn multiplexed_clone(&self) -> Option<Self> {
        let sender = match &self.sender {
            carrier::Sender::Http2(sender) if !sender.is_closed() => {
                carrier::Sender::Http2(sender.clone())
            }
            carrier::Sender::Http3(sender) => carrier::Sender::Http3(sender.clone()),
            carrier::Sender::OwnedHttp3(sender) => carrier::Sender::OwnedHttp3(sender.clone()),
            _ => return None,
        };
        Some(Self {
            sender,
            _driver: self._driver.clone(),
        })
    }
}
impl Connections {
    pub(super) fn take_returned_idle_original(&self, has_responses: bool) -> Option<Connection> {
        if let Some(connection) = self.idle.lock().unwrap().take_returned_original() {
            return Some(connection);
        }
        if has_responses {
            return None;
        }
        let Ok(mut state) = self.state.try_lock() else {
            return None;
        };
        if state
            .as_ref()
            .is_some_and(|connection| connection.is_owned() && !connection.has_logical_work())
        {
            state.take()
        } else {
            None
        }
    }
    pub(super) fn has_logical_work(&self) -> bool {
        let Ok(state) = self.state.try_lock() else {
            return true;
        };
        state.as_ref().is_some_and(Connection::has_logical_work)
            || self.idle.lock().unwrap().has_logical_work()
    }
    /// Actual owned group only; return its original Root handle after taking
    /// the one stored connection. Neither mutex removal nor Drop proves close.
    pub(super) async fn retire_owned(
        &self,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierProtocolRetirement> {
        let connection =
            { self.state.lock().await.take() }.or_else(|| self.idle.lock().unwrap().pop());
        let original = connection
            .as_ref()
            .and_then(|connection| match &connection._driver {
                Driver::Owned(original) => Some(original.retirement()),
                Driver::OwnedTcp(original) => Some(original.retirement()),
                Driver::Legacy(_) => None,
            });
        if let Some(original) = &original {
            original.request_retirement();
        }
        drop(connection);
        original
    }
    pub(super) fn recycle(&self, connection: Connection) {
        let memory = connection.idle_memory().cloned();
        let _notifications = memory.as_ref().map(|memory| memory.notifications());
        let returned = { self.idle.lock().unwrap().push(connection, memory.as_ref()) };
        drop(returned);
    }
    pub(super) async fn acquire_owned(
        &self,
        group: &GroupRef,
        factory: &OwnedXhttpCarrierFactory,
        kind: super::owned_connect::OwnedConnectionKind,
    ) -> io::Result<Connection> {
        // A new request group has no physical connection yet. Carry the real
        // selected version through rotation before polling its same factory.
        self.select_owned(group, kind).await?.finish(factory).await
    }
    pub(super) async fn acquire(
        &self,
        group: &GroupRef,
        factory: &OwnedXhttpCarrierFactory,
    ) -> io::Result<Connection> {
        self.acquire_mode(group, factory, false).await
    }
    async fn acquire_mode(
        &self,
        group: &GroupRef,
        factory: &OwnedXhttpCarrierFactory,
        owned: bool,
    ) -> io::Result<Connection> {
        loop {
            let connection = { self.idle.lock().unwrap().pop() };
            let Some(connection) = connection else {
                break;
            };
            if matches!(&connection.sender, carrier::Sender::Http1(sender) if !sender.is_closed()) {
                if owned && !connection.is_owned() {
                    return Err(io::ErrorKind::PermissionDenied.into());
                }
                return Ok(connection);
            }
        }
        let mut state = self.state.lock().await;
        if let Some(connection) = state.as_ref().and_then(Connection::multiplexed_clone) {
            if owned && !connection.is_owned() {
                return Err(io::ErrorKind::PermissionDenied.into());
            }
            return Ok(connection);
        }
        let carrier = factory.call().await.map_err(io::Error::other)?;
        if let XhttpCarrier::OwnedHttp3(owned) = carrier {
            let connection = Connection {
                sender: carrier::Sender::OwnedHttp3(owned.sender()),
                _driver: Driver::Owned(owned),
            };
            let previous = std::mem::replace(&mut *state, connection.multiplexed_clone());
            drop(state);
            drop(previous);
            return Ok(connection);
        }
        let carrier = match carrier {
            XhttpCarrier::OwnedTcp(original) => {
                let (sender, original) = original.into_parts();
                let connection = Connection {
                    sender,
                    _driver: Driver::OwnedTcp(original),
                };
                let previous = std::mem::replace(&mut *state, connection.multiplexed_clone());
                drop(state);
                drop(previous);
                return Ok(connection);
            }
            other => other,
        };
        if owned {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "installed owned XMUX cannot start a compatibility HTTP driver",
            ));
        }
        let weak = group.library_weak()?;
        let (sender, task) = match carrier {
            XhttpCarrier::Http1(stream) => {
                let prepared::PreparedConnection { sender, driver } =
                    prepared::http1(stream).await?;
                (
                    sender,
                    tokio::spawn(async move {
                        let _ = driver.await;
                    })
                    .abort_handle(),
                )
            }
            XhttpCarrier::Http2(stream) => {
                let settings = stream
                    .application_settings()
                    .filter(|settings| settings.protocol == b"h2")
                    .map(|settings| settings.peer.clone());
                // The compatibility path explicitly keeps its existing executor.
                // Managed owners must supply and supervise their own executor.
                let prepared::PreparedConnection { sender, driver } = prepared::http2(
                    stream,
                    settings.as_deref(),
                    group.keepalive,
                    TokioExecutor::new(),
                )
                .await?;
                (
                    sender,
                    tokio::spawn(async move {
                        let _ = driver.await;
                        if let Some(group) = weak.upgrade() {
                            group.fail();
                        }
                    })
                    .abort_handle(),
                )
            }
            XhttpCarrier::OwnedHttp3(_) | XhttpCarrier::OwnedTcp(_) => {
                unreachable!("actual owned branch returned above")
            }
            XhttpCarrier::Http3(connection) => {
                let (sender, task) =
                    super::super::http3::client::open_shared(connection, move || {
                        if let Some(group) = weak.upgrade() {
                            group.fail();
                        }
                    })
                    .await?;
                (carrier::Sender::Http3(sender), task)
            }
        };
        let connection = Connection {
            sender,
            _driver: Driver::Legacy(Arc::new(LegacyDriver(task))),
        };
        if let Some(shared) = connection.multiplexed_clone() {
            let previous = state.replace(shared);
            drop(state);
            drop(previous);
        }
        Ok(connection)
    }
}
