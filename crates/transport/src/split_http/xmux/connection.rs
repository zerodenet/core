use super::super::client::carrier;
use super::*;
use hyper_util::rt::TokioExecutor;
#[derive(Default)]
pub(super) struct Connections {
    state: tokio::sync::Mutex<Option<Connection>>,
    idle: Mutex<Vec<Connection>>,
}
pub(super) struct Connection {
    pub(super) sender: carrier::Sender,
    _driver: Driver,
}
enum Driver {
    Legacy(Arc<LegacyDriver>),
    Owned(super::owned::OwnedXhttp3),
}
impl Clone for Driver {
    fn clone(&self) -> Self {
        match self {
            Self::Legacy(original) => Self::Legacy(original.clone()),
            Self::Owned(original) => Self::Owned(original.clone()),
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
    pub(super) fn owned_origin(
        &self,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierOrigin> {
        match &self._driver {
            Driver::Owned(original) => Some(original.origin().clone()),
            Driver::Legacy(_) => None,
        }
    }
    pub(super) fn owned_runtime(
        &self,
    ) -> Option<zero_traits::packet_metadata::PacketMetadataArc<owned::Runtime>> {
        match &self._driver {
            Driver::Owned(original) => Some(original.runtime()),
            _ => None,
        }
    }
    fn multiplexed_clone(&self) -> Option<Self> {
        let sender = match &self.sender {
            carrier::Sender::Http2(sender) => carrier::Sender::Http2(sender.clone()),
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
    /// Actual owned group only; return its original Root handle after taking
    /// the one stored connection. Neither mutex removal nor Drop proves close.
    pub(super) async fn retire_owned(
        &self,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierProtocolRetirement> {
        let connection = { self.state.lock().await.take() };
        let original = connection
            .as_ref()
            .and_then(|connection| match &connection._driver {
                Driver::Owned(original) => Some(original.retirement()),
                Driver::Legacy(_) => None,
            });
        if let Some(original) = &original {
            original.request_retirement();
        }
        drop(connection);
        original
    }
    pub(super) fn recycle(&self, connection: Connection) {
        let retired = {
            let mut idle = self.idle.lock().unwrap();
            if idle.len() < 128 {
                idle.push(connection);
                None
            } else {
                Some(connection)
            }
        };
        drop(retired);
    }
    pub(super) async fn acquire_owned(
        &self,
        group: &GroupRef,
        factory: &XhttpCarrierFactory,
    ) -> io::Result<Connection> {
        self.acquire_mode(group, factory, true).await
    }
    pub(super) async fn acquire(
        &self,
        group: &GroupRef,
        factory: &XhttpCarrierFactory,
    ) -> io::Result<Connection> {
        self.acquire_mode(group, factory, false).await
    }
    async fn acquire_mode(
        &self,
        group: &GroupRef,
        factory: &XhttpCarrierFactory,
        owned: bool,
    ) -> io::Result<Connection> {
        loop {
            let connection = { self.idle.lock().unwrap().pop() };
            let Some(connection) = connection else {
                break;
            };
            if matches!(&connection.sender, carrier::Sender::Http1(sender) if !sender.is_closed()) {
                if owned {
                    return Err(io::ErrorKind::PermissionDenied.into());
                }
                return Ok(connection);
            }
        }
        let mut state = self.state.lock().await;
        if let Some(connection) = state.as_ref().and_then(Connection::multiplexed_clone) {
            if owned && !matches!(&connection._driver, Driver::Owned(_)) {
                return Err(io::ErrorKind::PermissionDenied.into());
            }
            return Ok(connection);
        }
        let carrier = factory().await.map_err(io::Error::other)?;
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
            XhttpCarrier::OwnedHttp3(_) => unreachable!("actual owned branch returned above"),
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
