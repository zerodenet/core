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
    _driver: Arc<Driver>,
}
struct Driver(tokio::task::AbortHandle);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl Connection {
    fn multiplexed_clone(&self) -> Option<Self> {
        let sender = match &self.sender {
            carrier::Sender::Http2(sender) => carrier::Sender::Http2(sender.clone()),
            carrier::Sender::Http3(sender) => carrier::Sender::Http3(sender.clone()),
            _ => return None,
        };
        Some(Self {
            sender,
            _driver: self._driver.clone(),
        })
    }
}
impl Connections {
    pub(super) fn recycle(&self, connection: Connection) {
        let mut idle = self.idle.lock().unwrap();
        if idle.len() < 128 {
            idle.push(connection);
        }
    }
    pub(super) async fn acquire(&self, group: &Arc<Group>) -> io::Result<Connection> {
        while let Some(connection) = self.idle.lock().unwrap().pop() {
            if matches!(&connection.sender, carrier::Sender::Http1(sender) if !sender.is_closed()) {
                return Ok(connection);
            }
        }
        let mut state = self.state.lock().await;
        if let Some(connection) = state.as_ref().and_then(Connection::multiplexed_clone) {
            return Ok(connection);
        }
        let carrier = (group.factory)().await.map_err(io::Error::other)?;
        let weak = Arc::downgrade(group);
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
            _driver: Arc::new(Driver(task)),
        };
        if let Some(shared) = connection.multiplexed_clone() {
            *state = Some(shared);
        }
        Ok(connection)
    }
}
