//! Claim one actual existing group before a physical factory is polled.
//! The flag serializes its own multiplexed creation, not IO/Scope authority.
use super::super::owned_connect::OwnedConnectionKind;
use super::*;
use std::sync::atomic::Ordering;
pub(in crate::split_http::xmux) struct ConnectionClaim {
    group: GroupRef,
    cached: Option<Connection>,
    opening: bool,
    kind: OwnedConnectionKind,
}
impl Connections {
    pub(in crate::split_http::xmux) async fn select_owned(
        &self,
        group: &GroupRef,
        kind: OwnedConnectionKind,
    ) -> io::Result<ConnectionClaim> {
        group.bind_connection_kind(kind)?;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            // These two original queues are inspected independently. No real
            // observer/controller predicate or final original Drop runs locked.
            if kind == OwnedConnectionKind::Http1 {
                loop {
                    let cached = { self.idle.lock().unwrap().pop() };
                    let Some(connection) = cached else { break };
                    if connection.is_owned()
                        && matches!(&connection.sender, carrier::Sender::Http1(sender) if !sender.is_closed())
                    {
                        return Ok(ConnectionClaim {
                            group: group.clone(),
                            cached: Some(connection),
                            opening: false,
                            kind,
                        });
                    }
                    drop(connection);
                }
                return Ok(ConnectionClaim {
                    group: group.clone(),
                    cached: None,
                    opening: false,
                    kind,
                });
            }
            let state = self.state.lock().await;
            if let Some(connection) = state.as_ref().and_then(Connection::multiplexed_clone) {
                if !connection.is_owned() {
                    return Err(io::ErrorKind::PermissionDenied.into());
                }
                return Ok(ConnectionClaim {
                    group: group.clone(),
                    cached: Some(connection),
                    opening: false,
                    kind,
                });
            }
            if !self.opening.swap(true, Ordering::AcqRel) {
                drop(state);
                return Ok(ConnectionClaim {
                    group: group.clone(),
                    cached: None,
                    opening: true,
                    kind,
                });
            }
            drop(state);
            changed.await;
        }
    }
}
impl ConnectionClaim {
    pub(in crate::split_http::xmux) fn needs_original(&self) -> bool {
        self.cached.is_none()
    }
    pub(in crate::split_http::xmux) fn same_group(&self, other: &GroupRef) -> bool {
        std::ptr::eq(&*self.group, &**other)
    }
    pub(in crate::split_http::xmux) async fn finish(
        mut self,
        factory: &OwnedXhttpCarrierFactory,
    ) -> io::Result<Connection> {
        if let Some(cached) = self.cached.take() {
            return Ok(cached);
        }
        // A later idle H1 return cannot substitute for this already selected
        // new original, leaving a preinstalled Root controller never started.
        let connection = match factory.call().await.map_err(io::Error::other)? {
            XhttpCarrier::OwnedHttp3(original) => Connection {
                sender: carrier::Sender::OwnedHttp3(original.sender()),
                _driver: Driver::Owned(original),
            },
            XhttpCarrier::OwnedTcp(original) => {
                let (sender, original) = original.into_parts();
                Connection {
                    sender,
                    _driver: Driver::OwnedTcp(original),
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "installed XMUX requires its original owned HTTP driver",
                ))
            }
        };
        let http1 = matches!(&connection.sender, carrier::Sender::Http1(_));
        if http1 != (self.kind == OwnedConnectionKind::Http1) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "actual owned HTTP version differs from its selected group",
            ));
        }
        let shared = connection.multiplexed_clone();
        if let Some(shared) = shared {
            let previous = { self.group.connections.state.lock().await.replace(shared) };
            drop(previous);
        }
        Ok(connection)
    }
}
impl Drop for ConnectionClaim {
    fn drop(&mut self) {
        // Neither cache removal nor this metadata claim is a closure receipt.
        // Actual body/driver ownership remains in Connection and its Root row.
        if self.opening {
            self.group
                .connections
                .opening
                .store(false, Ordering::Release);
            self.group.connections.changed.notify_waiters();
        }
    }
}
