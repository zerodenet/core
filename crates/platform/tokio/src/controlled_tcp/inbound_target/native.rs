//! Persist the actual raw original before bind/configuration and callbacks.
use super::*;
#[cfg(unix)]
use std::os::fd::{FromRawFd, IntoRawFd};
#[cfg(windows)]
use std::os::windows::io::{FromRawSocket, IntoRawSocket};
fn into_original(socket: TcpSocket) -> std::net::TcpStream {
    #[cfg(unix)]
    {
        unsafe { std::net::TcpStream::from_raw_fd(socket.into_raw_fd()) }
    }
    #[cfg(windows)]
    {
        unsafe { std::net::TcpStream::from_raw_socket(socket.into_raw_socket()) }
    }
}
fn error(stage: &'static str, error: io::Error) -> Arc<TcpConnectError> {
    Arc::new(TcpConnectError {
        stage,
        interface_bound: false,
        local_addr: None,
        error,
    })
}
impl InboundTargetNativeOwner {
    pub async fn prepare_tcp(
        &self,
        peer: SocketAddr,
        policy: &DialPolicy,
        selection: &EgressSelection,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Result<(PendingTcpConnect, usize), Arc<TcpConnectError>> {
        self.0
            .check()
            .map_err(|source| error("validate_dial_policy", io::Error::other(source)))?;
        let declared = match &self.0.endpoint {
            FallbackEndpoint::Tcp { server, port } => {
                *port == peer.port()
                    && zero_core::address::parse_ip_literal(server)
                        .is_none_or(|ip| ip.to_canonical() == peer.ip().to_canonical())
            }
            FallbackEndpoint::Unix { .. } => false,
        };
        if !declared {
            return Err(error(
                "validate_target_endpoint",
                io::Error::other(failure(Stage::Prepare, Reason::StalePlan)),
            ));
        }

        let dial = crate::dial::prepare(peer, policy, selection)
            .map_err(|source| error("validate_dial_policy", source))?;
        let mut state = self.0.state.lock().await;
        self.0
            .check()
            .map_err(|source| error("validate_dial_policy", io::Error::other(source)))?;
        let index = state.next;
        let next = index.checked_add(1).ok_or_else(|| {
            error(
                "target_identity",
                io::Error::other("target candidate identities exhausted"),
            )
        })?;
        let permit = match state.idle_permits.pop() {
            Some(permit) => permit,
            None => self
                .0
                .pool
                .permits
                .clone()
                .try_acquire_owned()
                .map_err(|source| error("target_capacity", io::Error::other(source)))?,
        };
        state.next = next;
        // Allocate the finite graph row before acquiring its actual socket.
        state.slots.insert(
            index,
            Slot {
                peer: Some(dial.peer),
                raw: None,
                control: None,
                permit: Some(permit),
            },
        );
        let socket = if dial.peer.is_ipv4() {
            TcpSocket::new_v4()
        } else {
            TcpSocket::new_v6()
        };
        match socket {
            Ok(socket) => {
                self.0.acquired.store(true, Ordering::Release);
                state.slots.get_mut(&index).unwrap().raw = Some(socket);
            }
            Err(source) => {
                let mut unused = state.slots.remove(&index).unwrap();
                state.idle_permits.extend(unused.permit.take());
                return Err(error("create_socket", source));
            }
        }
        let socket = state.slots[&index].raw.as_ref().unwrap();
        let configured = (|| {
            if let Some(source) = dial.source {
                socket
                    .bind(source)
                    .map_err(|source| error("bind_source", source))?;
            }
            if let Some(interface) = &dial.interface {
                crate::egress::bind_tcp_to_interface(socket, dial.peer, interface)
                    .map_err(|source| error("bind_interface", source))?;
            }
            Ok::<_, Arc<TcpConnectError>>(())
        })();
        // No observer/await exists in this exclusive raw->controller move. The
        // same unique descriptor is stored before any user callback can run.
        let original = into_original(state.slots.get_mut(&index).unwrap().raw.take().unwrap());
        let (pending, control) = PendingTcpConnect::from_socket(
            original,
            dial.peer,
            dial.interface,
            self.0.stop.clone(),
            None,
        );
        let shared = control.shared.clone();
        state.slots.get_mut(&index).unwrap().control = Some(control);
        drop(state);
        if let Err(source) = configured {
            drop(pending);
            let cleanup = self
                .close_candidate(index, Instant::now() + std::time::Duration::from_secs(5))
                .await;
            let cleanup = match cleanup {
                Ok(true) => None,
                Ok(false) => Some(failure(Stage::Close, Reason::Timeout)),
                Err(source) => Some(source),
            };
            if let Some(cleanup) = cleanup {
                return Err(error(
                    "close_failed_target",
                    io::Error::other(PreparationCleanupFailure {
                        preparation: source,
                        cleanup: Arc::new(cleanup),
                    }),
                ));
            }
            return Err(source);
        }
        *shared
            .observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = observer.clone();
        if let Some(observer) = observer {
            observer.stream_boundary();
        }
        Ok((pending, index))
    }
    #[cfg(unix)]
    pub async fn prepare_unix(&self, path: &str) -> io::Result<(PendingUnixConnect, usize)> {
        self.0.check().map_err(io::Error::other)?;
        if !matches!(&self.0.endpoint, FallbackEndpoint::Unix { path: declared } if declared == path)
        {
            return Err(io::Error::other(failure(Stage::Prepare, Reason::StalePlan)));
        }

        let mut state = self.0.state.lock().await;
        self.0.check().map_err(io::Error::other)?;
        let index = state.next;
        let next = index
            .checked_add(1)
            .ok_or_else(|| io::Error::other("target candidate identities exhausted"))?;
        let permit = match state.idle_permits.pop() {
            Some(permit) => permit,
            None => self
                .0
                .pool
                .permits
                .clone()
                .try_acquire_owned()
                .map_err(io::Error::other)?,
        };
        state.next = next;
        let (pending, control) = match PendingUnixConnect::prepare(path, self.0.stop.clone()) {
            Ok(original) => original,
            Err(source) => {
                state.idle_permits.push(permit);
                return Err(source);
            }
        };
        self.0.acquired.store(true, Ordering::Release);
        state.slots.insert(
            index,
            Slot {
                peer: None,
                raw: None,
                control: Some(control),
                permit: Some(permit),
            },
        );
        Ok((pending, index))
    }
}
impl Slot {
    pub(super) fn try_close(
        &mut self,
        stop: &CancellationToken,
    ) -> Result<CloseReport, ChannelError> {
        if let Some(raw) = self.raw.take() {
            let original = into_original(raw);
            let (pending, control) = PendingTcpConnect::from_socket(
                original,
                self.peer.expect("same actual raw TCP peer"),
                None,
                stop.clone(),
                None,
            );
            drop(pending);
            self.control = Some(control);
        }
        let report = self
            .control
            .as_mut()
            .ok_or_else(|| failure(Stage::Close, Reason::Internal))?
            .try_close_once()?;
        Ok(report)
    }
}

#[derive(Debug)]
struct PreparationCleanupFailure {
    preparation: Arc<TcpConnectError>,
    cleanup: Arc<ChannelError>,
}
impl std::fmt::Display for PreparationCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "original target preparation and cleanup both failed: {}; {}",
            self.preparation, self.cleanup
        )
    }
}
impl std::error::Error for PreparationCleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cleanup.as_ref())
    }
}
