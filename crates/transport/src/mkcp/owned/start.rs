//! Mask, wire and reliable driver registration share the granting task book.
use super::*;
use zero_platform_tokio::contract::scope::TaskClass;
impl OwnedMkcp {
    pub fn start(
        &mut self,
        plan: crate::finalmask::owned::PreparedMasks,
        icmp: Option<zero_platform_tokio::controlled_icmp::ControlledIcmpSocket>,
        peer: SocketAddr,
    ) -> Result<TcpRelayStream, ChannelError> {
        if self.attempted || self.stop.is_cancelled() {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            ));
        }
        self.attempted = true;
        let memory =
            budget::Memory::prepare(self.settings, self.buffers.clone()).map_err(|source| {
                ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component)
                    .with_source(source)
            })?;
        let socket = self
            .socket
            .take()
            .ok_or_else(|| failure(io::Error::other("mKCP socket consumed")))?;
        let prepared = plan.prepare(socket.socket.clone(), icmp).map_err(failure)?;
        for driver in prepared.drivers {
            let stop = driver.work.cancel.clone();
            let ordinal = driver.ordinal;
            let origin = OwnedDriverOrigin::Mask {
                kind: driver.kind,
                ordinal,
            };
            let id = self
                .graph
                .drivers()
                .spawn(origin, TaskClass::Critical, move |slot| async move {
                    let body = slot.record(body(origin, driver.work.future.await));
                    outcome(&body)
                })
                .map_err(|source| {
                    ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component)
                        .with_source(source)
                })?;
            self.masks.push((id, ordinal, stop));
        }
        let wire = crate::finalmask::packet_socket::from_socket(prepared.socket);
        let (io, driver) = super::super::stream::connection_owned(
            wire.clone(),
            peer,
            rand::random(),
            self.settings,
            super::super::stream::Input::Socket(wire, peer),
            memory.clone(),
        )
        .map_err(failure)?;
        let user = self.graph.acquire()?;
        let stopped = self.stop.clone();
        let id = self
            .graph
            .drivers()
            .spawn(
                OwnedDriverOrigin::Mkcp,
                TaskClass::Auxiliary,
                move |slot| async move {
                    let result = tokio::select! { biased;
                        _=stopped.cancelled()=>Ok(()),
                        result=driver=>result,
                    };
                    // Dropping the actual protocol future has returned all its I/O;
                    // the original receiver still has to consume this recorded body.
                    let body = slot.record(body(OwnedDriverOrigin::Mkcp, result));
                    outcome(&body)
                },
            )
            .map_err(|source| {
                ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component)
                    .with_source(source)
            })?;
        self.driver = Some(id);
        Ok(TcpRelayStream::new(stream::View {
            io,
            _user: user,
            _memory: memory,
        }))
    }
}
fn body(origin: OwnedDriverOrigin, result: io::Result<()>) -> DriverBody {
    DriverBody {
        origin,
        io_error: result.err().map(Arc::new),
        close_reason: None,
        #[cfg(any(feature = "split_http", feature = "hysteria", feature = "quic_h3"))]
        http3: None,
    }
}
fn outcome(body: &DriverBody) -> Result<(), ChannelError> {
    match &body.io_error {
        Some(source) => Err(
            ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                .with_source(OriginalIo(source.clone())),
        ),
        None => Ok(()),
    }
}
#[derive(Debug)]
struct OriginalIo(Arc<io::Error>);
impl std::fmt::Display for OriginalIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for OriginalIo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
