//! The wire's exact destination and controller publish as one retained value.
use super::*;
#[async_trait]
impl RawIpWireCarrier for RegisteredProxiedRawIpWireCarrier {
    fn stop_background(&self) -> Option<CarrierWorkerCompletion> {
        self.state.stop();
        let state = self.state.clone();
        Some(Box::pin(async move { state.wait_background().await }))
    }
    async fn close_native(&self) -> Result<(), EngineError> {
        self.state.close_originals().await
    }
    async fn recv(&self, buffer: &mut [u8]) -> std::io::Result<(usize, Option<SocketAddr>)> {
        let mut paths = self.state.path.subscribe();
        loop {
            if self.state.stopped() {
                return Err(background_stopped());
            }
            let path = paths.borrow_and_update().clone().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "outer carrier has not initialized",
                )
            })?;
            let mut closing = self.state.closing.subscribe();
            let received = tokio::select! {
                received = path.carrier.recv_from_with_source(buffer) => received,
                _ = closing.changed() => return Err(background_stopped()),
                _ = self.state.execution.cancelled() => return Err(background_stopped()),
            };
            match received {
                Ok(received) => return Ok(received),
                Err(source) => {
                    tracing::warn!(?source, "raw-IP outer packet path receive failed");
                    self.state
                        .rebuild_if_current(path.clone())
                        .map_err(std::io::Error::other)?;
                    self.state.wait_path_change(&mut paths, &path).await?;
                }
            }
        }
    }
    async fn send(&self, packet: &[u8], endpoint: SocketAddr) -> std::io::Result<()> {
        let mut paths = self.state.path.subscribe();
        let path = loop {
            if self.state.stopped() {
                return Err(background_stopped());
            }
            let path = paths.borrow_and_update().clone().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "outer carrier has not initialized",
                )
            })?;
            if path.endpoint == endpoint {
                break path;
            }
            // Endpoint roaming changes only this peer's actual invocation.
            // Keep the old path until its originals settle, then publish the
            // new fixed-target controller; never send through a mismatched one.
            *self
                .state
                .desired
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = endpoint;
            self.state
                .rebuild_if_current(path.clone())
                .map_err(std::io::Error::other)?;
            self.state.wait_path_change(&mut paths, &path).await?;
        };
        let target = match endpoint.ip() {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        };
        let mut closing = self.state.closing.subscribe();
        let sent = tokio::select! {
            sent = path.carrier.send_to(&target, endpoint.port(), packet) => sent,
            _ = closing.changed() => return Err(background_stopped()),
            _ = self.state.execution.cancelled() => return Err(background_stopped()),
        };
        match sent {
            Ok(()) => {
                self.sent_once.store(true, Ordering::Release);
                Ok(())
            }
            Err(source) => {
                tracing::warn!(?source, "raw-IP outer packet path send failed");
                self.state
                    .rebuild_if_current(path)
                    .map_err(std::io::Error::other)?;
                // Preserve the existing wire retransmission/backoff behavior.
                if self.sent_once.load(Ordering::Acquire) {
                    Ok(())
                } else {
                    Err(std::io::Error::other(source))
                }
            }
        }
    }
}
impl State {
    pub(super) fn stopped(&self) -> bool {
        *self.closing.borrow() || self.execution.is_cancelled()
    }
    async fn wait_path_change(
        self: &Arc<Self>,
        paths: &mut tokio::sync::watch::Receiver<Option<Arc<WirePath>>>,
        previous: &Arc<WirePath>,
    ) -> std::io::Result<()> {
        while paths
            .borrow()
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(previous, current))
        {
            if self.stopped() {
                return Err(background_stopped());
            }
            let actor = self.actor();
            let mut closing = self.closing.subscribe();
            tokio::select! {
                changed = paths.changed() => changed.map_err(|source|std::io::Error::new(std::io::ErrorKind::BrokenPipe,source))?,
                _ = closing.changed() => return Err(background_stopped()),
                _ = self.execution.cancelled() => return Err(background_stopped()),
                joined = async { match actor { Actor::Registered(w) => { let joined = w.wait().await; (w, joined) }, _ => std::future::pending().await } } => {
                    let (witness, joined) = joined;
                    if let Err(source) = joined.result() {
                        self.execution.receive_reconnect_failures().map_err(std::io::Error::other)?;
                        return Err(std::io::Error::other(ChannelError::retained(source)));
                    }
                    if paths.borrow().as_ref().is_some_and(|current| Arc::ptr_eq(previous, current)) {
                        // A new endpoint can arrive just after the preceding
                        // actor published its path. Wait for that real ACK
                        // before registering the next bounded reconnect.
                        tokio::select! {
                            _ = witness.wait_registration_released() => {},
                            _ = closing.changed() => return Err(background_stopped()),
                            _ = self.execution.cancelled() => return Err(background_stopped()),
                        }
                        if self.stopped() { return Err(background_stopped()); }
                        self.rebuild_if_current(previous.clone()).map_err(std::io::Error::other)?;
                    }
                }
            }
        }
        Ok(())
    }
}
