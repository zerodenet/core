//! Only the existing wire polls actual Path IO; no new receive pump.
use super::*;
#[async_trait]
impl RawIpWireCarrier for RegisteredProxiedRawIpWireCarrier {
    fn native_owner(
        &self,
    ) -> Option<Arc<crate::runtime::udp_dispatch::packet_owner::PacketCarrierOwnerSlot>> {
        self.state.path.borrow().native_owner()
    }
    fn background_drained(&self) -> bool {
        self.state.background_drained()
    }
    fn stop_background(&self) -> Option<CarrierWorkerCompletion> {
        self.state.stop();
        let state = self.state.clone();
        Some(Box::pin(async move { state.wait_background().await }))
    }
    async fn close_native(&self) -> Result<(), EngineError> {
        // Exact preserved TEMP physical ancestor/reconnect history remains
        // Unknown. Software joins/loan closure never certify that history.
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "outer carrier reconnect originals are not qualified for confirmed replacement",
        )
        .into())
    }
    async fn recv(&self, buffer: &mut [u8]) -> std::io::Result<(usize, Option<SocketAddr>)> {
        let mut paths = self.state.path.subscribe();
        loop {
            if *self.state.closing.borrow() {
                return Err(background_stopped());
            }
            let path = paths.borrow_and_update().clone();
            match path.recv_from_with_source(buffer).await {
                Ok(received) => return Ok(received),
                Err(source) => {
                    tracing::warn!(?source, "raw-IP outer packet path receive failed");
                    self.state
                        .rebuild_if_current(path.clone())
                        .map_err(std::io::Error::other)?;
                    while Arc::ptr_eq(&path, &paths.borrow()) {
                        let actor = self.state.actor();
                        let mut closing = self.state.closing.subscribe();
                        tokio::select! {
                            changed=paths.changed()=>changed.map_err(|source|std::io::Error::new(std::io::ErrorKind::BrokenPipe,source))?,
                            _=closing.changed()=>return Err(background_stopped()),
                            result=async {match actor {Actor::Registered(w)=>w.wait().await.result(),_=>std::future::pending().await}}=>{
                                if let Err(source)=result{return Err(std::io::Error::other(ChannelError::retained(source)));}
                                if Arc::ptr_eq(&path,&paths.borrow()){return Err(background_stopped());}
                            }
                        }
                    }
                }
            }
        }
    }
    async fn send(&self, packet: &[u8], endpoint: SocketAddr) -> std::io::Result<()> {
        if *self.state.closing.borrow() {
            return Err(background_stopped());
        }
        let target = match endpoint.ip() {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        };
        let path = self.state.path.borrow().clone();
        match path.send_to(&target, endpoint.port(), packet).await {
            Ok(()) => {
                self.sent_once.store(true, Ordering::Release);
                Ok(())
            }
            Err(source) => {
                tracing::warn!(?source, "raw-IP outer packet path send failed");
                self.state
                    .rebuild_if_current(path)
                    .map_err(std::io::Error::other)?;
                if self.sent_once.load(Ordering::Acquire) {
                    Ok(())
                } else {
                    Err(std::io::Error::other(source))
                }
            }
        }
    }
}
