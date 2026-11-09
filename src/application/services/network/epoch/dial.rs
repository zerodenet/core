use super::*;
use crate::application::services::network::stream::EventStream;
use zero_core::{component::NetworkGeneration, request::Destination, Address};
use zero_platform_tokio::contract::network::{CarrierContext, UpstreamBinding, UpstreamOpener};
use zero_platform_tokio::{controlled_carrier::NativeCarrierOpener, EgressInterfaceControl};

/// A cancelled waiting connect stops only its exact original. Cleanup remains
/// in the same Epoch/CarrierOwner and is driven after the actual helper joins.
struct Waiting {
    original: Arc<Original>,
    delivered: bool,
}
impl Drop for Waiting {
    fn drop(&mut self) {
        if !self.delivered {
            self.original.adoption.request_retirement();
        }
    }
}
impl Epoch {
    pub(in super::super) async fn connect(
        self: &Arc<Self>,
        peer: SocketAddr,
        egress: &EgressInterfaceControl,
        deadline: Instant,
    ) -> io::Result<EventStream> {
        if self.closing.load(Ordering::Acquire)
            || self.failed.load(Ordering::Acquire)
            || !self.faults.lock().unwrap().is_empty()
        {
            return Err(io::Error::other("event generation closing"));
        }
        self.reap().await?;
        let grants = {
            let mut native = self.native.lock().await;
            native.start(deadline).await.map_err(io::Error::other)?;
            native.grants()
        };
        let (original, witness, rx, start) = {
            let callbacks = self.registrar.defer_callbacks();
            let prepared = {
                let _admission = self.admission.lock().unwrap();
                if self.closing.load(Ordering::Acquire)
                    || self.failed.load(Ordering::Acquire)
                    || !self.faults.lock().unwrap().is_empty()
                {
                    return Err(io::Error::other("event generation closing"));
                }
                if self.originals.lock().unwrap().len() >= self.limits.max_originals().get() {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "event native connection capacity exhausted",
                    ));
                }
                let selection = egress.select_for_peer_with_policy(peer, &DialPolicy::default())?;
                selection.ensure_connectable()?;
                if selection.generation() != self.generation
                    || egress.generation() != self.generation
                {
                    return Err(io::Error::other("event dial network generation retired"));
                }
                let mut grant = grants.reserve().map_err(io::Error::other)?;
                let user = grant.claim_user_view().map_err(io::Error::other)?;
                let original = Arc::new(Original {
                    adoption: grant.adoption(),
                    close_fault: Mutex::new(None),
                    operation_fault: Mutex::new(None),
                    closing: AsyncMutex::new(()),
                    custody: Arc::new(Custody::default()),
                    receipt: Mutex::new(None),
                });
                // Both the same Host total and the Epoch own the actual reservation
                // before prepare/connect can acquire its original native socket.
                self.originals.lock().unwrap().push(original.clone());
                let custody = original.custody.clone();
                let attempt = original.clone();
                let bound = self.binding.clone();
                let host_stop = self.stop.clone();
                let egress = Arc::new(egress.clone());
                let binding = UpstreamBinding {
                    prefix: Vec::new(),
                    dial: DialPolicy::default(),
                    network_generation: NetworkGeneration(self.generation),
                };
                let endpoint = Destination::new(
                    match peer.ip() {
                        std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
                        std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
                    },
                    peer.port(),
                );
                let (start, started) = oneshot::channel();
                let (tx, rx) = oneshot::channel();
                let spawned = self.spawn_original(Box::pin(async move {
                    if started.await.is_err() {
                        return;
                    }
                    let result: io::Result<EventStream> = async {
                        let caller = ACTUAL_TASK
                            .try_with(Clone::clone)
                            .map_err(io::Error::other)?;
                        let context = CarrierContext {
                            session: None,
                            deadline: Some(deadline),
                            cancel: grant.cancellation(),
                        };
                        let ticket = bound
                            .direct_service_issuer(&host_stop, &grant, caller, &endpoint, &binding)
                            .and_then(|issuer| {
                                issuer.authorize(context.cancel.clone(), context.deadline)
                            })
                            .map_err(io::Error::other)?;
                        let native = NativeCarrierOpener::service(ticket, egress, None)
                            .map_err(io::Error::other)?;
                        let connected = native
                            .prepare_controlled_stream(&binding, endpoint, context, grant)
                            .map_err(io::Error::other)?
                            .open(deadline)
                            .await
                            .map_err(io::Error::other)?;
                        let (io, _) = connected.into_parts();
                        Ok(EventStream::new(io, user, custody))
                    }
                    .await;
                    let result = result.map_err(|source| {
                        let source = Arc::new(source);
                        *attempt.operation_fault.lock().unwrap() = Some(source.clone());
                        io::Error::new(source.kind(), source)
                    });
                    let _ = tx.send(result);
                }));
                let witness = match spawned {
                    Ok(witness) => {
                        original.custody.add(witness.clone());
                        witness
                    }
                    Err(error) => {
                        original.custody.seal();
                        return Err(error);
                    }
                };
                original.custody.seal();
                (original, witness, rx, start)
            };
            drop(callbacks);
            prepared
        };
        let mut waiting = Waiting {
            original: original.clone(),
            delivered: false,
        };
        start
            .send(())
            .map_err(|_| io::Error::other("original event preparation stopped"))?;
        let result = rx
            .await
            .map_err(|_| io::Error::other("original event connect task stopped"))?;
        let joined = tokio::time::timeout_at(deadline, witness.wait())
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "event connect exit not joined")
            })?;
        if let Err(source) = joined.result() {
            return Err(io::Error::other(source));
        }
        match result {
            Ok(stream) => {
                if egress.generation() != self.generation {
                    drop(stream);
                    self.close_returned(deadline).await?;
                    return Err(io::Error::other(
                        "event dial completed on a retired network generation",
                    ));
                }
                waiting.delivered = true;
                Ok(stream)
            }
            Err(error) => {
                self.close_returned(deadline).await?;
                if original.receipt.lock().unwrap().is_none() {
                    return Err(io::Error::other(
                        "failed event candidate original not closed",
                    ));
                }
                Err(error)
            }
        }
    }
}
