//! Real TCP/H2 carriers with a live, actual underlay binding. The independent
//! control is the carrier's source, not a fabricated DNS config generation.
use super::*;
use crate::{DnsBindingRevokedFuture, DnsCarrierBinding, DnsOutboundConnector};
use std::io;

#[derive(Debug)]
struct PhysicalBinding {
    control: EgressInterfaceControl,
    generation: u64,
}
impl DnsCarrierBinding for PhysicalBinding {
    fn check_current(&self) -> io::Result<()> {
        if self.control.generation() == self.generation {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "actual test carrier underlay changed",
            ))
        }
    }
    fn revoked(&self) -> DnsBindingRevokedFuture<'_> {
        Box::pin(async move {
            let mut changes = self.control.subscribe_generation();
            loop {
                if let Err(source) = self.check_current() {
                    return source;
                }
                if let Err(source) = changes.changed().await {
                    return io::Error::new(io::ErrorKind::NotConnected, source);
                }
            }
        })
    }
}
#[derive(Debug)]
struct BoundDetour {
    control: EgressInterfaceControl,
    opens: Arc<AtomicUsize>,
}
impl DnsOutboundConnector for BoundDetour {
    fn connect(
        &self,
        outbound: String,
        endpoint: std::net::SocketAddr,
    ) -> crate::DnsOutboundConnectFuture {
        let carrier = self.connect_carrier_with_context(outbound, endpoint, Default::default());
        Box::pin(async move { carrier.await.map(|carrier| carrier.stream) })
    }
    fn connect_carrier_with_context(
        &self,
        _outbound: String,
        endpoint: std::net::SocketAddr,
        _context: crate::DnsConnectContext,
    ) -> crate::DnsOutboundCarrierFuture {
        let control = self.control.clone();
        let opens = self.opens.clone();
        Box::pin(async move {
            let selection = control.select_for_peer(endpoint);
            selection.ensure_connectable()?;
            let binding = Arc::new(PhysicalBinding {
                control: control.clone(),
                generation: selection.generation(),
            });
            let stream =
                zero_platform_tokio::TokioSocket::connect_addr_on(endpoint, selection.interface())
                    .await?;
            binding.check_current()?;
            opens.fetch_add(1, Ordering::AcqRel);
            Ok(crate::DnsOutboundCarrier {
                stream: stream.into(),
                binding: Some(binding),
            })
        })
    }
}

#[tokio::test]
async fn stored_binding_reuses_a_connection_without_rescheduling_its_carrier() {
    let server = Server::start(FirstConnection::Healthy).await;
    let resolver = server.resolver(EgressInterfaceControl::default());
    let detour = BoundDetour {
        control: EgressInterfaceControl::default(),
        opens: Arc::new(AtomicUsize::new(0)),
    };
    let query = crate::message::build_query("carrier.reuse.test", crate::message::TYPE_A).unwrap();
    for _ in 0..3 {
        resolver
            .exchange(&query, Some("actual-bound-carrier"), Some(&detour))
            .await
            .unwrap();
    }
    assert_eq!(detour.opens.load(Ordering::Acquire), 1);
    assert_eq!(server.accepted.load(Ordering::Acquire), 1);
    resolver
        .owner
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn binding_watch_retires_an_actual_pending_user_before_another_query_exists() {
    let server = Server::start(FirstConnection::Stalled).await;
    let resolver = server.resolver(EgressInterfaceControl::default());
    let detour = Arc::new(BoundDetour {
        control: EgressInterfaceControl::default(),
        opens: Arc::new(AtomicUsize::new(0)),
    });
    let first = tokio::spawn({
        let resolver = resolver.clone();
        let detour = detour.clone();
        async move {
            let query = crate::message::build_query("carrier.pending.test", crate::message::TYPE_A)
                .unwrap();
            resolver
                .exchange(&query, Some("actual-bound-carrier"), Some(detour.as_ref()))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), server.first_request.notified())
        .await
        .unwrap();
    detour.control.invalidate_network();
    let source = tokio::time::timeout(Duration::from_secs(2), first)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    let mut native: &(dyn std::error::Error + 'static) = &source;
    while native.downcast_ref::<h2::Error>().is_none() {
        native = native
            .source()
            .expect("actual H2 source must remain reachable");
    }
    assert!(
        resolver.owner.drained(),
        "actual old driver and user exit before any new connection"
    );
    assert_eq!(detour.opens.load(Ordering::Acquire), 1);
    let query = crate::message::build_query("carrier.next.test", crate::message::TYPE_A).unwrap();
    for _ in 0..2 {
        resolver
            .exchange(&query, Some("actual-bound-carrier"), Some(detour.as_ref()))
            .await
            .unwrap();
    }
    assert_eq!(detour.opens.load(Ordering::Acquire), 2);
    assert_eq!(server.accepted.load(Ordering::Acquire), 2);
    resolver
        .owner
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn an_adapter_without_a_binding_cannot_silently_reuse_a_detour_pool() {
    let server = Server::start(FirstConnection::Healthy).await;
    let resolver = server.resolver(EgressInterfaceControl::default());
    let query = crate::message::build_query("unbound.detour.test", crate::message::TYPE_A).unwrap();
    for _ in 0..2 {
        resolver
            .exchange(&query, Some("unverified-legacy"), Some(&LoopbackDetour))
            .await
            .unwrap();
    }
    assert_eq!(server.accepted.load(Ordering::Acquire), 2);
    resolver
        .owner
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}
