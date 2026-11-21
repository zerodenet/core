//! A direct Service-role call has no external pending ledger to drive cleanup.
use super::*;
use std::sync::Arc;
use zero_core::{
    component::NetworkGeneration,
    request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts},
};
use zero_platform_tokio::contract::{
    context::{OpenContext, RequestIdentity},
    network::{CarrierContext, UpstreamBinding, UpstreamTransport},
    CancellationToken,
};
#[path = "no_observer/multiple.rs"]
mod multiple;
#[path = "no_observer/ready.rs"]
mod ready;
#[path = "no_observer/rejection.rs"]
mod rejection;

#[tokio::test]
async fn abandoned_direct_socks_service_role_closes_its_original_without_observer_next_dial_or_shutdown(
) {
    exercise_original_budget(1).await;
    exercise_original_budget(2).await;
}
async fn exercise_original_budget(originals: usize) {
    tokio::time::timeout(Duration::from_secs(3), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Destination::new(Address::Ipv4([127, 0, 0, 1]), listener.local_addr().unwrap().port());
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "runtime":{"native_stream_max_originals":originals},
            "outbounds":[{"tag":"node","protocol":{"type":"socks5","server":"127.0.0.1","port":endpoint.port}}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        handle.apply_runtime_config_and_wait(config, Duration::from_secs(3)).await.unwrap();
        let services = proxy.tcp_runtime_services().execution();
        let plan = services.components.as_ref().unwrap().clone();
        let slot = plan.configured("node").unwrap();
        let physical = slot.original_carrier_owner(0).unwrap();
        let role = slot.datagram(Instant::now() + Duration::from_secs(3)).await.unwrap();
        let cancel = CancellationToken::new();
        let end = Instant::now() + Duration::from_secs(3);
        let call = CarrierContext { session:None, deadline:Some(end), cancel:cancel.clone() };
        let binding = UpstreamBinding {
            prefix:vec![], dial:Default::default(),
            network_generation:NetworkGeneration(services.upstream().network_generation()),
        };
        let opener = crate::component::stream_bridge::network::NodeOpener::new(
            services.upstream_observed("node"), endpoint,
        ).with_plan(Some(Arc::downgrade(&plan))).with_service_call(call);
        let context = OpenContext {
            identity:RequestIdentity::Service(Arc::new(ServiceRequestFacts {
                origin:ServiceOrigin::Runtime, purpose:ServicePurpose::DiagnosticProbe,
            }).into()), deadline:Some(end), cancel:cancel.clone(), observer:None,
            upstream:UpstreamTransport::new(binding, Arc::new(opener)),
        };
        assert!(context.upstream.observed_pending_carrier().is_none());
        if originals == 1 {
            // SOCKS UDP declares two actual originals: TCP control and UDP.
            // An undersized native budget must reject before either is acquired.
            let failure = role.open(DatagramRequest {
                association:AssociationId(71),
                targets:DatagramTargets::Fixed(Destination::new(Address::Ipv4([203,0,113,9]),53)),
            }, context).await.err().expect("one native position cannot fund the actual pair");
            assert_eq!(failure.reason, zero_platform_tokio::contract::Reason::Capacity);
            assert!(physical.observe().unwrap().slots().is_empty());
            let accepting = listener.accept();
            tokio::pin!(accepting);
            std::future::poll_fn(|cx| {
                assert!(accepting.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            }).await;
            drop(role);
            running.shutdown().await.unwrap();
            assert!(physical.closed().unwrap().matches(&physical));
            assert!(physical.users_closed().unwrap().matches(&physical));
            return;
        }
        let (seen, greeting) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut tcp, _) = listener.accept().await.unwrap();
            let mut bytes = [0;3];
            tcp.read_exact(&mut bytes).await.unwrap();
            assert_eq!(bytes, [5,1,0]);
            seen.send(()).unwrap();
            // The original handshake remains Pending until its own call drops.
            let mut tail = Vec::new();
            tcp.read_to_end(&mut tail).await.unwrap();
            assert!(tail.is_empty());
            tcp.shutdown().await.unwrap();
        });
        let original;
        let reservations;
        {
            let operation = role.open(DatagramRequest {
                association:AssociationId(71),
                targets:DatagramTargets::Fixed(Destination::new(Address::Ipv4([203,0,113,9]),53)),
            }, context);
            tokio::pin!(operation);
            tokio::select! {
                result = &mut operation => panic!("unanswered actual SOCKS handshake returned: {:?}", result.err()),
                result = greeting => result.unwrap(),
            }
            let actual = physical.observe().unwrap();
            assert_eq!(actual.slots().len(),2,"both original positions are reserved");
            let acquired=actual.slots().iter().filter(|slot|slot.native().is_some()).collect::<Vec<_>>();
            assert_eq!(acquired.len(),1,"the UDP reservation has no native socket before BND response");
            let control=acquired[0];
            assert!(control.physical_view_live());
            assert!(control.driver().is_none(),"a taskless role invents no task join");
            original=control.original().clone();
            reservations=actual.slots().iter().map(|slot|slot.original().clone()).collect::<Vec<_>>();
            assert!(original.closed().is_none());
            // Only this real role future drops. No caller cancellation, next
            // acquire, external RetiredDatagrams pump or Host shutdown follows.
        }
        assert!(!cancel.is_cancelled());
        let receipt = loop {
            if let Some(receipt) = original.closed() {
                if physical.observe().unwrap().slots().is_empty() { break receipt; }
            }
            tokio::task::yield_now().await;
        };
        assert!(original.matches_closed(&receipt) && receipt.had_native_acquisition());
        for reserved in reservations {
            let closed=reserved.closed().expect("actual reserved original retired without an observer");
            assert!(reserved.matches_closed(&closed));
            assert_eq!(closed.had_native_acquisition(),reserved.same_original(&original));
        }
        assert!(physical.observe().unwrap().slots().is_empty(), "same N refunded only after real close");
        peer.await.unwrap();
        drop(role);
        running.shutdown().await.unwrap();
        assert!(physical.closed().unwrap().matches(&physical));
        assert!(physical.users_closed().unwrap().matches(&physical));
    }).await.unwrap();
}
