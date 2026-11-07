//! Runtime-configured budget of the actual one adapter pool, never per profile.
use super::*;
use std::sync::Arc;
use zero_core::request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts};
use zero_platform_tokio::{
    contract::{
        context::{OpenContext, RequestIdentity},
        network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
        stream::{BoxStream, ConnectRequest},
        BoxFuture, CancellationToken, ChannelError, Reason,
    },
    controlled_carrier::loan::StreamLoan,
};
mod setup;
struct Unused;
impl UpstreamOpener for Unused {
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: zero_core::request::Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async { panic!("actual Mieru pool must select its real provider") })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: zero_platform_tokio::contract::datagram::DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async { panic!("actual Mieru pool must select its real provider") })
    }
}
async fn open(proxy: &Proxy, tag: &str) -> Result<StreamLoan, ChannelError> {
    let services = proxy.tcp_runtime_services();
    let slot = services
        .components
        .as_ref()
        .unwrap()
        .configured(tag)
        .unwrap();
    slot.stream(deadline())
        .await
        .unwrap()
        .connect_carrier(
            ConnectRequest {
                destination: destination(),
            },
            OpenContext {
                identity: RequestIdentity::Service(
                    Arc::new(ServiceRequestFacts {
                        origin: ServiceOrigin::Runtime,
                        purpose: ServicePurpose::BackgroundControl,
                    })
                    .into(),
                ),
                deadline: Some(deadline()),
                cancel: CancellationToken::new(),
                observer: None,
                upstream: UpstreamTransport::new(
                    UpstreamBinding {
                        prefix: vec![],
                        dial: slot.dial.clone(),
                        network_generation: zero_core::component::NetworkGeneration(
                            services.upstream().network_generation(),
                        ),
                    },
                    Arc::new(Unused),
                ),
            },
        )
        .await
        .map(|connected| connected.io)
}
async fn echo(loan: &mut StreamLoan) {
    loan.io.write_all(b"budget").await.unwrap();
    let mut bytes = [0; 6];
    loan.io.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"budget");
}
async fn returned(loan: StreamLoan) {
    let StreamLoan { io, mut owner } = loan;
    drop(io);
    let report = owner.close(deadline()).await.unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert!(owner.closed().unwrap().matches(&owner));
}
#[tokio::test]
async fn ordinary_mieru_configured_total_budget_refuses_other_profiles_and_replaces_only_after_original_cleanup(
) {
    tokio::time::timeout(Duration::from_secs(25), async {
        let a = Server::new(false).await;
        let b = Server::new(false).await;
        let c = Server::new(false).await;
        let mut config = config([a.address, b.address, c.address]);
        config.runtime.mieru_max_carriers = Some(1);
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let handle = ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy.clone(),
        );
        handle
            .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(
            a.accepts.load(Ordering::SeqCst)
                + b.accepts.load(Ordering::SeqCst)
                + c.accepts.load(Ordering::SeqCst),
            0,
            "capacity is not native preallocation"
        );
        let old_slot = proxy
            .tcp_runtime_services()
            .components
            .as_ref()
            .unwrap()
            .configured("a")
            .unwrap();
        let mut first = open(&proxy, "a").await.unwrap();
        echo(&mut first).await;
        let original = first.owner.original().clone();
        assert_eq!(original.owner().limit().get(), 1);
        let error = match open(&proxy, "b").await {
            Ok(_) => panic!("a different profile cannot borrow a second physical slot"),
            Err(error) => error,
        };
        assert_eq!(error.reason, Reason::Capacity, "{error}");
        assert_eq!(b.accepts.load(Ordering::SeqCst), 0);
        // Logical reuse consumes no second physical reservation and does not
        // evict or cancel the active user when another profile was refused.
        let mut same = open(&proxy, "a").await.unwrap();
        echo(&mut same).await;
        assert!(same.owner.original().same_original(&original));
        assert_eq!(a.accepts.load(Ordering::SeqCst), 1);
        returned(same).await;
        echo(&mut first).await;
        config.runtime.mieru_max_carriers = Some(2);
        let replacing = tokio::spawn(async move {
            handle
                .apply_runtime_config_and_wait(config, Duration::from_secs(10))
                .await
        });
        original.driver().unwrap().wait().await;
        assert!(
            !replacing.is_finished(),
            "joined old driver is insufficient while its actual logical control is retained"
        );
        // Physical retirement can complete before the actual protocol loan
        // returns. It is a native-only receipt, not a logical user ACK.
        tokio::time::timeout_at(deadline(), async {
            loop {
                if let Some(closed) = original.closed() {
                    assert!(closed.matches(&original) && closed.had_native_acquisition());
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(original.owner().closed().unwrap().matches(original.owner()));
        assert!(first.owner.closed().is_none());
        let retained = first
            .owner
            .close(tokio::time::Instant::now())
            .await
            .unwrap();
        assert!(
            !retained.is_complete(),
            "actual logical view/control remains held"
        );
        assert!(first.owner.closed().is_none());
        assert!(
            !replacing.is_finished(),
            "native close alone cannot ACK replacement"
        );
        returned(first).await;
        replacing.await.unwrap().unwrap();
        assert!(original.closed().unwrap().matches(&original));
        assert!(original.owner().closed().unwrap().matches(original.owner()));
        let next = proxy
            .tcp_runtime_services()
            .components
            .as_ref()
            .unwrap()
            .configured("a")
            .unwrap();
        assert_ne!(
            old_slot.instance_id(),
            next.instance_id(),
            "configured total budget is part of actual pool replacement identity"
        );
        assert_eq!(
            a.accepts.load(Ordering::SeqCst),
            1,
            "new generation remains cold until actual demand"
        );
        assert_eq!(b.accepts.load(Ordering::SeqCst), 0);
        let mut next_a = open(&proxy, "a").await.unwrap();
        echo(&mut next_a).await;
        let mut next_b = open(&proxy, "b").await.unwrap();
        echo(&mut next_b).await;
        assert_eq!(next_a.owner.original().owner().limit().get(), 2);
        assert!(!next_a
            .owner
            .original()
            .same_original(next_b.owner.original()));
        let error = match open(&proxy, "c").await {
            Ok(_) => panic!("two total slots cannot fund a third profile"),
            Err(error) => error,
        };
        assert_eq!(error.reason, Reason::Capacity, "{error}");
        assert_eq!(c.accepts.load(Ordering::SeqCst), 0);
        returned(next_a).await;
        returned(next_b).await;
        running.shutdown().await.unwrap();
        a.close().await;
        b.close().await;
        c.close().await;
    })
    .await
    .unwrap();
}
