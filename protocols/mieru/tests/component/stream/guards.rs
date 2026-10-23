use super::*;
use crate::{
    component::{test_support::context_with, PoolFactory},
    transport::MieruOutboundOptionsRef,
};
use std::num::NonZeroUsize;
use zero_core::component::ResourceGeneration;
use zero_platform_tokio::contract::network::UpstreamTransport;

#[tokio::test]
async fn stale_selected_binding_is_rejected_before_the_persistent_grant_can_open_io() {
    let address = "127.0.0.1:1".parse().unwrap();
    let mut host = Harness::new(address, 1).await;
    let observer = Arc::new(Observations::default());
    let mut context = host.context(observer.clone());
    let mut binding = context.upstream.binding().clone();
    binding.network_generation.0 += 1;
    context.upstream = UpstreamTransport::new(binding, host.opener.clone());
    let error = host
        .role(0)
        .connect(
            ConnectRequest {
                destination: destination(),
            },
            context,
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.stage, Stage::Prepare);
    assert_eq!(error.reason, Reason::StalePlan);
    assert_eq!(error.delivery, DeliveryState::NotSubmitted);
    assert_eq!(
        host.opener.opens.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert!(observer.facts.lock().unwrap().is_empty());
    host.close().await;
}

#[tokio::test]
async fn canonical_dependency_generation_and_complete_pool_recipe_are_both_required() {
    let address = "127.0.0.1:1".parse().unwrap();
    let mut host = Harness::new(address, 1).await;
    for wrong_generation in [true, false] {
        let options = if wrong_generation {
            host.pool.component_options(NonZeroUsize::new(1).unwrap())
        } else {
            host.pool.component_options(NonZeroUsize::new(2).unwrap())
        };
        let (mut scope, mut context) = context_with(0, host.opener.clone());
        context.component = zero_core::component::ComponentId(99);
        context.background_carrier = None;
        context.dependencies = vec![zero_core::component::ComponentInstanceId {
            component: zero_core::component::ComponentId(61),
            generation: ResourceGeneration(if wrong_generation { 2 } else { 1 }),
        }]
        .into();
        let mut role = StreamFactory
            .build(
                Arc::new(prepare(&options, address).bind(zero_core::component::ComponentId(61))),
                context,
            )
            .unwrap();
        assert_eq!(
            role.lifecycle.start(deadline()).await.unwrap_err().reason,
            Reason::Revoked
        );
        assert!(role
            .lifecycle
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(scope.shutdown(deadline()).await.is_complete());
    }
    assert_eq!(
        host.opener.opens.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    host.close().await;
}

#[test]
fn native_options_reject_udp_and_invalid_protocol_options_without_dropping_fields() {
    let pool = Arc::new(crate::client::ClientPool::default());
    let recipe = pool.component_options(NonZeroUsize::new(1).unwrap());
    let valid = crate::config::MieruTransportOptions::default();
    let plain = pool.uncontrolled_component_options(NonZeroUsize::new(1).unwrap());
    assert_ne!(
        PoolFactory.describe(&recipe).static_identity,
        PoolFactory.describe(&plain).static_identity
    );
    assert!(matches!(
        plain.prepare_tcp(input(false, &valid)),
        Err(zero_core::Error::Unsupported(_))
    ));
    assert!(matches!(
        recipe.prepare_tcp(input(true, &valid)),
        Err(zero_core::Error::Unsupported(_))
    ));
    let mut invalid = valid.clone();
    invalid.receive.stall_timeout_ms = 0;
    assert!(matches!(
        recipe.prepare_tcp(input(false, &invalid)),
        Err(zero_core::Error::Config(_))
    ));
    let prepared = recipe.prepare_tcp(input(false, &valid)).unwrap();
    let debug = format!("{prepared:?}");
    assert!(!debug.contains("private-user") && !debug.contains("private-password"));
    assert_eq!(
        PoolFactory.describe(prepared.pool_options()).task_limit,
        NonZeroUsize::new(1)
    );
}

fn input(udp: bool, options: &crate::config::MieruTransportOptions) -> TcpPreparation<'_> {
    TcpPreparation {
        tag: "native",
        server: "127.0.0.1",
        port: 443,
        protocol: MieruOutboundOptionsRef {
            username: "private-user",
            password: "private-password",
            udp,
            options,
        },
        dial: Default::default(),
    }
}
