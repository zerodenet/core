//! Original host/session/native-source fixture, not a replacement capability.
use crate::inventory::{PreparedUdpLeafCandidate, PreparedUdpOutbound};
use crate::protocol_registry::UdpAdapterContext;
use crate::runtime::udp_dispatch::LiveDirectSource;
use zero_engine::RouteMode;
#[tokio::test]
async fn direct_inventory_preserves_exact_policy_and_generation_without_a_compatibility_operation()
{
    let live = LiveDirectSource::new().await;
    let trace = live.runtime.route_trace(&live.session).await;
    let (resolved, _) = live
        .runtime
        .resolve_outbound(&trace.decision, &live.session)
        .unwrap();
    let config = live.proxy.engine().config();
    for mode in [RouteMode::Auto, RouteMode::Flow, RouteMode::Translate] {
        let prepared = live
            .proxy
            .protocols
            .prepare_udp_outbound(
                UdpAdapterContext::new(&config, live.runtime.runtime_services()),
                &live.session,
                &resolved,
                b"",
                mode,
            )
            .unwrap_or_else(|failure| panic!("{}: {}", failure.stage, failure.error));
        let PreparedUdpOutbound::Single(PreparedUdpLeafCandidate::Direct(selected)) = prepared
        else {
            panic!("Direct must execute through the installed native provider");
        };
        assert_eq!(selected.policy_tag, live.operation.policy_tag);
        assert_eq!(selected.dial_policy, live.operation.dial_policy);
        assert_eq!(selected.dial_generation, live.operation.dial_generation);
        selected
            .socket_source(
                live.proxy.current_service_source(),
                &live.runtime,
                &live.session,
                &live.context,
            )
            .await
            .unwrap();
    }
    let strict = live
        .proxy
        .protocols
        .prepare_udp_outbound(
            UdpAdapterContext::new(&config, live.runtime.runtime_services()),
            &live.session,
            &resolved,
            b"",
            RouteMode::Packet,
        )
        .err()
        .expect("Direct has no Flow-to-Packet stack");
    assert_eq!(strict.stage, "data_plane_route");
    live.close().await;
}
