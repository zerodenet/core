//! Same original Source memory and real Scope progression; no OS close claim.
use super::*;
use std::{future::Future, sync::Arc};
use zero_core::{Address, ProtocolType};
use zero_platform_tokio::contract::scope::{TaskClass, TaskScope};
use zero_traits::packet_metadata::{PacketMetadataCredit, PacketMetadataLayout};
use zero_tun::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
struct HeldCpu {
    _bytes: Box<[u8]>,
    _credit: PacketMetadataCredit,
}
fn runtime() -> crate::runtime::udp_ingress::UdpIngressRuntime {
    let config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap();
    let proxy = crate::runtime::Proxy::new(config).unwrap();
    crate::runtime::udp_ingress::UdpIngressRuntime::new(proxy.tcp_runtime_services())
}
fn state(root: &Arc<PacketReturnTotals>) -> UdpSniffingState {
    let policy = SniffingPolicy::new(true, &["quic".into()], &[], false, false).unwrap();
    UdpSniffingState::from_memory(policy, runtime(), Some(root.driver_metadata_view()))
}
fn name() -> Decision {
    Decision::Domain {
        name: InlineName::new("example.com").unwrap(),
        protocol: SniffedProtocol::Quic,
        source: TargetHostSource::QuicSni,
        fake_dns_fallback: false,
    }
}
fn hold_name(root: &Arc<PacketReturnTotals>) -> HeldCpu {
    let before = root.driver_metadata().occupied;
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        before
            .checked_add(PacketMetadataLayout::array::<u8>(11).unwrap())
            .unwrap(),
    )
    .unwrap();
    let credit = root
        .driver_metadata_view()
        .reserve(PacketMetadataLayout::array::<u8>(11).unwrap())
        .unwrap();
    HeldCpu {
        _bytes: vec![0; 11].into_boxed_slice(),
        _credit: credit,
    }
}
fn packet(payload: zero_traits::PacketBuffer) -> InboundUdpDispatch {
    InboundUdpDispatch::new_with_packet(
        ProtocolType::UNKNOWN,
        Address::Ipv4([192, 0, 2, 1]),
        443,
        payload,
        None,
    )
}
#[tokio::test]
async fn known_name_wait_preserves_original_and_fifo_until_real_paid_owner_is_freed() {
    let root = Arc::new(PacketReturnTotals::default());
    root.install_native(2, 68).unwrap();
    let initial = root.driver_metadata().occupied;
    let read = root.native_grant(64).unwrap();
    let view = read.memory_view().unwrap();
    let mut input = read.try_buffer().unwrap();
    input.as_mut_bytes()[..5].copy_from_slice(b"first");
    let first = input
        .into_packet(5)
        .unwrap_or_else(|_| panic!("actual initialized input"));
    let first_pointer = first.as_ptr();
    let second = view.copy_received_packet(b"two").unwrap();
    let second_pointer = second.as_ptr();
    let mut state = state(&root);
    state.push_decision(0, None, packet(first), name());
    state.push(1, None, packet(second));
    let foreign = hold_name(&root);
    let native = root.native();
    assert!(state.pop_ready().is_none());
    assert!(matches!(
        state.packets.current_mut(0).unwrap().decision,
        Decision::Domain { .. }
    ));
    {
        let pending = state.wait_capacity();
        tokio::pin!(pending);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                pending.as_mut().poll(cx).is_pending()
            ))
            .await
        );
    }
    assert!(
        state.pop_ready().is_none(),
        "a later fallback packet cannot overtake the known name"
    );
    assert_eq!(root.native(), native);
    drop(foreign); // actual allocation first, then its same-Root ticket/wake
    state.wait_capacity().await;
    let first = state.pop_ready().unwrap();
    assert_eq!(first.payload().as_ptr(), first_pointer);
    assert_eq!(first.target(), &Address::Domain("example.com".into()));
    assert!(first.domain_metadata().is_some());
    let second = state.pop_ready().unwrap();
    assert_eq!(second.payload().as_ptr(), second_pointer);
    assert_eq!(second.payload(), b"two");
    assert!(state.pop_ready().is_none());
    drop(state);
    read.revoke();
    assert_eq!(
        root.native().occupied.frames,
        2,
        "returning inspector Scope storage does not return delivered views"
    );
    drop(first);
    drop(second);
    drop(view);
    drop(read);
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
    assert_eq!(root.driver_metadata().occupied, initial);
}
#[tokio::test]
async fn known_name_scope_cancel_keeps_native_until_actual_body_drop_and_registration_until_host_ack(
) {
    let root = Arc::new(PacketReturnTotals::default());
    root.install_native(1, 64).unwrap();
    let read = root.native_grant(64).unwrap();
    let input = read
        .try_buffer()
        .unwrap()
        .into_packet(1)
        .unwrap_or_else(|_| panic!("same real read"));
    let mut state = state(&root);
    state.push_decision(0, None, packet(input), name());
    // The original provider has sealed new reads; this accepted buffer is
    // still charged. Set pressure after its unused cold promises really free.
    read.revoke();
    let foreign = hold_name(&root);
    let mut scope = TaskScope::suspended_hosted(
        tokio::runtime::Handle::current(),
        std::num::NonZeroUsize::new(1),
    );
    scope.enable_registration().unwrap();
    let tasks = scope.registrar();
    let stop = tasks.cancellation();
    let (entered, pending) = tokio::sync::oneshot::channel();
    let (_, witness) = tasks
        .spawn_tracked(TaskClass::Critical, async move {
            let work = async move {
                assert!(state.pop_ready().is_none());
                assert!(matches!(
                    state.packets.current_mut(0).unwrap().decision,
                    Decision::Domain { .. }
                ));
                let waiting = state.wait_capacity();
                tokio::pin!(waiting);
                assert!(
                    std::future::poll_fn(|cx| std::task::Poll::Ready(
                        waiting.as_mut().poll(cx).is_pending()
                    ))
                    .await
                );
                entered.send(()).unwrap();
                waiting.await;
                panic!("no owner was freed before this actual Scope cancellation");
            };
            tokio::select! {biased; _=stop.cancelled()=>Ok(()), _=work=>unreachable!("the exact held owner was not returned")}
        })
        .unwrap();
    pending.await.unwrap();
    assert_eq!(root.native().occupied.frames, 1);
    scope.begin_shutdown();
    let exit = scope.next_receipt().await.unwrap();
    assert!(witness.matches_joined(exit.joined()));
    assert!(exit.result().is_ok());
    assert_eq!(
        root.native().occupied.frames,
        0,
        "actual task body dropped the held original"
    );
    assert!(
        !exit.joined().registration_released(),
        "memory free is not Host ACK"
    );
    scope.acknowledge(&exit).unwrap();
    witness.wait_registration_released().await;
    drop(foreign);
    drop(read);
}
