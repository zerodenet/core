use std::collections::VecDeque;
use std::time::Instant;

use zero_config::RuntimeConfig;
use zero_core::{Address, InboundUdpDispatch, ProtocolType};
use zero_transport::quic_initial::QuicInitialSniffer;

use super::{BufferedDispatch, FlowKey, PendingQuic, UdpSniffingState, MAX_QUEUED_DATAGRAMS};
use crate::runtime::sniff::{SniffingPolicy, SNIFF_TIMEOUT};
use crate::runtime::udp_ingress::UdpIngressRuntime;

fn runtime() -> UdpIngressRuntime {
    let config =
        RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#).unwrap();
    let proxy = crate::runtime::Proxy::new(config).unwrap();
    UdpIngressRuntime::new(proxy.tcp_runtime_services())
}

fn dispatch_for(target: Address, payload: &[u8]) -> InboundUdpDispatch {
    InboundUdpDispatch::new(
        ProtocolType::new("vless"),
        target,
        443,
        payload.to_vec(),
        Some(3),
    )
}

fn dispatch(payload: &[u8]) -> InboundUdpDispatch {
    dispatch_for(Address::Ipv4([203, 0, 113, 9]), payload)
}

#[tokio::test]
async fn non_quic_datagram_is_released_immediately_and_cached_as_fallback() {
    let policy = SniffingPolicy::new(true, &["quic".to_owned()], &[], false, false).unwrap();
    let mut state = UdpSniffingState::new(policy, runtime());
    state.observe(dispatch(b"ordinary datagram")).await;
    let first = state.pop_ready().expect("first datagram");
    assert_eq!(first.payload(), b"ordinary datagram");
    assert!(state.next_deadline().is_none());

    state.observe(dispatch(b"later datagram")).await;
    let second = state.pop_ready().expect("cached fallback datagram");
    assert_eq!(second.payload(), b"later datagram");
    assert!(state.next_deadline().is_none());
}

#[tokio::test]
async fn metadata_only_udp_never_starts_quic_buffering() {
    let policy = SniffingPolicy::new(true, &["quic".to_owned()], &[], true, false).unwrap();
    let mut state = UdpSniffingState::new(policy, runtime());
    state
        .observe(dispatch(&[0xc0, 0, 0, 0, 1, 0, 0, 0, 0]))
        .await;
    assert!(state.pop_ready().is_some());
    assert!(state.next_deadline().is_none());
}

#[test]
fn ready_datagrams_do_not_overtake_an_earlier_pending_sequence() {
    let policy = SniffingPolicy::new(true, &["quic".to_owned()], &[], false, false).unwrap();
    let mut state = UdpSniffingState::new(policy, runtime());
    state.ready.insert(1, dispatch(b"second"));
    assert!(state.pop_ready().is_none());
    state.ready.insert(0, dispatch(b"first"));
    assert_eq!(state.pop_ready().unwrap().payload(), b"first");
    assert_eq!(state.pop_ready().unwrap().payload(), b"second");
}

#[tokio::test]
async fn cross_target_pressure_releases_oldest_pending_without_dropping_or_reordering() {
    let policy = SniffingPolicy::new(true, &["quic".to_owned()], &[], false, false).unwrap();
    let mut state = UdpSniffingState::new(policy, runtime());
    let first = dispatch(b"first-pending");
    let key = FlowKey::from_dispatch(&first);
    state.queued_datagrams = 1;
    state.queued_bytes = first.payload().len();
    state.next_sequence = 1;
    state.pending.insert(
        key,
        PendingQuic {
            sniffer: QuicInitialSniffer::new(),
            datagrams: VecDeque::from([BufferedDispatch {
                sequence: 0,
                dispatch: first,
            }]),
            buffered_bytes: b"first-pending".len(),
            deadline: Instant::now() + SNIFF_TIMEOUT,
            fake_dns_fallback: false,
        },
    );

    for index in 1..MAX_QUEUED_DATAGRAMS {
        assert!(!state.release_for_capacity());
        state
            .observe(dispatch_for(
                Address::Ipv4([198, 51, 100, index as u8]),
                &[index as u8],
            ))
            .await;
    }
    assert_eq!(state.queued_datagrams, MAX_QUEUED_DATAGRAMS);
    assert!(state.release_for_capacity());
    assert_eq!(state.pop_ready().unwrap().payload(), b"first-pending");
    for expected in 1..MAX_QUEUED_DATAGRAMS {
        assert_eq!(state.pop_ready().unwrap().payload(), &[expected as u8]);
    }
    assert_eq!(state.queued_datagrams, 0);
    assert_eq!(state.queued_bytes, 0);
}
