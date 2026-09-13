#![cfg(feature = "runtime")]
use vless::reverse::*;

#[test]
fn reference_control_vectors_preserve_unknown_fields_and_states() {
    let active = Control::from_packet(&[0x9a, 0x06, 0x03, 1, 2, 3]).unwrap();
    assert_eq!(active.state, ACTIVE);
    assert_eq!(active.random, [1, 2, 3]);
    let drain = Control::from_packet(&[8, 1, 0x9a, 6, 1, 42]).unwrap();
    assert_eq!(drain.state, DRAIN);
    assert_eq!(drain.clone().into_packet(), [8, 1, 0x9a, 6, 1, 42]);
    let mut bridge = BridgeState::default();
    let unknown = Control::from_packet(&[8, 7, 0xa0, 6, 4]).unwrap();
    bridge.apply(&unknown);
    assert!(!bridge.active());
    assert!(Control::from_packet(&[0x9a, 6, 64, 1]).is_err());
    assert!(Control::from_packet(&[8, 0xff]).is_err());
}

#[test]
fn monitor_uses_active_workers_and_reference_integer_threshold() {
    assert!(needs_worker([]));
    assert!(!needs_worker([WorkerLoad {
        active: true,
        connections: 16
    }]));
    assert!(needs_worker([WorkerLoad {
        active: true,
        connections: 17
    }]));
    assert!(!needs_worker([
        WorkerLoad {
            active: true,
            connections: 17
        },
        WorkerLoad {
            active: true,
            connections: 16
        },
    ]));
    assert!(needs_worker([WorkerLoad {
        active: false,
        connections: 0
    }]));
}

#[test]
fn portal_sends_first_and_ten_second_heartbeats_and_drains_once() {
    let mut state = PortalHeartbeat::default();
    for tick in 0..11 {
        let message = state.tick(256);
        assert_eq!(message.is_some(), tick % 5 == 0);
        if let Some(message) = message {
            assert_eq!(message.state, ACTIVE);
            assert!((1..=64).contains(&message.random.len()));
        }
    }
    assert_eq!(state.tick(257).unwrap().state, DRAIN);
    assert!(state.draining());
    assert!(state.tick(258).is_none());
}
