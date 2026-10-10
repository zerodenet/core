// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0
use super::*;
use crate::noise::{TunnResult, index_table::IndexTable, rate_limiter::RateLimiter};
use crate::x25519::{PublicKey, StaticSecret};
use mock_instant::thread_local::MockClock;
use std::sync::Arc;

fn pair(keepalive: Option<u16>) -> (Tunn, Tunn) {
    MockClock::set_time(Duration::ZERO);
    let make = |private: u8, remote: u8, keepalive| {
        let private = StaticSecret::from([private; 32]);
        let public = PublicKey::from(&private);
        Tunn::new(
            private,
            PublicKey::from(&StaticSecret::from([remote; 32])),
            None,
            keepalive,
            IndexTable::from_os_rng(),
            Arc::new(RateLimiter::new(&public, 100)),
        )
    };
    (make(11, 22, keepalive), make(22, 11, None))
}
fn network(result: TunnResult) -> WgKind {
    match result {
        TunnResult::WriteToNetwork(packet) => packet,
        _ => panic!("expected network packet"),
    }
}
fn establish(a: &mut Tunn, b: &mut Tunn) {
    let initiation = a.format_handshake_initiation(false).unwrap();
    let response = network(b.handle_incoming_packet(initiation.into()));
    let keepalive = network(a.handle_incoming_packet(response));
    b.handle_incoming_packet(keepalive);
}

#[test]
fn exact_deadline_exposes_sampled_retries_and_attempt_expiration_without_resampling() {
    let (mut a, _) = pair(Some(20));
    assert_eq!(a.next_timer_delay(), Some(Duration::ZERO));
    assert!(matches!(
        a.update_timers(),
        Ok(Some(WgKind::HandshakeInit(_)))
    ));
    let next = a.next_timer_delay().unwrap();
    assert!(next >= REKEY_TIMEOUT && next <= REKEY_TIMEOUT + MAX_JITTER);
    assert_eq!(a.next_timer_delay(), Some(next));
    MockClock::advance(next - Duration::from_nanos(1));
    assert_eq!(a.next_timer_delay(), Some(Duration::from_nanos(1)));
    assert!(matches!(a.update_timers(), Ok(None)));
    MockClock::advance(Duration::from_nanos(1));
    assert_eq!(a.next_timer_delay(), Some(Duration::ZERO));
    assert!(matches!(
        a.update_timers(),
        Ok(Some(WgKind::HandshakeInit(_)))
    ));
    MockClock::set_time(REKEY_ATTEMPT_TIME);
    assert_eq!(a.next_timer_delay(), Some(Duration::ZERO));
    assert!(matches!(
        a.update_timers(),
        Err(WireGuardError::ConnectionExpired)
    ));
    assert_eq!(a.next_timer_delay(), None);
    a.format_handshake_initiation(false).unwrap();
    assert!(
        a.next_timer_delay()
            .is_some_and(|next| next > Duration::ZERO)
    );
}

#[test]
fn inactive_peers_schedule_key_destruction_and_authenticated_receive_revives_them() {
    let (mut a, mut b) = pair(None);
    assert_eq!(a.next_timer_delay(), Some(REJECT_AFTER_TIME * 3));
    MockClock::advance(REJECT_AFTER_TIME * 3);
    assert!(matches!(
        a.update_timers(),
        Err(WireGuardError::ConnectionExpired)
    ));
    assert_eq!(a.next_timer_delay(), None);
    establish(&mut b, &mut a);
    assert_eq!(
        a.next_timer_delay(),
        Some(REJECT_AFTER_TIME + Duration::from_nanos(1))
    );
    assert!(!a.is_expired());
}

#[test]
fn packet_io_after_long_idle_stamps_authentication_and_keepalive_at_actual_time() {
    let (mut a, mut b) = pair(Some(20));
    MockClock::advance(Duration::from_secs(35));
    establish(&mut a, &mut b);
    assert_eq!(a.timers[TimeSessionEstablished], Duration::from_secs(35));
    assert_eq!(b.timers[TimeSessionEstablished], Duration::from_secs(35));
    assert_eq!(a.next_timer_delay(), Some(Duration::from_secs(20)));
    assert_eq!(
        b.next_timer_delay(),
        Some(REJECT_AFTER_TIME + Duration::from_nanos(1))
    );
    MockClock::advance(Duration::from_secs(15));
    let keepalive = b
        .handle_outgoing_packet(
            crate::packet::Packet::from_bytes(bytes::BytesMut::new()),
            None,
        )
        .unwrap();
    a.handle_incoming_packet(keepalive);
    assert_eq!(a.timers[TimePersistentKeepalive], Duration::from_secs(50));
    assert_eq!(a.next_timer_delay(), Some(Duration::from_secs(20)));
}

#[test]
fn occupied_session_deadline_uses_strict_expiry_and_does_not_spin_at_the_boundary() {
    let (mut a, mut b) = pair(None);
    establish(&mut a, &mut b);
    assert_eq!(
        b.next_timer_delay(),
        Some(REJECT_AFTER_TIME + Duration::from_nanos(1))
    );
    MockClock::advance(REJECT_AFTER_TIME);
    assert_eq!(b.next_timer_delay(), Some(Duration::from_nanos(1)));
    assert!(matches!(b.update_timers(), Ok(None)));
    assert!(b.sessions.iter().any(Option::is_some));
    MockClock::advance(Duration::from_nanos(1));
    assert!(matches!(b.update_timers(), Ok(None)));
    assert!(b.sessions.iter().all(Option::is_none));
    assert!(
        b.next_timer_delay()
            .is_some_and(|next| next > Duration::ZERO)
    );
}

#[test]
fn silence_keepalive_rekey_and_configuration_use_the_owner_timer_state() {
    let (mut a, mut b) = pair(None);
    establish(&mut a, &mut b);
    b.timers.want_keepalive = Some(Duration::ZERO);
    assert_eq!(b.next_timer_delay(), Some(KEEPALIVE_TIMEOUT));
    MockClock::advance(KEEPALIVE_TIMEOUT);
    assert!(matches!(b.update_timers(), Ok(Some(WgKind::Data(_)))));
    assert!(b.timers.want_keepalive.is_none());
    b.set_persistent_keepalive(Some(20));
    assert_eq!(b.next_timer_delay(), Some(Duration::ZERO));
    b.update_timers().unwrap();
    assert_eq!(b.next_timer_delay(), Some(Duration::from_secs(20)));
    b.set_persistent_keepalive(None);
    b.timers.want_handshake = Some(b.timers.now());
    let expected = b.timers.new_handshake_timeout;
    assert_eq!(b.next_timer_delay(), Some(expected));
    MockClock::advance(expected);
    assert!(matches!(
        b.update_timers(),
        Ok(Some(WgKind::HandshakeInit(_)))
    ));
    // Initiator rekey is armed only after actual send/receive demand.
    a.timers[TimeLastDataPacketSent] = Duration::from_nanos(1);
    assert_eq!(
        a.next_timer_delay(),
        Some(REKEY_AFTER_TIME - b.timers.now())
    );
    a.timers[TimeLastDataPacketSent] = Duration::ZERO;
    a.timers[TimeLastDataPacketReceived] = Duration::from_nanos(1);
    assert_eq!(
        a.next_timer_delay(),
        Some(REJECT_AFTER_TIME - KEEPALIVE_TIMEOUT - REKEY_TIMEOUT - a.timers.now())
    );
}

#[test]
fn cookie_expiration_is_scheduled_even_when_no_packets_are_exchanged() {
    let (mut a, mut b) = pair(None);
    let initiation = a.format_handshake_initiation(false).unwrap();
    let remote_public = PublicKey::from(&StaticSecret::from([22; 32]));
    let limiter = RateLimiter::new(&remote_public, 0);
    let cookie =
        match limiter.verify_packet("192.0.2.1:51820".parse().unwrap(), initiation.into_bytes()) {
            Err(result) => network(result),
            Ok(_) => panic!("expected a cookie challenge"),
        };
    a.handle_incoming_packet(cookie);
    assert!(a.handshake.has_cookie());
    let initiation = a.format_handshake_initiation(true).unwrap();
    let response = network(b.handle_incoming_packet(initiation.into()));
    let keepalive = network(a.handle_incoming_packet(response));
    b.handle_incoming_packet(keepalive);
    assert_eq!(a.next_timer_delay(), Some(COOKIE_EXPIRATION_TIME));
    MockClock::advance(COOKIE_EXPIRATION_TIME);
    assert_eq!(a.next_timer_delay(), Some(Duration::ZERO));
    a.update_timers().unwrap();
    assert!(!a.handshake.has_cookie());
    assert!(
        a.next_timer_delay()
            .is_some_and(|next| next > Duration::ZERO)
    );
}

#[test]
fn large_clock_advance_expires_once_and_never_replays_a_retry_burst() {
    let (mut a, _) = pair(Some(20));
    a.update_timers().unwrap();
    MockClock::advance(Duration::from_secs(3600));
    assert_eq!(a.next_timer_delay(), Some(Duration::ZERO));
    assert!(matches!(
        a.update_timers(),
        Err(WireGuardError::ConnectionExpired)
    ));
    assert_eq!(a.next_timer_delay(), None);
}

#[test]
fn expired_sessions_are_rejected_on_io_before_a_late_executor_timer_runs() {
    let (mut a, mut b) = pair(None);
    establish(&mut a, &mut b);
    let delayed = a
        .handle_outgoing_packet(
            crate::packet::Packet::from_bytes(bytes::BytesMut::new()),
            None,
        )
        .unwrap();
    MockClock::advance(REJECT_AFTER_TIME + Duration::from_nanos(1));
    // No update_timers call: model an executor that did not advance during sleep.
    assert!(matches!(
        b.handle_incoming_packet(delayed),
        TunnResult::Err(WireGuardError::NoCurrentSession)
    ));
    assert!(b.sessions.iter().all(Option::is_none));
    assert!(matches!(
        a.handle_outgoing_packet(
            crate::packet::Packet::from_bytes(bytes::BytesMut::new()),
            None
        ),
        Some(WgKind::HandshakeInit(_))
    ));
    assert!(a.sessions.iter().all(Option::is_none));
}

#[test]
fn late_handshake_response_cannot_revive_expired_attempt_keys() {
    let (mut a, mut b) = pair(None);
    let initiation = a.format_handshake_initiation(false).unwrap();
    let response = network(b.handle_incoming_packet(initiation.into()));
    MockClock::advance(REKEY_ATTEMPT_TIME);
    assert!(matches!(
        a.handle_incoming_packet(response),
        TunnResult::Err(_)
    ));
    assert!(a.is_expired());
    assert_eq!(a.next_timer_delay(), None);
    assert!(a.sessions.iter().all(Option::is_none));
    assert!(a.format_handshake_initiation(false).is_some());
    assert!(!a.is_expired());
}
