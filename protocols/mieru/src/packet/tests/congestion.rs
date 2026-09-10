use super::*;
use crate::packet::congestion::Cubic;
use std::{collections::VecDeque, num::NonZeroUsize, time::Duration};
use tokio::time::Instant;

#[test]
fn cubic_slow_start_and_bounds_match_the_reference() {
    let now = Instant::now();
    let mut cubic = Cubic::new(16, 18);
    assert_eq!(cubic.window_size(), 16);
    cubic.on_ack(now);
    assert_eq!(cubic.window_size(), 17);
    cubic.on_ack(now);
    cubic.on_ack(now);
    assert_eq!(cubic.window_size(), 18);
}

#[test]
fn cubic_loss_follows_the_reference_curve_and_timeout_restarts_slow_start() {
    let epoch = Instant::now();
    let mut cubic = Cubic::new(16, 4096);
    for _ in 0..16 {
        cubic.on_ack(epoch);
    }
    assert_eq!(cubic.window_size(), 32);

    cubic.on_loss(epoch);
    assert_eq!(cubic.window_size(), 22);
    cubic.on_ack(epoch + Duration::from_secs(3));
    assert!(cubic.window_size() >= 32);

    cubic.on_timeout();
    assert_eq!(cubic.window_size(), 16);
    cubic.on_ack(epoch + Duration::from_secs(4));
    assert_eq!(cubic.window_size(), 17);
}

#[test]
fn reliable_session_applies_each_new_ack_to_cubic_capacity() {
    let mut session = ReliableSession::with_fragment_size(1, true, NonZeroUsize::new(1).unwrap());
    session.queue_open().unwrap();
    session.queue_data(&[1; 15]).unwrap();
    assert_eq!(session.poll_transmissions(8).unwrap().len(), 16);

    let mut cumulative_ack = data(0, 16, 64, &[]);
    cumulative_ack.data_meta.as_mut().unwrap().protocol_type = ACK_SERVER_TO_CLIENT;
    session.receive(cumulative_ack).unwrap();
    assert_eq!(session.congestion_window(), 32);

    session.queue_data(&[2; 32]).unwrap();
    assert_eq!(session.poll_transmissions(8).unwrap().len(), 32);
}

#[test]
fn fast_retransmit_is_loss_but_elapsed_rto_is_timeout() {
    let mut session = ReliableSession::with_fragment_size(1, true, NonZeroUsize::new(1).unwrap());
    session.queue_open().unwrap();
    session.queue_data(&[1; 15]).unwrap();
    session.poll_transmissions(8).unwrap();
    let mut ack = data(0, 16, 64, &[]);
    ack.data_meta.as_mut().unwrap().protocol_type = ACK_SERVER_TO_CLIENT;
    session.receive(ack).unwrap();
    session.queue_data(&[2; 32]).unwrap();
    session.poll_transmissions(8).unwrap();

    let mut duplicate = data(0, 16, 64, &[]);
    duplicate.data_meta.as_mut().unwrap().protocol_type = ACK_SERVER_TO_CLIENT;
    for _ in 0..3 {
        session.receive(duplicate.clone()).unwrap();
    }
    assert_eq!(session.poll_transmissions(8).unwrap().len(), 1);
    assert_eq!(session.congestion_window(), 22);

    session.expire_retransmission_timers();
    assert!(!session.poll_transmissions(8).unwrap().is_empty());
    assert_eq!(session.congestion_window(), 16);
}

#[tokio::test(start_paused = true)]
async fn one_to_three_lost_packets_recover_across_a_long_rtt_link() {
    for lost in 1..=3 {
        let mut client =
            ReliableSession::with_fragment_size(1, true, NonZeroUsize::new(1).unwrap());
        let mut server =
            ReliableSession::with_fragment_size(1, false, NonZeroUsize::new(1).unwrap());
        let payload: Vec<_> = (0..64).collect();
        client.queue_open().unwrap();
        client.queue_data(&payload).unwrap();
        let through = client.next_send;

        let one_way = Duration::from_millis(750);
        let tick = Duration::from_millis(10);
        let mut client_to_server = VecDeque::new();
        let mut server_to_client = VecDeque::new();
        let mut dropped = [false; 3];
        let mut received = Vec::new();

        for _ in 0..2_000 {
            let now = Instant::now();
            for transmission in client.poll_transmissions(128).unwrap() {
                let drop_index = transmission
                    .segment
                    .data_meta
                    .as_ref()
                    .filter(|meta| meta.protocol_type == DATA_CLIENT_TO_SERVER)
                    .and_then(|meta| meta.sequence_number.checked_sub(1))
                    .map(|sequence| sequence as usize)
                    .filter(|index| *index < lost && !dropped[*index]);
                if let Some(index) = drop_index {
                    dropped[index] = true;
                } else {
                    client_to_server.push_back((now + one_way, transmission.segment));
                }
            }
            for transmission in server.poll_transmissions(128).unwrap() {
                server_to_client.push_back((now + one_way, transmission.segment));
            }

            while client_to_server
                .front()
                .is_some_and(|(delivery, _)| *delivery <= now)
            {
                let (_, segment) = client_to_server.pop_front().unwrap();
                server.receive(segment).unwrap();
            }
            while server_to_client
                .front()
                .is_some_and(|(delivery, _)| *delivery <= now)
            {
                let (_, segment) = server_to_client.pop_front().unwrap();
                client.receive(segment).unwrap();
            }
            while let Some(segment) = server.peek() {
                received.extend_from_slice(&segment.payload);
                server.consume().unwrap();
            }
            if received.len() == payload.len() && client.is_flushed(through) {
                break;
            }
            tokio::time::advance(tick).await;
        }

        assert!(dropped[..lost].iter().all(|dropped| *dropped));
        assert_eq!(received, payload);
        assert!(client.is_flushed(through));
    }
}

#[test]
fn retransmission_attempt_limit_returns_timeout_deterministically() {
    let mut session = ReliableSession::new(1, true);
    session.queue_open().unwrap();
    session.poll_transmissions(8).unwrap();

    for _ in 0..9 {
        session.expire_retransmission_timers();
        assert_eq!(session.poll_transmissions(8).unwrap().len(), 1);
    }
    session.expire_retransmission_timers();
    assert_eq!(
        session.poll_transmissions(8).err().unwrap().kind(),
        std::io::ErrorKind::TimedOut
    );
}
