use super::*;
use crate::{metadata::*, segment::Segment};
use tokio::sync::oneshot::error::TryRecvError;

fn ack(through: u32) -> Segment {
    let mut meta = DataMetadata::new(ACK_SERVER_TO_CLIENT);
    meta.session_id = 1;
    meta.unack_sequence = through;
    meta.window_size = 8;
    Segment {
        session_meta: None,
        data_meta: Some(meta),
        payload: Vec::new(),
    }
}

fn state() -> State {
    State {
        reliable: ReliableSession::new(1, true),
        barriers: Vec::new(),
        closing: None,
    }
}

fn push_barrier(state: &mut State, close: bool) -> oneshot::Receiver<io::Result<()>> {
    let (done, receiver) = oneshot::channel();
    state.barriers.push(Barrier {
        through: state.reliable.next_send,
        close,
        done,
    });
    receiver
}

#[test]
fn acknowledged_flush_succeeds_when_peer_closes_afterward() {
    let mut state = state();
    state.reliable.queue_open().unwrap();
    state.reliable.poll_transmissions(8).unwrap();
    let mut receiver = push_barrier(&mut state, false);
    state.reliable.receive(ack(1)).unwrap();

    state.complete_barriers(Retirement::PeerClosed);

    assert!(receiver.try_recv().unwrap().is_ok());
}

#[test]
fn peer_close_fails_an_unacknowledged_flush() {
    let mut state = state();
    state.reliable.queue_open().unwrap();
    state.reliable.poll_transmissions(8).unwrap();
    state.reliable.receive(ack(1)).unwrap();
    state.reliable.queue_data(b"pending business data").unwrap();
    state.reliable.poll_transmissions(8).unwrap();
    let mut receiver = push_barrier(&mut state, false);

    state.complete_barriers(Retirement::PeerClosed);

    assert_eq!(
        receiver.try_recv().unwrap().unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn received_close_fails_pending_flush_and_duplicate_only_answers_tombstone() {
    let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (ready, _ready_rx) = mpsc::channel(1);
    let (outgoing, _outgoing_rx) = mpsc::channel(8);
    let (dropped, _dropped_rx) = mpsc::unbounded_channel();
    let book = Reader::new(ready, outgoing, dropped, Default::default());
    let mut driver = Driver::new(
        book,
        PacketCodec::new([9; 32], "user"),
        PacketIo::Client {
            carrier: Arc::new(crate::client::UdpClientDatagramCarrier::new(
                socket,
                peer.local_addr().unwrap(),
            )),
        },
        true,
    );
    driver.start(1).unwrap();
    driver.start(2).unwrap();

    let status = Arc::new(Status::default());
    let state = driver.states.get_mut(&1).unwrap();
    state.reliable.queue_open().unwrap();
    state.reliable.poll_transmissions(8).unwrap();
    state.reliable.receive(ack(1)).unwrap();
    driver
        .command(Command::Data {
            id: 1,
            payload: b"pending business data".to_vec(),
            status: status.clone(),
        })
        .unwrap();
    driver
        .states
        .get_mut(&1)
        .unwrap()
        .reliable
        .poll_transmissions(8)
        .unwrap();
    let (done, mut flush) = oneshot::channel();
    driver
        .command(Command::Barrier {
            id: 1,
            close: false,
            status,
            done,
        })
        .unwrap();
    let mut other_flush = push_barrier(driver.states.get_mut(&2).unwrap(), false);

    let mut close = SessionMetadata::new(CLOSE_SESSION_REQUEST);
    close.session_id = 1;
    driver
        .receive(Segment {
            session_meta: Some(close.clone()),
            data_meta: None,
            payload: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(
        flush.try_recv().unwrap().unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );

    driver
        .receive(Segment {
            session_meta: Some(close),
            data_meta: None,
            payload: Vec::new(),
        })
        .await
        .unwrap();
    assert!(driver.states.contains_key(&2));
    assert!(matches!(other_flush.try_recv(), Err(TryRecvError::Empty)));

    let mut response = [0; 1501];
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(1), peer.recv_from(&mut response))
            .await
            .unwrap()
            .unwrap();
    }
}

#[test]
fn shutdown_waits_for_close_handshake_after_data_is_acknowledged() {
    let mut state = state();
    state.reliable.queue_open().unwrap();
    state.reliable.poll_transmissions(8).unwrap();
    let mut receiver = push_barrier(&mut state, true);
    state.reliable.receive(ack(1)).unwrap();

    state.advance_barriers().unwrap();

    assert!(state.closing.is_some());
    assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
    state.complete_barriers(Retirement::LocalCloseAcknowledged);
    assert!(receiver.try_recv().unwrap().is_ok());
}

#[test]
fn peer_close_wins_race_with_acked_shutdown_before_close_is_sent() {
    let mut state = state();
    state.reliable.queue_open().unwrap();
    state.reliable.poll_transmissions(8).unwrap();
    let mut receiver = push_barrier(&mut state, true);
    state.reliable.receive(ack(1)).unwrap();

    state.complete_barriers(Retirement::PeerClosed);

    assert!(state.closing.is_none());
    assert!(receiver.try_recv().unwrap().is_ok());
}

#[test]
fn crossed_and_repeated_peer_close_complete_an_acked_shutdown_once() {
    let mut state = state();
    state.reliable.queue_open().unwrap();
    state.reliable.poll_transmissions(8).unwrap();
    let mut receiver = push_barrier(&mut state, true);
    state.reliable.receive(ack(1)).unwrap();
    state.advance_barriers().unwrap();

    state.complete_barriers(Retirement::PeerClosed);
    state.complete_barriers(Retirement::PeerClosed);

    assert!(receiver.try_recv().unwrap().is_ok());
    assert!(state.barriers.is_empty());
}

#[test]
fn missing_close_response_times_out_shutdown_even_after_data_ack() {
    let mut state = state();
    state.reliable.queue_open().unwrap();
    state.reliable.poll_transmissions(8).unwrap();
    let mut receiver = push_barrier(&mut state, true);
    state.reliable.receive(ack(1)).unwrap();
    state.advance_barriers().unwrap();

    state.complete_barriers(Retirement::Failed(io::ErrorKind::TimedOut));

    assert_eq!(
        receiver.try_recv().unwrap().unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
}
