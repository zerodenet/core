use super::*;

fn policy(
    max_pending_bytes: usize,
    max_pending_frames: usize,
    max_connection_pending_bytes: usize,
) -> MieruReceivePolicy {
    MieruReceivePolicy {
        stall_timeout_ms: 1_000,
        max_pending_bytes,
        max_pending_frames,
        max_connection_pending_bytes,
    }
}

fn reader(policy: MieruReceivePolicy) -> (Reader, mpsc::Receiver<Command>) {
    let (ready, _incoming) = mpsc::channel(MAX_SESSIONS);
    let (outgoing, commands) = mpsc::channel(64);
    let (dropped, _drop_events) = mpsc::unbounded_channel();
    (Reader::new(ready, outgoing, dropped, policy), commands)
}

fn data(id: u32, payload: Vec<u8>) -> Segment {
    let mut meta = DataMetadata::new(DATA_CLIENT_TO_SERVER);
    meta.session_id = id;
    meta.payload_length = payload.len() as u16;
    Segment {
        session_meta: None,
        data_meta: Some(meta),
        payload,
    }
}

fn open(id: u32, payload: Vec<u8>) -> Segment {
    let mut meta = SessionMetadata::new(OPEN_SESSION_REQUEST);
    meta.session_id = id;
    meta.payload_length = payload.len() as u16;
    Segment {
        session_meta: Some(meta),
        data_meta: None,
        payload,
    }
}

fn assert_close(commands: &mut mpsc::Receiver<Command>, expected_id: u32) {
    assert!(matches!(
        commands.try_recv(),
        Ok(Command::Close {
            id,
            response: false
        }) if id == expected_id
    ));
}

#[tokio::test]
async fn continuously_ready_frames_give_a_healthy_consumer_time_to_run() {
    let (mut reader, _commands) = reader(policy(10, 10, 64));
    let mut stream = reader.create(1, Vec::new()).unwrap();
    let consumer = tokio::spawn(async move {
        for byte in 0..128 {
            assert_eq!(stream.incoming.recv().await.unwrap(), vec![byte]);
        }
    });
    for byte in 0..128 {
        reader.dispatch(data(1, vec![byte])).await.unwrap();
    }
    assert!(reader.sessions.contains_key(&1));
    tokio::task::yield_now().await;
    reader.drain_pending().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), consumer)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn incoming_data_refills_a_recovered_consumer_without_waiting_for_the_timer() {
    let (mut reader, _commands) = reader(policy(10, 10, 64));
    let mut stream = reader.create(1, Vec::new()).unwrap();
    for byte in 0..10 {
        reader.dispatch(data(1, vec![byte])).await.unwrap();
    }
    for byte in 0..8 {
        assert_eq!(stream.incoming.try_recv().unwrap(), vec![byte]);
    }

    // The consumer has recovered, but no timer has run. The next incoming
    // frame must release the older backlog in order instead of keeping it
    // inaccessible until the periodic drain.
    reader.dispatch(data(1, vec![10])).await.unwrap();
    for byte in 8..=10 {
        assert_eq!(stream.incoming.try_recv().unwrap(), vec![byte]);
    }
    assert!(!stream.status.closed());
}

#[tokio::test]
async fn session_backlog_limit_closes_only_the_violating_session() {
    let (mut reader, mut commands) = reader(policy(10, 10, 64));
    let stalled = reader.create(1, Vec::new()).unwrap();
    let mut healthy = reader.create(2, Vec::new()).unwrap();

    for byte in 0..10 {
        reader.dispatch(data(1, vec![byte])).await.unwrap();
    }
    assert_eq!(reader.sessions[&1].backlog.frames(), 2);
    reader.dispatch(data(2, b"healthy".to_vec())).await.unwrap();
    assert_eq!(healthy.incoming.recv().await.unwrap(), b"healthy");
    reader.drain_pending().await.unwrap();

    reader.dispatch(data(1, vec![10])).await.unwrap();

    assert!(!reader.sessions.contains_key(&1));
    assert!(reader.sessions.contains_key(&2));
    assert_eq!(reader.pending_bytes, 0);
    assert!(stalled.status.closed());
    assert_close(&mut commands, 1);
}

#[tokio::test]
async fn connection_backlog_limit_evicts_only_the_session_that_exceeds_it() {
    let (mut reader, mut commands) = reader(policy(18, 18, 18));
    let mut first = reader.create(1, Vec::new()).unwrap();
    let second = reader.create(2, Vec::new()).unwrap();

    for _ in 0..8 {
        reader.dispatch(data(1, vec![1])).await.unwrap();
        reader.dispatch(data(2, vec![2])).await.unwrap();
    }
    reader.dispatch(data(1, vec![3, 4])).await.unwrap();
    reader.dispatch(data(2, vec![5])).await.unwrap();

    assert!(reader.sessions.contains_key(&1));
    assert!(!reader.sessions.contains_key(&2));
    assert_eq!(reader.pending_bytes, 10);
    assert!(second.status.closed());
    assert_close(&mut commands, 2);

    assert_eq!(first.incoming.recv().await.unwrap(), vec![1]);
}

#[tokio::test]
async fn oversized_new_open_closes_only_that_session_and_preserves_budget() {
    let (mut reader, mut commands) = reader(policy(10, 10, 10));
    let mut existing = reader.create(1, Vec::new()).unwrap();
    for byte in 0..10 {
        reader.dispatch(data(1, vec![byte])).await.unwrap();
    }
    assert_eq!(reader.pending_bytes, 10);

    reader.dispatch(open(2, vec![99])).await.unwrap();

    assert!(reader.sessions.contains_key(&1));
    assert!(!reader.sessions.contains_key(&2));
    assert_eq!(reader.pending_bytes, 10);
    assert!(matches!(commands.try_recv(), Ok(Command::Open(2))));
    assert_close(&mut commands, 2);

    assert_eq!(existing.incoming.recv().await.unwrap(), vec![0]);
    reader.drain_pending().await.unwrap();
    reader.dispatch(data(1, vec![10])).await.unwrap();
    assert!(reader.sessions.contains_key(&1));
    assert_eq!(reader.pending_bytes, 10);
}

#[tokio::test]
async fn default_policy_keeps_a_short_stall_and_closes_at_thirty_seconds() {
    let (mut reader, mut commands) = reader(MieruReceivePolicy::default());
    let stream = reader.create(1, Vec::new()).unwrap();
    for byte in 0..9 {
        reader.dispatch(data(1, vec![byte])).await.unwrap();
    }
    let since = reader.sessions[&1].backlog.stalled_since().unwrap();

    reader
        .drain_pending_at(since + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(reader.sessions.contains_key(&1));

    reader
        .drain_pending_at(since + Duration::from_secs(30))
        .await
        .unwrap();
    assert!(!reader.sessions.contains_key(&1));
    assert!(stream.status.closed());
    assert_close(&mut commands, 1);
}

#[tokio::test]
async fn backlog_stall_timeout_restarts_only_after_consumer_progress() {
    let (mut reader, mut commands) = reader(policy(64, 64, 64));
    let mut stream = reader.create(1, Vec::new()).unwrap();
    for byte in 0..10 {
        reader.dispatch(data(1, vec![byte])).await.unwrap();
    }
    let since = reader.sessions[&1].backlog.stalled_since().unwrap();

    assert_eq!(stream.incoming.recv().await.unwrap(), vec![0]);
    reader
        .drain_pending_at(since + Duration::from_millis(900))
        .await
        .unwrap();
    assert_eq!(reader.sessions[&1].backlog.frames(), 1);

    reader
        .drain_pending_at(since + Duration::from_millis(1_700))
        .await
        .unwrap();
    assert!(reader.sessions.contains_key(&1));

    reader
        .drain_pending_at(since + Duration::from_millis(1_901))
        .await
        .unwrap();
    assert!(!reader.sessions.contains_key(&1));
    assert_eq!(reader.pending_bytes, 0);
    assert!(stream.status.closed());
    assert_close(&mut commands, 1);
}
