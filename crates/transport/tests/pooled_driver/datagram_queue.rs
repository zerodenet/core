use super::*;
#[tokio::test]
async fn full_packet_queue_keeps_fifo_endings_and_rejects_new_data_without_losing_old_frames() {
    let (queue, mut receive) = channel();
    let mut first = queue.writer(vec![100]).unwrap();
    let mut second = queue.writer(vec![101]).unwrap();
    for value in 0..8 {
        first.send(vec![value]).unwrap();
    }
    assert_eq!(
        second.send(vec![9]).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    drop(first);
    drop(second);
    for value in 0..8 {
        assert_eq!(receive.recv().await.unwrap().into_bytes(), vec![value]);
    }
    assert_eq!(receive.recv().await.unwrap().into_bytes(), vec![100]);
    assert!(
        receive.try_recv().is_err(),
        "unsubmitted sessions must not emit a spurious END"
    );
    let mut next = queue.writer(vec![102]).unwrap();
    next.send(vec![10]).unwrap();
    drop(next);
    assert_eq!(receive.recv().await.unwrap().into_bytes(), vec![10]);
    assert_eq!(receive.recv().await.unwrap().into_bytes(), vec![102]);
}
#[tokio::test]
async fn physical_queue_bounds_retained_close_controllers_until_their_real_frames_return() {
    let (queue, mut receive) = channel();
    let mut writers = Vec::new();
    for _ in 0..LOGICAL_CONTROLS {
        writers.push(queue.writer(vec![1]).unwrap());
    }
    assert!(queue.writer(vec![1]).is_err());
    writers[0].send(vec![0]).unwrap();
    drop(writers);
    // A submitted END still carries its actual control reservation.
    let next = (0..LOGICAL_CONTROLS - 1)
        .map(|_| queue.writer(vec![1]).unwrap())
        .collect::<Vec<_>>();
    assert!(queue.writer(vec![1]).is_err());
    assert_eq!(receive.recv().await.unwrap().into_bytes(), vec![0]);
    assert!(queue.writer(vec![1]).is_err());
    assert_eq!(receive.recv().await.unwrap().into_bytes(), vec![1]);
    assert!(queue.writer(vec![1]).is_ok());
    drop(next);
}
