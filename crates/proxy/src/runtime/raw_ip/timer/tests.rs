use super::*;

#[tokio::test(start_paused = true)]
async fn peer_deadlines_are_independent_and_rearming_retains_no_history() {
    let mut timers = PeerTimers::default();
    timers.rebuild(1024, |_| TimerSchedule::Parked);
    timers.update(0, TimerSchedule::After(Duration::from_secs(20)));
    timers.update(1, TimerSchedule::After(Duration::from_secs(5)));
    let first = timers.deadline();
    for _ in 0..10_000 {
        timers.update(0, TimerSchedule::After(Duration::from_secs(30)));
    }
    assert_eq!(timers.ordered.len(), 2);
    assert_eq!(timers.deadline(), first);
    assert_eq!(timers.pop_due(), None);
    wait(first).await;
    assert_eq!(timers.pop_due(), Some(1));
    assert_eq!(timers.pop_due(), None);
    timers.update(0, TimerSchedule::Parked);
    assert_eq!(timers.deadline(), None);
    // Removed indices and old generations leave neither wakes nor labels.
    timers.rebuild(1, |_| TimerSchedule::After(Duration::from_secs(2)));
    timers.update(1023, TimerSchedule::After(Duration::ZERO));
    assert_eq!(timers.ordered.len(), 1);
    assert_eq!(timers.peers.len(), 1);
    assert!(timers.ordered.capacity() <= 16);
    assert!(timers.peers.capacity() <= 16);
    wait(timers.deadline()).await;
    assert_eq!(timers.pop_due(), Some(0));
}

#[tokio::test(start_paused = true)]
async fn deadline_heap_keeps_indices_correct_through_reordering_and_removal() {
    let mut timers = PeerTimers::default();
    timers.rebuild(128, |_| TimerSchedule::Parked);
    let allocation = timers.ordered.as_ptr();
    let mut expected = vec![None; 128];
    let mut state = 42_u64;
    for step in 0..5000 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let peer = ((state >> 32) as usize) % expected.len();
        let delay = if step % 3 == 0 {
            None
        } else {
            Some(Duration::from_millis(state % 10_000))
        };
        expected[peer] = delay.map(|delay| Instant::now() + delay);
        timers.update(peer, delay.into());
        assert_eq!(timers.deadline(), expected.iter().flatten().copied().min());
        assert_eq!(timers.ordered.as_ptr(), allocation);
        assert_eq!(timers.ordered.len(), expected.iter().flatten().count());
        for (position, &(_, peer)) in timers.ordered.iter().enumerate() {
            assert_eq!(timers.peers[peer], Some(position));
        }
    }
    tokio::time::advance(Duration::from_secs(20)).await;
    while let Some(peer) = timers.pop_due() {
        expected[peer] = None;
        assert_eq!(timers.deadline(), expected.iter().flatten().copied().min());
    }
    assert!(expected.iter().all(Option::is_none));
}

#[tokio::test(start_paused = true)]
async fn legacy_polling_is_not_starved_by_traffic_or_replayed_after_a_pause() {
    let mut timers = PeerTimers::default();
    timers.rebuild(1, |_| TimerSchedule::Polling);
    let first = timers.deadline();
    for _ in 0..249 {
        tokio::time::advance(Duration::from_millis(1)).await;
        timers.update(0, TimerSchedule::Polling);
    }
    assert_eq!(timers.deadline(), first);
    wait(first).await;
    assert_eq!(timers.pop_due(), Some(0));
    timers.update(0, TimerSchedule::Polling);
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(timers.pop_due(), Some(0));
    timers.update(0, TimerSchedule::Polling);
    assert_eq!(timers.pop_due(), None);
    let before = Instant::now();
    wait(timers.deadline()).await;
    assert_eq!(Instant::now() - before, Duration::from_millis(250));
}

#[tokio::test(start_paused = true)]
async fn immediate_deadlines_and_parked_waits_preserve_shutdown_selection() {
    let mut timers = PeerTimers::default();
    timers.rebuild(2, |_| TimerSchedule::Parked);
    timers.update(1, TimerSchedule::After(Duration::ZERO));
    wait(timers.deadline()).await;
    assert_eq!(timers.pop_due(), Some(1));
    let (sender, mut shutdown) = tokio::sync::watch::channel(false);
    sender.send_replace(true);
    tokio::select! {
        _ = wait(timers.deadline()) => panic!("parked timer woke"),
        changed = shutdown.changed() => assert!(changed.is_ok()),
    }
}
