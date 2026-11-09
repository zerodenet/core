use super::*;
use crate::packet_memory::PacketReturnTotals;
use std::sync::{Arc, Barrier};
#[test]
fn original_epoch_io_unlock_notification_is_retained_when_initial_observer_arms_after_publication()
{
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let owner = MacosHelperOwner::prepare(&read.memory_view().unwrap(), 65536, true, rpc::CAPACITY)
        .unwrap();
    let saw_busy = Arc::new(Barrier::new(2));
    let published = Arc::new(Barrier::new(2));
    let _progress = EpochProgress(&owner.0);
    let mut epoch = owner.0.epochs[0].lock().unwrap();
    let fd = unsafe { libc::open(c"/dev/null".as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
    assert!(fd >= 0);
    epoch.local_file = Some(Original::new(fd, epoch.id));
    let actual = owner.clone();
    let observer_busy = saw_busy.clone();
    let observer_publish = published.clone();
    let observer = std::thread::spawn(move || {
        assert!(actual.0.epochs[0].try_lock().is_err());
        observer_busy.wait();
        observer_publish.wait(); // actual notification deliberately BEFORE arm
        actual.0.wait_signal(true);
        actual.0.epochs[0].lock().unwrap().require_closed().unwrap();
    });
    saw_busy.wait();
    assert_eq!(
        unsafe { libc::read(fd, [0u8; 1].as_mut_ptr().cast(), 1) },
        0
    );
    epoch.local_file.as_mut().unwrap().close_once().unwrap();
    drop(epoch);
    drop(_progress); // production post-unlock publisher, not a test notify
    published.wait();
    observer.join().unwrap();
}
#[test]
fn one_actual_source_notification_survives_before_arm_for_each_existing_duty_independently() {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let owner = MacosHelperOwner::prepare(&read.memory_view().unwrap(), 65536, true, rpc::CAPACITY)
        .unwrap();
    owner.stop();
    owner.0.wait_signal(true);
    owner.0.wait_signal(false); // initial consumption cannot steal route's wake
    let pending = owner.0.wait.lock().unwrap();
    assert_eq!(pending.duties, [false, false]);
}
