use super::*;
use crate::packet_memory::{PacketMetadataArc, PacketReturnTotals};
use std::{io::Write, os::fd::AsRawFd, os::unix::net::UnixStream, sync::Arc};
fn backing(memory: &PacketMemoryView) -> [PacketMetadataArc<super::super::inline::Storage>; 2] {
    let layout = super::super::inline::Storage::layout().unwrap();
    std::array::from_fn(|_| {
        super::super::inline::Storage::from_prepaid(memory.reserve_driver_metadata(layout).unwrap())
            .unwrap()
    })
}
fn done() -> Finished {
    Finished {
        status: 0,
        child_pid: 0,
        operation_errno: 0,
        wait_errno: 0,
        close_errnos: [0; 10],
    }
}
#[test]
fn partial_command_header_uses_original_continuation_and_result_keeps_its_exact_prepaid_backing() {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let memory = read.memory_view().unwrap();
    let storage = backing(&memory);
    let charged = root.driver_metadata().occupied;
    let mut receiving = Receiving::new(51, &storage).unwrap();
    let (mut peer, source) = UnixStream::pair().unwrap();
    source.set_nonblocking(true).unwrap();
    let mut header = [0; wire::HEADER];
    header[0] = Kind::Stdout as u8;
    header[1..9].copy_from_slice(&51u64.to_be_bytes());
    header[9..].copy_from_slice(&9u32.to_be_bytes());
    peer.write_all(&header[..5]).unwrap();
    assert!(!receiving
        .progress(source.as_raw_fd(), &mut [0; 7], &memory, 65536)
        .unwrap());
    assert_eq!(receiving.read, 5);
    assert_eq!(root.driver_metadata().occupied, charged);
    peer.write_all(&header[5..]).unwrap();
    peer.write_all(b"continued").unwrap();
    wire::write(&mut peer, 51, Kind::Stderr, b"stderr").unwrap();
    wire::write(&mut peer, 51, Kind::Finish, &done().encode()).unwrap();
    assert!(receiving
        .progress(source.as_raw_fd(), &mut [0; 7], &memory, 65536)
        .unwrap());
    let output = receiving.take().unwrap();
    assert_eq!(&*output.stdout, b"continued");
    assert_eq!(&*output.stderr, b"stderr");
    assert!(
        Receiving::new(52, &storage).is_err(),
        "live output cannot be overwritten by next RPC"
    );
    drop(output);
    assert!(Receiving::new(52, &storage).is_ok());
    assert_eq!(root.driver_metadata().occupied, charged);
}
#[test]
fn metadata_pause_preserves_small_cleanup_output_on_the_same_already_paid_workspace() {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let memory = read.memory_view().unwrap();
    let storage = backing(&memory);
    let charged = root.driver_metadata().occupied;
    // Actual Root metadata admission is sealed, existing allocated bytes are
    // still owned. It cannot force this original cleanup to allocate elsewhere.
    let limit = root.driver_metadata().limit;
    let pause = root
        .pause_driver_metadata_for_limit(crate::packet_memory::PacketMetadataLayout::new(
            limit.allocations(),
            limit.bytes() - 1,
        ))
        .unwrap()
        .unwrap();
    assert_eq!(
        memory
            .reserve_driver_metadata(
                crate::packet_memory::PacketMetadataLayout::array::<u8>(1).unwrap()
            )
            .unwrap_err(),
        PacketMemoryError::Paused
    );
    let (mut peer, source) = UnixStream::pair().unwrap();
    wire::write(&mut peer, 61, Kind::Stderr, b"route already absent").unwrap();
    wire::write(&mut peer, 61, Kind::Finish, &done().encode()).unwrap();
    let mut receiving = Receiving::new(61, &storage).unwrap();
    assert!(receiving
        .progress(source.as_raw_fd(), &mut [0; 7], &memory, 65536)
        .unwrap());
    let output = receiving.take().unwrap();
    assert_eq!(&*output.stderr, b"route already absent");
    assert_eq!(root.driver_metadata().occupied, charged);
    drop(output);
    drop(pause);
}
#[test]
fn a_full_original_root_drains_segmented_output_to_its_terminal_fault_then_reuses_only_the_original_cleanup_backing(
) {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let memory = read.memory_view().unwrap();
    let storage = backing(&memory);
    let charged = root.driver_metadata().occupied;
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        crate::packet_memory::PacketMetadataLayout::new(charged.allocations(), charged.bytes()),
    )
    .unwrap();
    let (mut peer, source) = UnixStream::pair().unwrap();
    source.set_nonblocking(true).unwrap();
    let mut receiving = Receiving::new(71, &storage).unwrap();
    for _ in 0..8 {
        wire::write(&mut peer, 71, Kind::Stdout, &[23; 4096]).unwrap();
        assert!(!receiving
            .progress(source.as_raw_fd(), &mut [0; 4096], &memory, 65536)
            .unwrap());
    }
    wire::write(
        &mut peer,
        71,
        Kind::Stdout,
        b"must be drained without another allocation",
    )
    .unwrap();
    let mut terminal = done();
    terminal.operation_errno = libc::EIO;
    terminal.close_errnos[3] = libc::EBADF;
    wire::write(&mut peer, 71, Kind::Finish, &terminal.encode()).unwrap();
    assert!(receiving
        .progress(source.as_raw_fd(), &mut [0; 7], &memory, 65536)
        .unwrap());
    let error = receiving.take().unwrap_err();
    let failure = error
        .get_ref()
        .unwrap()
        .downcast_ref::<RemoteFailure>()
        .unwrap();
    assert_eq!(failure.storage, Some(PacketMemoryError::Busy));
    assert_eq!(failure.remote.operation_errno, libc::EIO);
    assert_eq!(failure.remote.close_errnos[3], libc::EBADF);
    assert_eq!(root.driver_metadata().occupied, charged);
    let mut cleanup = Receiving::new(72, &storage).unwrap();
    wire::write(&mut peer, 72, Kind::Stderr, b"original cleanup result").unwrap();
    wire::write(&mut peer, 72, Kind::Finish, &done().encode()).unwrap();
    assert!(cleanup
        .progress(source.as_raw_fd(), &mut [0; 7], &memory, 65536)
        .unwrap());
    let output = cleanup.take().unwrap();
    assert_eq!(&*output.stderr, b"original cleanup result");
    assert_eq!(root.driver_metadata().occupied, charged);
}
#[test]
fn the_last_result_and_its_actual_weak_header_keep_the_original_source_backing_charge_after_the_rpc_owner_returns(
) {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let memory = read.memory_view().unwrap();
    let baseline = root.driver_metadata().occupied;
    let storage = backing(&memory);
    let layout = super::super::inline::Storage::layout().unwrap();
    let weak = PacketMetadataArc::downgrade(&storage[0]);
    let (mut peer, source) = UnixStream::pair().unwrap();
    wire::write(&mut peer, 81, Kind::Stdout, b"retained result").unwrap();
    wire::write(&mut peer, 81, Kind::Finish, &done().encode()).unwrap();
    let mut receiving = Receiving::new(81, &storage).unwrap();
    assert!(receiving
        .progress(source.as_raw_fd(), &mut [0; 7], &memory, 65536)
        .unwrap());
    let output = receiving.take().unwrap();
    drop(storage);
    assert_eq!(&*output.stdout, b"retained result");
    assert_eq!(
        root.driver_metadata().occupied,
        baseline
            .checked_add(layout)
            .unwrap()
            .checked_add(layout)
            .unwrap()
    );
    drop(output);
    assert!(weak.upgrade().is_none());
    assert_eq!(
        root.driver_metadata().occupied,
        baseline.checked_add(layout).unwrap()
    );
    drop(weak);
    assert_eq!(root.driver_metadata().occupied, baseline);
}
