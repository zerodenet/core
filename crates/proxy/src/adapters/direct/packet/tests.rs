use super::*;
use std::os::fd::AsRawFd;

fn config(fd: i32) -> zero_config::DirectPacketDeviceConfig {
    zero_config::DirectPacketDeviceConfig {
        backend: zero_config::DirectPacketDeviceBackend::Descriptor,
        fd: Some(fd),
        interface: "not-a-tun".into(),
        router_addresses: vec![],
    }
}

#[tokio::test]
async fn failed_host_duplicate_keeps_the_same_original_error_until_explicit_cleanup() {
    let adapter = DirectAdapter::default();
    let error = match adapter.prepare_host_device(config(i32::MAX), 1500) {
        Ok(_) => panic!("invalid host descriptor"),
        Err(error) => error,
    };
    let zero_engine::EngineError::Io(error) = error else {
        panic!("actual acquisition failure")
    };
    let source = error
        .get_ref()
        .unwrap()
        .downcast_ref::<zero_tun::TunAcquisitionError>()
        .unwrap();
    let owner = adapter.packet_original.lock().unwrap().clone().unwrap();
    assert!(Arc::ptr_eq(
        &source.source,
        &owner.acquisition_failure().unwrap()
    ));
    assert!(matches!(
        owner.outcome(),
        zero_tun::TunAcquisitionOutcome::KnownNoNative
    ));
    assert!(adapter.packet.lock().unwrap().is_none());
    adapter.close_devices().await.unwrap();
    assert!(adapter.packet_original.lock().unwrap().is_none());
    assert!(Arc::ptr_eq(
        &source.source,
        &owner.acquisition_failure().unwrap()
    ));
}

#[tokio::test]
async fn failed_host_validation_retains_matching_duplicate_close_and_preserves_borrowed_host() {
    let (host, peer) = std::os::unix::net::UnixDatagram::pair().unwrap();
    let adapter = DirectAdapter::default();
    let error = match adapter.prepare_host_device(config(host.as_raw_fd()), 1500) {
        Ok(_) => panic!("ordinary datagram descriptor is not TUN"),
        Err(error) => error,
    };
    let zero_engine::EngineError::Io(error) = error else {
        panic!("actual validation failure")
    };
    let source = error
        .get_ref()
        .unwrap()
        .downcast_ref::<zero_tun::TunAcquisitionError>()
        .unwrap();
    let zero_tun::TunAcquisitionOutcome::Closed(receipt) = &source.outcome else {
        panic!("the successfully acquired duplicate requires an actual close receipt")
    };
    let owner = adapter.packet_original.lock().unwrap().clone().unwrap();
    let retained = owner.close().unwrap();
    assert!(Arc::ptr_eq(receipt, &retained));
    assert!(receipt.identity().same_owner(retained.identity()));
    adapter.close_devices().await.unwrap();
    assert!(adapter.packet_original.lock().unwrap().is_none());
    host.send(b"borrowed host remains live").unwrap();
    let mut bytes = [0; 64];
    let size = peer.recv(&mut bytes).unwrap();
    assert_eq!(&bytes[..size], b"borrowed host remains live");
}
