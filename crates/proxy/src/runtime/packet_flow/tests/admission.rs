use super::support::{target, Fixture};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn common_converter_selects_a_supported_resolved_target_and_fences_generations() {
    let fixture = Fixture::new(1420);
    let unsupported = "[fd00::2]:443".parse().unwrap();
    let (selected, _) = fixture.adapter.select(&[unsupported, target()], 0).unwrap();
    assert_eq!(selected, target());
    assert!(fixture.adapter.select(&[unsupported], 0).is_err());
    assert!(fixture.adapter.select(&[], 0).is_err());
    fixture.generation.store(1, Ordering::Release);
    assert!(fixture.adapter.select(&[target()], 0).is_err());
    assert!(fixture.adapter.select(&[target()], 1).is_ok());
    fixture.device.close_now();
    assert!(fixture.adapter.select(&[target()], 1).is_err());
}

#[tokio::test]
async fn runtime_binding_failure_never_falls_through_to_another_target() {
    use crate::runtime::packet_flow::{
        PacketFlowAdapter, PacketFlowBindError, PacketFlowBinding, PacketFlowSource,
    };
    use std::{
        net::IpAddr,
        sync::{atomic::AtomicUsize, Arc},
    };
    struct Source(Arc<AtomicUsize>);
    impl PacketFlowSource for Source {
        fn bind(&self, _: IpAddr, _: u64) -> Result<PacketFlowBinding, PacketFlowBindError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Err(PacketFlowBindError::Failed(zero_engine::EngineError::Io(
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "removed resource"),
            )))
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let adapter = PacketFlowAdapter::new("packet-only".into(), Arc::new(Source(calls.clone())));
    assert!(adapter.select(&[target(), target()], 0).is_err());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn mismatched_local_address_family_is_rejected_before_packet_io() {
    use crate::runtime::packet_flow::{
        PacketFlowAdapter, PacketFlowBindError, PacketFlowBinding, PacketFlowEndpoint,
        PacketFlowSource,
    };
    use std::{net::IpAddr, sync::Arc};
    struct Source(Arc<dyn PacketFlowEndpoint>);
    impl PacketFlowSource for Source {
        fn bind(&self, _: IpAddr, _: u64) -> Result<PacketFlowBinding, PacketFlowBindError> {
            Ok(PacketFlowBinding {
                local_ip: "fd00::1".parse().unwrap(),
                peer_id: None,
                endpoint: self.0.clone(),
            })
        }
    }
    let mut fixture = Fixture::new(1420);
    let adapter = PacketFlowAdapter::new(
        "packet-only".into(),
        Arc::new(Source(fixture.device.clone())),
    );
    assert!(adapter.select(&[target()], 0).is_err());
    assert!(fixture.packets.try_recv().is_err());
}
