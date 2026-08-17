use zero_platform_tokio::{EgressInterface, EgressInterfaceControl};
use zero_transport::OutboundDatagramSocketFactory;

#[test]
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
fn factory_reads_the_current_egress_for_each_new_socket() {
    let control = EgressInterfaceControl::default();
    let factory = OutboundDatagramSocketFactory::new(control.clone());
    let peer = "192.0.2.1:443".parse().unwrap();

    let selected = EgressInterface::new("not-a-real-interface", u32::MAX).unwrap();
    control.replace_for(false, Some(selected.clone()));
    assert_eq!(factory.egress_for(peer), Some(selected));

    control.replace_for(false, None);
    assert!(factory.egress_for(peer).is_none());
    let socket = factory
        .bind_std(peer)
        .expect("new socket must observe the reconciled automatic egress");
    assert!(socket.local_addr().unwrap().is_ipv4());
}
