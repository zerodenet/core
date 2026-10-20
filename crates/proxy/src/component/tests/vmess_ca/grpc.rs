//! VMess gRPC+CA remains an explicit native ownership gap, not a success case.
use super::fixtures::*;
use std::sync::{atomic::Ordering, Arc};
use zero_transport::profile::OwnedGrpcProfile;
#[tokio::test]
async fn native_vmess_ca_grpc_rejects_before_file_owner_read_or_outbound_acquisition() {
    let directory = Directory::new();
    let path = directory.0.join("identity.pem");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut tls = tls_profile();
    tls.alpn = vec!["h2".into()];
    let grpc = OwnedGrpcProfile {
        service_names: vec!["ca.vmess/Tun".into()],
        authority: Some("native.example".into()),
        ..Default::default()
    };
    let observations = Arc::new(Observations::default());
    // This is the same pipeline whose TLS/WS positive cases count the actual
    // host read_file invocation and prove authenticated I/O after removing CA.
    let failure = super::preparation::prepare(
        address,
        &tls,
        None,
        Some(&grpc),
        &directory.0,
        4096,
        observations.clone(),
    )
    .err()
    .unwrap();
    assert!(
        matches!(
            failure,
            zero_core::Error::Unsupported(
                "VMess gRPC requires its carrier and registered driver owner"
            )
        ),
        "retain the original typed cold error: {failure:?}"
    );
    assert!(
        !path.exists(),
        "an absent CA cannot turn this into a later file error"
    );
    assert_eq!(observations.file_owners.load(Ordering::Relaxed), 0);
    assert_eq!(observations.file_reads.load(Ordering::Relaxed), 0);
    assert_eq!(observations.dials.load(Ordering::Relaxed), 0);
    assert_eq!(observations.boundaries.load(Ordering::Relaxed), 0);
    assert_eq!(observations.outer.load(Ordering::Relaxed), 0);
    assert!(observations.facts.lock().unwrap().is_empty());
    assert!(observations.protocol.lock().unwrap().is_none());
}
