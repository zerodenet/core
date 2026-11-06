//! Ordinary assembly must keep the original native acquisition classification.
use super::*;
use std::{error::Error, time::Duration};
fn configuration(fd: i32) -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse(&serde_json::json!({
        "runtime":{"network":{"direct_packet_device":{"fd":fd,"interface":"not-a-tun","router_addresses":["10.66.0.1"]}}},
        "route":{"final":{"type":"direct"},"final_mode":"packet"}
    }).to_string()).unwrap()
}
fn acquisition(mut source: &(dyn Error + 'static)) -> Option<&zero_tun::TunAcquisitionError> {
    for _ in 0..64 {
        if let Some(original) = source.downcast_ref::<zero_tun::TunAcquisitionError>() {
            return Some(original);
        }
        source = if let Some(io) = source.downcast_ref::<std::io::Error>() {
            io.get_ref()
                .map(|source| source as &(dyn Error + 'static))
                .or_else(|| io.source())?
        } else {
            source.source()?
        };
    }
    None
}
async fn fail(fd: i32) -> zero_engine::EngineError {
    let proxy = crate::Proxy::new(configuration(fd)).unwrap();
    assert!(!*proxy.orchestration_ready.borrow());
    let source = tokio::time::timeout(
        Duration::from_secs(5),
        proxy.run_until(std::future::pending()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        !*proxy.orchestration_ready.borrow(),
        "failed native original cannot publish Ready"
    );
    if let Some(plan) = proxy.protocols.current_components(&proxy.engine().config()) {
        assert!(
            !plan.has_packet_provider(plan.direct(None).unwrap().role_id()),
            "failed native duplicate cannot authorize Packet"
        );
    }
    source
}
#[tokio::test]
async fn ordinary_direct_invalid_original_descriptor_reports_known_no_native_after_actual_cleanup()
{
    let source = fail(i32::MAX).await;
    let original = acquisition(&source)
        .unwrap_or_else(|| panic!("actual original acquisition error missing: {source:?}"));
    assert!(matches!(
        original.outcome,
        zero_tun::TunAcquisitionOutcome::KnownNoNative
    ));
    assert_eq!(original.source.raw_os_error(), Some(9));
}
#[tokio::test]
async fn ordinary_direct_failed_tun_validation_closes_only_duplicate_and_keeps_real_host_original()
{
    use std::os::fd::AsRawFd;
    let (host, remote) = std::os::unix::net::UnixDatagram::pair().unwrap();
    let source = fail(host.as_raw_fd()).await;
    let original = acquisition(&source)
        .unwrap_or_else(|| panic!("actual original validation error missing: {source:?}"));
    let zero_tun::TunAcquisitionOutcome::Closed(receipt) = &original.outcome else {
        panic!("acquired duplicate requires matching real native close: {original:?}")
    };
    assert!(
        receipt.identity().closed().is_some(),
        "same actual SDK close cell, never EOF/Drop proof"
    );
    host.send(b"host original is still owned").unwrap();
    let mut bytes = [0; 64];
    let size = remote.recv(&mut bytes).unwrap();
    assert_eq!(&bytes[..size], b"host original is still owned");
}
