//! Root CA preparation uses the existing configured original-file byte budget.
use super::*;

#[tokio::test]
async fn hysteria2_actual_ca_file_budget_rejection_closes_original_before_subsequent_preparation() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let server = Server::new().await;
        let ca = server._material.path().join("cert.pem");
        let bytes = std::fs::read(&ca).unwrap();
        assert!(bytes.len() > 1);
        let mut config = reload::config(&server, Some(&ca));
        config.runtime.file_read_max_bytes = Some(1);
        let failure = match Proxy::new(config.clone()) {
            Err(error) => error,
            Ok(_) => panic!("actual configured CA byte budget was bypassed"),
        };
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&failure);
        let mut original_limit = false;
        while let Some(error) = source {
            if let Some(error) = error.downcast_ref::<std::io::Error>() {
                original_limit |= error.kind() == std::io::ErrorKind::InvalidData
                    && error
                        .to_string()
                        .contains("exceeds its declared byte limit");
            }
            source = error.source();
        }
        assert!(
            original_limit,
            "the original bounded File read error must survive: {failure:?}"
        );
        assert_eq!(server.authenticated.load(Ordering::SeqCst), 0);
        // A failed read returns only after its same original File close matches.
        // Any retained startup debt would reject this independent final check
        // and the next actual configuration preparation below.
        let observed = zero_runtime::configuration::ConfigurationSource::default();
        observed.confirm_original_io_closed().unwrap();
        observed.ensure_available().unwrap();
        config.runtime.file_read_max_bytes = Some(bytes.len());
        let proxy = Proxy::new(config).unwrap();
        start(&proxy).await;
        let stream = connect(&proxy, "hy").await;
        assert_eq!(server.authenticated.load(Ordering::SeqCst), 1);
        drop(stream);
        proxy.protocols.shutdown_components(end()).await.unwrap();
        server.close().await;
    })
    .await
    .unwrap();
}
