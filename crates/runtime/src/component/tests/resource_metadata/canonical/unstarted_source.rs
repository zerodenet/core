//! Reject actual source construction after the original native grant transferred.
use super::*;
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{component::ResourceAccess, packet::BoxPacketSession, BoxFuture},
    ControlledTcpListener,
};
struct CancelAfterGrant {
    slot: Arc<ComponentSlot>,
    issued: Arc<Issued>,
    native: Arc<HostResources>,
    original: Mutex<Option<GrantedMetadataListener>>,
}
impl ResourceAccess for CancelAfterGrant {
    fn listen_metadata_datagram(
        &self,
        request: MetadataDatagramRequest,
    ) -> BoxFuture<'_, Result<GrantedMetadataListener, ChannelError>> {
        Box::pin(async move {
            assert_eq!(request.ordinal, 0);
            let grant = self
                .original
                .lock()
                .unwrap()
                .take()
                .expect("one actual original grant");
            self.slot.revoke(); // Close the real original host registration, not a timer/probe.
            assert!(!grant.scope.registration_open());
            Ok(grant)
        })
    }
    fn confirm_metadata_users(
        &self,
        ordinal: usize,
        receipt:&zero_platform_tokio::controlled_datagram::metadata::users::MetadataUsersClosedReceipt,
        tasks: &TaskRegistrar,
    ) -> Result<(), ChannelError> {
        self.native
            .confirm_metadata_users(ordinal, receipt, tasks)?;
        self.issued
            .confirmation
            .lock()
            .unwrap()
            .get_or_insert(receipt.clone());
        Ok(())
    }
    fn read_file(
        &self,
        path: std::path::PathBuf,
        max: usize,
    ) -> BoxFuture<'_, Result<Vec<u8>, ChannelError>> {
        self.native.read_file(path, max)
    }
    fn listen_tcp(
        &self,
        address: Destination,
    ) -> BoxFuture<'_, Result<ControlledTcpListener, ChannelError>> {
        self.native.listen_tcp(address)
    }
    fn listen_datagram(
        &self,
        address: Destination,
    ) -> BoxFuture<'_, Result<GrantedDatagramListener, ChannelError>> {
        self.native.listen_datagram(address)
    }
    fn open_device(&self, key: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, ChannelError>> {
        self.native.open_device(key)
    }
}
#[tokio::test]
async fn unstarted_original_rx_return_after_source_start_rejection_closes_actual_provider() {
    let (fixture, grant, tasks) = CanonicalMetadata::start(loopback(), limits(), n(1)).await;
    let native = fixture
        .slot
        .live
        .get()
        .unwrap()
        .as_ref()
        .unwrap()
        .resources
        .clone();
    let resources = Arc::new(CancelAfterGrant {
        slot: fixture.slot.clone(),
        issued: fixture.issued.clone(),
        native,
        original: Mutex::new(Some(grant)),
    });
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let server = quinn::ServerConfig::with_single_cert(
        vec![certificate.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der()).into(),
    )
    .unwrap();
    let (mut owner, source) = zero_transport::quic::source::source(
        request(loopback()),
        server,
        Vec::new(),
        zero_transport::quic::owned::OwnedEndpointLimits {
            drivers: n(1),
            users: n(1),
        },
        resources,
        tasks,
        CancellationToken::new(),
        |_| -> zero_platform_tokio::contract::inbound::PendingInbound {
            panic!("rejected source must not deliver a fabricated handshake")
        },
    );
    assert!(owner.start(deadline()).await.is_err());
    drop(source);
    assert!(owner.shutdown(deadline()).await.unwrap().is_complete());
    let report = fixture.shutdown().await;
    assert!(report.is_complete(), "{report:?}");
    assert!(
        report.exited_tasks.is_empty(),
        "no Quinn task was started: {report:?}"
    );
    assert!(
        report
            .closed_io
            .iter()
            .any(|line| line.contains("metadata declaration 0 main")),
        "only the actual original provider closes native IO: {report:?}"
    );
}
