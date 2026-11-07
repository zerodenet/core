//! An earlier actual main fault must not hide its provider's cleanup receiver.
use super::*;
use zero_platform_tokio::contract::{
    lifecycle::{erase_lifecycle, ResourceLifecycle},
    CleanupFailureReceipt,
};
struct FailedStartWithCleanup {
    start: Arc<std::io::Error>,
    close: Arc<std::io::Error>,
    receipt: CleanupFailureReceipt,
}
impl ResourceLifecycle for FailedStartWithCleanup {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        Err(
            ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component)
                .with_source(self.start.clone()),
        )
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        if !self.receipt.is_acknowledged() {
            return Err(
                ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                    .with_source(self.close.clone())
                    .with_cleanup_failure_receipt(self.receipt.clone()),
            );
        }
        // The fixture itself acquired no IO. The separate real TCP controller
        // below must still close its original and consume its actual Scope.
        Ok(CloseReport::default())
    }
}
#[tokio::test]
async fn earlier_main_fault_preserves_same_cleanup_payload_and_ack_before_original_recovery() {
    timeout(Duration::from_secs(5), async {
        let proxy = proxy();
        let config = proxy.engine().config();
        let mut lease = lease(&proxy, &config, 0);
        lease.bind_tcp().unwrap();
        let (operation, listener, installed) =
            crate::runtime::inbound_owners::bind::tests::promote_tcp(lease.finish().unwrap());
        drop(operation);
        let owner = installed.owner();
        let missing = tempfile::tempdir().unwrap();
        let start = Arc::new(std::fs::File::open(missing.path().join("start")).unwrap_err());
        let close = Arc::new(std::fs::File::open(missing.path().join("cleanup")).unwrap_err());
        let receipt = CleanupFailureReceipt::new();
        assert!(receipt.bind_payload(close.clone()));
        let identity = proxy.protocols.allocate_resource_identity().unwrap();
        owner
            .install_resource(
                identity,
                erase_lifecycle(FailedStartWithCleanup {
                    start: start.clone(),
                    close: close.clone(),
                    receipt: receipt.clone(),
                }),
            )
            .unwrap();
        owner
            .spawn_driver(async move {
                let _original_view = listener;
                panic!("failed start must prevent polling business work");
            })
            .unwrap();
        owner.seal_listener_installation();
        assert!(owner.observe_driver().await.is_err());
        let first = owner.failure.lock().unwrap().clone().unwrap();
        assert_eq!(first.stage, Stage::Prepare);
        assert!(Arc::ptr_eq(
            first
                .source()
                .unwrap()
                .downcast_ref::<Arc<std::io::Error>>()
                .unwrap(),
            &start
        ));
        let original_cleanup = {
            let resources = owner.resources.lock().unwrap();
            let resource = resources
                .iter()
                .find(|resource| resource.identity == identity)
                .unwrap();
            assert!(!resource.stopped.load(Ordering::Acquire));
            let fault = resource.cleanup_fault.get().unwrap().clone();
            assert!(fault
                .cleanup_failure_receipt()
                .unwrap()
                .same_receipt(&receipt));
            assert!(Arc::ptr_eq(
                fault
                    .source()
                    .unwrap()
                    .downcast_ref::<Arc<std::io::Error>>()
                    .unwrap(),
                &close
            ));
            fault
        };
        assert!(
            receipt.is_acknowledged(),
            "actual Resource retained the same fault before ACK"
        );
        assert!(
            owner.terminal().is_none(),
            "fault delivery cannot close the real TCP original"
        );
        let closed = proxy
            .inbound_owners
            .confirm_shutdown("first", Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(closed.terminal().closed_io_receipts(), 1);
        assert_eq!(closed.terminal().joined_tasks(), 1);
        assert!(Arc::ptr_eq(&closed.terminal().failure().unwrap(), &first));
        assert!(closed
            .terminal()
            .native_bound_failures()
            .iter()
            .any(|fault| Arc::ptr_eq(fault, &original_cleanup)));
        assert!(
            proxy
                .inbound_owners
                .release_closed_reported(closed, |_| Ok(()))
                .is_err(),
            "the actual original main fault remains reported"
        );
    })
    .await
    .unwrap();
}
