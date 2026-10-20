use super::*;
use zero_platform_tokio::contract::lifecycle::{erase_lifecycle, ResourceLifecycle};

struct FailedStart(Arc<std::io::Error>);
impl ResourceLifecycle for FailedStart {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        Err(
            ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component)
                .with_source(self.0.clone()),
        )
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        Ok(CloseReport::default()) // This fixture acquired no native resource.
    }
}

#[tokio::test]
async fn real_start_failure_closes_prebound_native_and_retains_original_os_source_before_join() {
    let proxy = proxy();
    let config = proxy.engine().config();
    let mut lease = lease(&proxy, &config, 0);
    lease.bind_tcp().unwrap();
    let (operation, listener, installed) = lease.finish().unwrap().promote().unwrap();
    drop(operation);
    let address = listener.local_addr().unwrap();
    let owner = installed.owner();
    let empty = tempfile::tempdir().unwrap();
    let source = Arc::new(std::fs::File::open(empty.path().join("missing-file")).unwrap_err());
    assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
    let origin = proxy.protocols.allocate_resource_identity().unwrap();
    owner
        .install_resource(origin, erase_lifecycle(FailedStart(source.clone())))
        .unwrap();
    owner
        .spawn_driver(async move {
            let _view = listener;
            panic!("actual failed start must prevent polling user work");
        })
        .unwrap();
    owner.seal_listener_installation();
    let replacement = rebound(address).await;
    assert!(!owner.driver_finished.load(Ordering::Acquire));
    assert!(owner.terminal().is_none());
    let first = owner.failure.lock().unwrap().clone().unwrap();
    assert_eq!(first.component, Some(origin.component));
    assert_eq!(first.stage, Stage::Prepare);
    assert_eq!(first.reason, Reason::Io);
    assert!(Arc::ptr_eq(
        first
            .source()
            .unwrap()
            .downcast_ref::<Arc<std::io::Error>>()
            .unwrap(),
        &source
    ));
    assert!(owner.observe_driver().await.is_err());
    {
        let faults = owner.task_failures.lock().unwrap();
        let task = &faults[owner.driver.get().unwrap()];
        assert_eq!(task.class, TaskClass::Critical);
        assert!(Arc::ptr_eq(
            task.source
                .source()
                .unwrap()
                .downcast_ref::<SharedError>()
                .unwrap(),
            &first
        ));
    }
    let receipt = proxy
        .inbound_owners
        .confirm_shutdown("first", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.terminal().closed_io_receipts(), 1);
    assert_eq!(receipt.terminal().joined_tasks(), 1);
    assert!(Arc::ptr_eq(&receipt.terminal().failure().unwrap(), &first));
    proxy
        .inbound_owners
        .release_closed_reported(receipt, |_| Ok(()))
        .unwrap();
    assert_eq!(replacement.local_addr().unwrap(), address);
}
