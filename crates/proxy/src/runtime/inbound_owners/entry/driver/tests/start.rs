use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use zero_platform_tokio::contract::lifecycle::{erase_lifecycle, ResourceLifecycle};

struct FailedStart {
    source: Arc<std::io::Error>,
    native: std::net::SocketAddr,
    shutdown: Arc<AtomicBool>,
}
impl ResourceLifecycle for FailedStart {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        Err(
            ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component)
                .with_source(self.source.clone()),
        )
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        assert!(
            TcpListener::bind(self.native).await.is_err(),
            "the dependency must finish before the original native listener closes"
        );
        self.shutdown.store(true, Ordering::Release);
        Ok(CloseReport::default()) // This fixture acquired no native resource.
    }
}

#[tokio::test]
async fn real_start_failure_retains_original_os_source_and_closes_native_after_its_dependency() {
    let proxy = proxy();
    let config = proxy.engine().config();
    let mut lease = lease(&proxy, &config, 0);
    lease.bind_tcp().unwrap();
    let (operation, listener, installed) =
        crate::runtime::inbound_owners::bind::tests::promote_tcp(lease.finish().unwrap());
    drop(operation);
    let address = listener.local_addr().unwrap();
    let owner = installed.owner();
    let empty = tempfile::tempdir().unwrap();
    let source = Arc::new(std::fs::File::open(empty.path().join("missing-file")).unwrap_err());
    assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
    let origin = proxy.protocols.allocate_resource_identity().unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    owner
        .install_resource(
            origin,
            erase_lifecycle(FailedStart {
                source: source.clone(),
                native: address,
                shutdown: shutdown.clone(),
            }),
        )
        .unwrap();
    owner
        .spawn_driver(async move {
            let _view = listener;
            panic!("actual failed start must prevent polling user work");
        })
        .unwrap();
    owner.seal_listener_installation();
    // The failed-start dependency must confirm shutdown before native close.
    assert!(owner.observe_driver().await.is_err());
    assert!(shutdown.load(Ordering::Acquire));
    let replacement = rebound(address).await;
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
    {
        let faults = owner.task_failures.lock().unwrap();
        let task = &faults[owner.driver.get().unwrap()];
        assert_eq!(task.class, TaskClass::Critical);
        assert!(std::ptr::eq::<zero_platform_tokio::contract::ChannelError>(
            (task
                .source
                .source()
                .unwrap()
                .downcast_ref::<SharedError>()
                .unwrap())
            .as_ref(),
            (&first).as_ref()
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
    assert!(std::ptr::eq::<zero_platform_tokio::contract::ChannelError>(
        (&receipt.terminal().failure().unwrap()).as_ref(),
        (&first).as_ref()
    ));
    let released = proxy
        .inbound_owners
        .release_closed_reported(receipt, |terminal| {
            assert!(std::ptr::eq::<zero_platform_tokio::contract::ChannelError>(
                (&terminal.failure().unwrap()).as_ref(),
                (&first).as_ref()
            ));
            Ok(())
        })
        .unwrap_err();
    assert_eq!(
        crate::runtime::inbound_owners::released_fault_identity(&released),
        Some(owner.identity())
    );
    assert!(std::ptr::eq(
        crate::transport::channel_failure(&released).unwrap(),
        first.as_ref()
    ));
    assert!(proxy.inbound_owners.owner("first").is_none());
    assert_eq!(replacement.local_addr().unwrap(), address);
}
