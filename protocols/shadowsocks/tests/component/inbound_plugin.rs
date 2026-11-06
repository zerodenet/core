//! Same actor and original process tree; library direct-child seam stays explicit.
use super::*;
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
};
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    component::{InboundListenerEndpoints, ProcessRequest},
    packet::BoxPacketSession,
    scope::{SpawnError, TaskClass, TaskScope},
    BoxFuture,
};
#[derive(Default)]
struct Grant {
    starts: AtomicUsize,
    #[cfg(target_os = "linux")]
    originals: std::sync::Mutex<Vec<Arc<zero_platform_tokio::owned_process::OwnedProcess>>>,
}
fn denied<T: Send + 'static>() -> BoxFuture<'static, Result<T, ChannelError>> {
    Box::pin(async {
        Err(error(
            Stage::Prepare,
            Reason::Unsupported,
            "undeclared fixture resource",
        ))
    })
}
impl ResourceAccess for Grant {
    fn inbound_endpoints(&self) -> Result<InboundListenerEndpoints, ChannelError> {
        let endpoint = "127.0.0.1:9999".parse().unwrap();
        Ok(InboundListenerEndpoints {
            external: endpoint,
            stream: endpoint,
            datagram: endpoint,
        })
    }
    #[cfg(target_os = "linux")]
    fn prepare_owned_process(
        &self,
        request: ProcessRequest,
        tasks: &TaskRegistrar,
    ) -> Result<Arc<zero_platform_tokio::owned_process::OwnedProcess>, ChannelError> {
        assert_eq!(request.program, std::path::Path::new("python3"));
        assert_eq!(self.starts.fetch_add(1, Ordering::SeqCst), 0);
        let original = zero_platform_tokio::owned_process::OwnedProcess::prepare(request, tasks)
            .map_err(|source| io(Stage::Prepare, source))?;
        self.originals.lock().unwrap().push(original.clone());
        Ok(original)
    }
    fn spawn_process(&self, request: ProcessRequest) -> Result<Child, ChannelError> {
        assert_eq!(request.program, std::path::Path::new("python3"));
        assert_eq!(self.starts.fetch_add(1, Ordering::SeqCst), 0);
        request
            .command()
            .spawn()
            .map_err(|source| io(Stage::Prepare, source))
    }
    fn read_file(
        &self,
        _: std::path::PathBuf,
        _: usize,
    ) -> BoxFuture<'_, Result<Vec<u8>, ChannelError>> {
        denied()
    }
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, ChannelError>> {
        denied()
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedDatagramListener, ChannelError>,
    > {
        denied()
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, ChannelError>> {
        denied()
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn owner(scope: &TaskScope, grant: Arc<Grant>) -> Owner {
    Owner::new(
        PluginConfig {
            command: "python3".into(),
            args: vec!["-c".into(), "import time; time.sleep(60)".into()],
            options: None,
            mode: crate::validation::PluginMode::UdpOnly,
        },
        grant,
        scope.registrar(),
        CancellationToken::new(),
    )
}
#[tokio::test]
async fn inbound_actor_reserves_its_real_task_before_process_start() {
    let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
    let grant = Arc::new(Grant::default());
    let mut owner = owner(&scope, grant.clone());
    assert_eq!(
        owner.start(deadline()).await.unwrap_err().reason,
        Reason::Capacity
    );
    assert_eq!(grant.starts.load(Ordering::SeqCst), 0);
    assert!(owner.shared.state.lock().unwrap().child.is_none());
    assert!(owner.shutdown(deadline()).await.unwrap().is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}
#[tokio::test]
async fn inbound_single_actor_matches_scope_join_and_its_actual_native_backend_close() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let grant = Arc::new(Grant::default());
        let mut owner = owner(&scope, grant.clone());
        owner.start(deadline()).await.unwrap();
        let actor = owner.actor.clone().unwrap();
        assert!(!actor.is_joined());
        assert!(actor
            .scope_identity()
            .same_scope(&scope.registrar().scope_identity()));
        assert_eq!(grant.starts.load(Ordering::SeqCst), 1);
        assert!(owner.shared.state.lock().unwrap().child.is_some());
        assert!(matches!(
            scope
                .registrar()
                .spawn(TaskClass::Critical, async { Ok(()) }),
            Err(SpawnError::Capacity)
        ));
        let (native, tasks) = tokio::join!(owner.shutdown(deadline()), scope.shutdown(deadline()));
        assert!(native.unwrap().is_complete());
        assert!(tasks.is_complete());
        assert_eq!(tasks.exited_tasks.len(), 1);
        let joined = actor.wait().await;
        assert!(actor.matches_joined(&joined));
        assert!(owner.shared.state.lock().unwrap().exited.is_some());
        #[cfg(target_os = "linux")]
        {
            let originals = grant.originals.lock().unwrap();
            assert_eq!(originals.len(), 1);
            let receipt = originals[0]
                .closed()
                .expect("actual tree/parent-fd close after actor join");
            assert!(receipt.matches(&originals[0]) && receipt.had_native_acquisition());
        }

        assert!(owner.start(deadline()).await.is_err());
        assert_eq!(grant.starts.load(Ordering::SeqCst), 1);
    })
    .await
    .unwrap();
}
