use super::*;
use zero_platform_tokio::contract::scope::TaskRegistrar;
struct Denied;
fn denied<T: Send + 'static>() -> BoxFuture<'static, Result<T, BuildError>> {
    Box::pin(async {
        Err(ChannelError::new(
            Stage::Prepare,
            Reason::Unsupported,
            Impact::Component,
        ))
    })
}
impl ResourceAccess for Denied {
    fn read_file(
        &self,
        _: std::path::PathBuf,
        _: usize,
    ) -> BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        denied()
    }
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, BuildError>> {
        denied()
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedDatagramListener, BuildError>,
    > {
        denied()
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        denied()
    }
}
pub(super) fn instance(component: u64, generation: u64) -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(component),
        generation: ResourceGeneration(generation),
    }
}
pub(super) fn owner(
    options: Arc<TotalsOptions>,
    instance: ComponentInstanceId,
    tasks: TaskRegistrar,
) -> ComponentInstance {
    TotalsFactory
        .build(
            options,
            BuildContext {
                component: instance.component,
                generation: instance.generation,
                dependencies: Arc::from([]),
                roles: Default::default(),
                resources: Arc::new(Denied),
                tasks,
                cancel: CancellationToken::new(),
                background_carrier: None,
            },
        )
        .unwrap()
}
pub(super) fn scope() -> TaskScope {
    let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), None);
    scope.enable_registration().unwrap();
    scope
}
pub(super) async fn server() -> (
    std::net::SocketAddr,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let cancel = stop.clone();
    let task = tokio::spawn(async move {
        let accepted = tokio::select! {biased;_=cancel.cancelled()=>return,accepted=listener.accept()=>accepted.unwrap()};
        let incoming = crate::grpc::accept_grpc_connection(
            accepted.0,
            &crate::profile::OwnedGrpcProfile {
                service_names: vec!["owned".into()],
                ..Default::default()
            },
        )
        .unwrap();
        let mut workers = tokio::task::JoinSet::new();
        loop {
            let stream = tokio::select! {biased;_=cancel.cancelled()=>break,stream=incoming.accept()=>stream};
            let Some(mut stream) = stream else {
                break;
            };
            let cancel = cancel.clone();
            workers.spawn(async move {
                tokio::select! {biased;_=cancel.cancelled()=>{},_=async {
                    let mut byte=[0;1];while let Ok(1)=stream.read(&mut byte).await {stream.write_all(&byte).await.unwrap();stream.flush().await.unwrap();}
                }=>{}}
            });
        }
        incoming.close();
        while let Some(result) = workers.join_next().await {
            result.unwrap();
        }
    });
    (peer, stop, task)
}
pub(super) async fn echo(stream: &mut NativeGrpcStream) {
    stream.write_all(b"x").await.unwrap();
    stream.flush().await.unwrap();
    let mut byte = [0; 1];
    stream.read_exact(&mut byte).await.unwrap();
    assert_eq!(byte, *b"x");
}
