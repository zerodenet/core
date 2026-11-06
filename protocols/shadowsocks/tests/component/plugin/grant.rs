use super::*;
#[derive(Default)]
pub(super) struct Grant {
    pub(super) starts: AtomicUsize,
}
fn denied<T: Send + 'static>() -> BoxFuture<'static, Result<T, BuildError>> {
    Box::pin(async {
        Err(ChannelError::new(
            Stage::Prepare,
            Reason::Unsupported,
            Impact::Component,
        ))
    })
}
impl ResourceAccess for Grant {
    fn spawn_process(
        &self,
        request: zero_platform_tokio::contract::component::ProcessRequest,
    ) -> Result<tokio::process::Child, BuildError> {
        if request.program != std::path::Path::new("python3") {
            return Err(failure(
                ComponentId(770),
                Stage::Prepare,
                Reason::InvalidConfig,
                "undeclared test executable",
            ));
        }
        self.starts
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| {
                failure(
                    ComponentId(770),
                    Stage::Prepare,
                    Reason::Capacity,
                    "single actual Child allowance exhausted",
                )
            })?;
        request.command().spawn().map_err(|source| {
            failure(
                ComponentId(770),
                Stage::Prepare,
                Reason::Io,
                "actual child launch failed",
            )
            .with_source(source)
        })
    }
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
