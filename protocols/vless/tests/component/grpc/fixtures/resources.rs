use super::*;
/// Actual test allocator: exact canonical declaration, instance and real Scope.
/// It grants no socket, I/O, driver or close receipt; NativeCarrierOpener still
/// owns the original connect and the test consumes its real controller later.
struct Granted {
    declaration: zero_platform_tokio::contract::component::ResourceDeclaration,
    instance: ComponentInstanceId,
    scope: zero_platform_tokio::contract::scope::TaskScopeIdentity,
    cancel: Arc<CancellationToken>,
}
pub(super) fn granted(
    options: &zero_transport::grpc::NativePoolOptions,
    instance: ComponentInstanceId,
    tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    cancel: &CancellationToken,
) -> Arc<dyn ResourceAccess> {
    Arc::new(Granted {
        declaration: PoolFactory.describe(options).resources[0].clone(),
        instance,
        scope: tasks.scope_identity(),
        cancel: Arc::new(cancel.clone()),
    })
}
impl ResourceAccess for Granted {
    fn register_controlled_carrier(
        &self,
        ordinal: usize,
        registration: zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    ) -> Result<zero_platform_tokio::controlled_carrier::CarrierOwnerBinding, BuildError> {
        if ordinal != 0 || registration.instance() != self.instance || !self.scope.matches(tasks) {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        registration.bind_declared(&self.declaration, tasks, &self.cancel)
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
