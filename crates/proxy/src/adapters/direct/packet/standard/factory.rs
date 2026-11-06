use super::*;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentRoles},
    context::OpenContext,
    lifecycle::erase_lifecycle,
    packet::{BoxPacketSession, PacketRequest},
    BoxFuture,
};
pub(super) struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        options.spec.clone()
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, ChannelError> {
        let instance = context.instance();
        let scope = context.tasks.scope_identity();
        let mut installed = options.scope.lock().unwrap();
        if installed.is_some() {
            return Err(error(
                Reason::StalePlan,
                "Direct native original Options were already built",
            ));
        }
        *installed = Some(scope);
        drop(installed);
        let shared = Arc::new(Shared {
            device: OnceLock::new(),
        });
        let role = Arc::new(Outbound(shared.clone()));
        options
            .role
            .set(Arc::downgrade(&role))
            .map_err(|_| error(Reason::StalePlan, "Direct original role already installed"))?;
        Ok(ComponentInstance {
            roles: ComponentRoles {
                packet: Some(role),
                ..Default::default()
            },
            lifecycle: erase_lifecycle(owner::Owner {
                shared,
                options,
                resources: context.resources,
                tasks: context.tasks,
                instance,
            }),
        })
    }
}
impl DynPacketOutbound for Outbound {
    fn open(
        &self,
        request: PacketRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<BoxPacketSession, ChannelError>> {
        Box::pin(async move {
            self.0
                .device
                .get()
                .ok_or_else(|| {
                    error(
                        Reason::Revoked,
                        "Direct native Packet original is not started",
                    )
                })?
                .open_packet(request, context)
        })
    }
}
