//! Host generations fence each new operation, including cached role handles.
use super::instance::invalid;
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    context::{OpenContext, RequestIdentity},
    stream::{BoxStream, ConnectRequest, Connected, DynStreamOutbound},
    BoxFuture, ChannelError, Reason,
};

#[derive(Default)]
struct View {
    stopping: bool,
    current: BTreeMap<ComponentInstanceId, Arc<()>>,
    preparing: BTreeMap<ComponentInstanceId, Arc<()>>,
}
#[derive(Default)]
pub(super) struct HostAdmission(RwLock<View>);
impl HostAdmission {
    pub(super) fn stage(&self, instances: impl Iterator<Item = (ComponentInstanceId, Arc<()>)>) {
        self.0.write().expect("host admission poisoned").preparing = instances.collect();
    }
    pub(super) fn publish(&self, instances: impl Iterator<Item = (ComponentInstanceId, Arc<()>)>) {
        let mut view = self.0.write().expect("host admission poisoned");
        view.current = instances.collect();
        view.preparing.clear();
    }
    pub(super) fn stop_business(&self) {
        self.0.write().expect("host admission poisoned").stopping = true;
    }
    pub(super) fn close(&self) {
        *self.0.write().expect("host admission poisoned") = View::default();
    }
    pub(super) fn check(
        &self,
        instance: ComponentInstanceId,
        owner: &Arc<()>,
        context: &OpenContext,
    ) -> Result<(), ChannelError> {
        let view = self.0.read().expect("host admission poisoned");
        let service = matches!(context.identity, RequestIdentity::Service(_));
        let permitted = ((!view.stopping || service)
            && view
                .current
                .get(&instance)
                .is_some_and(|token| Arc::ptr_eq(token, owner)))
            || (service
                && view
                    .preparing
                    .get(&instance)
                    .is_some_and(|token| Arc::ptr_eq(token, owner)));
        if permitted {
            return Ok(());
        }
        let mut error = invalid(
            Reason::StalePlan,
            "component generation is not admitted by the host",
        );
        error.component = Some(instance.component);
        error.session = context.identity.session();
        Err(error)
    }
}

pub(super) struct FencedStream {
    pub(super) inner: Arc<dyn DynStreamOutbound>,
    pub(super) host: Arc<HostAdmission>,
    pub(super) instance: ComponentInstanceId,
    pub(super) owner: Arc<()>,
}
impl DynStreamOutbound for FencedStream {
    fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<Connected<BoxStream>, ChannelError>> {
        Box::pin(async move {
            context.session_identity()?;
            self.host.check(self.instance, &self.owner, &context)?;
            self.inner.connect(request, context).await
        })
    }
}

// Only new opens consult the host view. Transferred sessions keep their real
// protocol/session owner and are not spuriously cancelled by a publication.
pub(super) struct FencedDatagram {
    pub(super) inner: Arc<dyn zero_platform_tokio::contract::datagram::DynDatagramOutbound>,
    pub(super) host: Arc<HostAdmission>,
    pub(super) instance: ComponentInstanceId,
    pub(super) owner: Arc<()>,
}
impl zero_platform_tokio::contract::datagram::DynDatagramOutbound for FencedDatagram {
    fn open(
        &self,
        request: zero_platform_tokio::contract::datagram::DatagramRequest,
        context: OpenContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async move {
            context.session_identity()?;
            self.host.check(self.instance, &self.owner, &context)?;
            self.inner.open(request, context).await
        })
    }
}
