//! Actual taskless protocol framing over one same-slot native or borrowed carrier.
//! The protocol keeps its existing role, TLS/security state and lifecycle.
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use tokio::time::Instant;
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentSpec, ResourceAccess,
            ResourceDeclaration,
        },
        context::OpenContext,
        lifecycle::{
            erase_lifecycle, BuildError, CloseReport, DynChannelControl, DynResourceLifecycle,
            ResourceLifecycle,
        },
        network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
        scope::TaskRegistrar,
        stream::{
            erase_stream, BoxStream, ConnectRequest, Connected, DynStreamOutbound, StreamOutbound,
        },
        BoxFuture, CancellationToken, ChannelError, CloseReason, Reason,
    },
    controlled_carrier::{
        loan::{LoanOwner, StreamLoan},
        CarrierAdoption, CarrierGrant, CarrierGrants, CarrierOwner,
    },
};

pub(crate) struct Options<T> {
    inner: Arc<T>,
    endpoint: Destination,
    dial: zero_traits::DialPolicy,
    limit: NonZeroUsize,
}
impl<T> Options<T> {
    pub(crate) fn new(
        inner: T,
        endpoint: Destination,
        dial: zero_traits::DialPolicy,
        limit: NonZeroUsize,
    ) -> Self {
        Self {
            inner: Arc::new(inner),
            endpoint,
            dial,
            limit,
        }
    }
}
pub(crate) struct Factory<F>(pub(crate) F);
impl<F: ComponentFactory> ComponentFactory for Factory<F> {
    type Options = Options<F::Options>;
    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        let mut spec = self.0.describe(&options.inner);
        spec.resources
            .push(ResourceDeclaration::ControlledCarrierPool {
                endpoint: options.endpoint.clone(),
                dial: options.dial.clone(),
                datagram: false,
                max_sockets: options.limit,
            });
        let mut identity = spec.static_identity.to_vec();
        identity.extend_from_slice(b"same-slot-taskless-carrier-v1");
        identity.extend_from_slice(&options.limit.get().to_le_bytes());
        identity.extend_from_slice(format!("{:?}{:?}", options.endpoint, options.dial).as_bytes());
        spec.static_identity = identity.into();
        spec
    }
    fn build(
        &self,
        options: Arc<Self::Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let original = self.0.describe(&options.inner);
        // A transport driver must keep its actual protocol owner; it cannot be
        // made taskless by placing its role inside this custody adapter.
        if original.task_limit.is_some() {
            return Err(super::super::instance::invalid(
                Reason::InvalidConfig,
                "carrier framing requires the actual taskless role",
            ));
        }
        let carrier = CarrierOwner::new(context.instance(), options.limit, &context.cancel);
        let resources = context.resources.clone();
        let tasks = context.tasks.clone();
        let endpoint = options.endpoint.clone();
        let dial = options.dial.clone();
        let transfers = Arc::new(Mutex::new(Vec::new()));
        let mut instance = self.0.build(options.inner.clone(), context)?;
        let inner = instance.roles.stream.take().ok_or_else(|| {
            super::super::instance::invalid(
                Reason::InvalidConfig,
                "carrier framing requires the actual Stream role",
            )
        })?;
        instance.roles.stream = Some(erase_stream(Outbound {
            inner,
            endpoint,
            dial,
            grants: carrier.grants(),
            transfers: transfers.clone(),
        }));
        instance.lifecycle = erase_lifecycle(Owner {
            inner: instance.lifecycle,
            carrier,
            resources,
            tasks,
            ordinal: original.resources.len(),
            transfers,
        });
        Ok(instance)
    }
}

mod transfer;
use transfer::{Control, Opening, Transfer, Transfers};
mod lifecycle;
use lifecycle::Owner;
mod stream;
use stream::Outbound;
mod opener;
use opener::Opener;

mod caller;
