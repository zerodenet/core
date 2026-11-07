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

mod options;
pub(crate) use options::Options;
pub(crate) struct Factory<F>(pub(crate) F);
impl<F: ComponentFactory> ComponentFactory for Factory<F> {
    type Options = Options<F::Options>;
    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        let mut spec = self.0.describe(&options.inner);
        // The same original carrier owns at most one live setup or rejected
        // preparation job per reservation. The protocol opt-in preserves this
        // actual Scope; it is not a taskless or cached-driver exemption.
        if spec.task_limit.is_none() || options.preparation_jobs {
            spec.task_limit = Some(options.limit);
        }
        spec.resources
            .push(ResourceDeclaration::ControlledNodeCarrier {
                endpoint: options.endpoint.clone(),
                dial: options.dial.clone(),
                max_sockets: options.limit,
            });
        if let Some(node) = options.node_dns {
            if !spec.dependencies.contains(&node) {
                spec.dependencies.push(node);
            }
        }
        if let Some((process, _)) = options.process {
            if !spec.dependencies.contains(&process) {
                spec.dependencies.push(process);
            }
        }
        let mut identity = spec.static_identity.to_vec();
        identity.extend_from_slice(b"same-slot-prepared-carrier-v2");
        identity.push(u8::from(options.preparation_jobs));
        if options.cleanup_network.is_some() {
            identity.extend_from_slice(b"same-retained-catalog-source-v1");
        }
        identity.extend_from_slice(&options.limit.get().to_le_bytes());
        identity.extend_from_slice(format!("{:?}{:?}", options.endpoint, options.dial).as_bytes());
        if let Some(node) = options.node_dns {
            identity.extend_from_slice(b"taskless.actual-node-dns");
            identity.extend_from_slice(&node.0.to_le_bytes());
        }
        if let Some((process, startup)) = options.process {
            identity.extend_from_slice(b"actual-process-dependency-v1");
            identity.extend_from_slice(&process.0.to_le_bytes());
            identity.extend_from_slice(&startup.as_nanos().to_le_bytes());
        }
        spec.static_identity = identity.into();
        spec
    }
    fn build(
        &self,
        options: Arc<Self::Options>,
        mut context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let original = self.0.describe(&options.inner);
        // A transport driver must keep its actual protocol owner; it cannot be
        // made taskless by placing its role inside this custody adapter.
        if original.task_limit.is_some() && !options.preparation_jobs {
            return Err(super::super::instance::invalid(
                Reason::InvalidConfig,
                "carrier framing requires the actual taskless role",
            ));
        }
        let mut expected = original.dependencies.clone();
        if let Some(node) = options.node_dns {
            if !expected.contains(&node) {
                expected.push(node);
            }
        }
        if let Some((process, _)) = options.process {
            if !expected.contains(&process) {
                expected.push(process);
            }
        }
        let process = options
            .process
            .map(|(id, startup)| {
                context
                    .dependency(id)
                    .map(|instance| (instance, startup))
                    .ok_or_else(|| {
                        super::super::instance::invalid(
                            Reason::StalePlan,
                            "actual process dependency changed",
                        )
                    })
            })
            .transpose()?;
        if context.dependencies.len() != expected.len()
            || expected.iter().any(|id| context.dependency(*id).is_none())
        {
            return Err(super::super::instance::invalid(
                Reason::StalePlan,
                "taskless actual Node dependency changed",
            ));
        }
        // This custody owner consumes the real Node dependency; the original
        // protocol factory receives exactly its own unchanged dependency set.
        context.dependencies = context
            .dependencies
            .iter()
            .copied()
            .filter(|id| original.dependencies.contains(&id.component))
            .collect::<Vec<_>>()
            .into();
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
        let outbound = Outbound {
            inner,
            tasks: Some(tasks.clone()),
            cleanup_network: options.cleanup_network.clone(),
            process,
            endpoint,
            dial,
            grants: carrier.grants(),
            transfers: transfers.clone(),
            limit: options.limit,
        };
        // Existing native Datagram owners (SS, association/UDP providers) keep
        // their own exact role. Only framing with File-only resources borrows
        // the same actual per-connection Stream original/N.
        if original
            .resources
            .iter()
            .all(|resource| matches!(resource, ResourceDeclaration::File { .. }))
        {
            if let Some(inner) = instance.roles.datagram.take() {
                instance.roles.datagram = Some(
                    zero_platform_tokio::contract::datagram::erase_datagram(datagram::Datagram {
                        owner: outbound.clone(),
                        inner,
                    }),
                );
            }
        }
        instance.roles.stream = Some(erase_stream(outbound));
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

mod admission;
mod business;

#[cfg(test)]
mod tests;

mod attempt;
mod closing;
mod pending;
mod service;
pub(crate) use service::into_stream as service_stream;
mod setup;

mod datagram;

mod job;
