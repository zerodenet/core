//! Build/start/cleanup of these same original peers in one standard Host slot.
use super::super::recipe::carrier::{Carrier, Graph};
use super::*;
use zero_platform_tokio::contract::{
    component::{
        BoundDatagramRequest, BuildContext, ComponentFactory, ComponentInstance, ComponentRoles,
    },
    lifecycle::{erase_lifecycle, CloseReport, ResourceLifecycle, Unfinished},
    ChannelError, CloseReason,
};
pub(super) struct Factory;
impl ComponentFactory for Factory {
    type Options = StandaloneStackOptions;
    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        options.spec.clone()
    }
    fn build(
        &self,
        options: Arc<Self::Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, ChannelError> {
        if context.dependencies.len() != options.spec.dependencies.len()
            || options
                .spec
                .dependencies
                .iter()
                .any(|declared| context.dependency(*declared).is_none())
        {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        {
            let mut state = options
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.stopped || state.attempted || state.scope.is_some() {
                return Err(error(Stage::Prepare, Reason::Revoked));
            }
            state.scope = Some(context.tasks.scope_identity());
            state.dependencies = Some(context.dependencies.clone());
            state.resources = Some(context.resources.clone());
        }
        for peer in &options.peers {
            peer.owner
                .bind_scope(context.tasks.downgrade())
                .map_err(|source| failure(Stage::Prepare, source))?;
        }
        let carrier = Arc::new(Carrier {
            options: Graph::Standalone(options.clone()),
            resources: context.resources,
            scope: context.tasks.scope_identity(),
        });
        options
            .carrier
            .set(Arc::downgrade(&carrier))
            .map_err(|_| error(Stage::Prepare, Reason::StalePlan))?;
        Ok(ComponentInstance {
            roles: ComponentRoles {
                stream: Some(carrier.clone()),
                datagram: Some(carrier.clone()),
                packet: Some(carrier),
                ..Default::default()
            },
            lifecycle: erase_lifecycle(Owner { options }),
        })
    }
}
struct Owner {
    options: Arc<StandaloneStackOptions>,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: tokio::time::Instant) -> Result<(), ChannelError> {
        let (resources, scope, endpoints, stacks) = {
            let mut state = self
                .options
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.stopped || state.attempted {
                return Err(error(Stage::Prepare, Reason::Revoked));
            }
            let resources = state
                .resources
                .clone()
                .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
            let scope = state
                .scope
                .clone()
                .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
            let endpoints = state
                .endpoints
                .clone()
                .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
            state.attempted = true;
            (
                resources,
                scope,
                endpoints,
                std::mem::take(&mut state.stacks),
            )
        };
        for (index, (endpoint, options)) in endpoints.into_iter().zip(stacks).enumerate() {
            let peer = &self.options.peers[index];
            let (pause, checkpoint) = ReadPause::new(scope.clone());
            peer.pause
                .set(pause.clone())
                .map_err(|_| error(Stage::Prepare, Reason::StalePlan))?;
            let tunnel = self
                .options
                .plan
                .build_tunnel(index)
                .map_err(|source| failure(Stage::Prepare, source))?;
            let (recipe, mut pending, carrier) =
                crate::runtime::raw_ip::WireStackRecipe::prepare_hosted(
                    self.options.plan.local_addresses(),
                    self.options.plan.mtu(),
                    endpoint,
                    tunnel,
                    options,
                )
                .map_err(|source| failure(Stage::Prepare, source))?;
            peer.original
                .set(carrier)
                .map_err(|_| error(Stage::Prepare, Reason::StalePlan))?;
            let device = peer
                .owner
                .start_wire(recipe)
                .map_err(|source| failure(Stage::Prepare, source))?;
            let driver = peer
                .owner
                .driver_witness()
                .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
            pause.bind(&driver)?;
            pending
                .wait_input_pending(deadline)
                .await
                .map_err(|source| failure(Stage::Prepare, source))?;
            // The original TCP-control/wire registrations really exist before
            // first native acquisition. No pending marker substitutes for a
            // Scope witness: this receiver was polled by that exact driver.
            let ordinal = index * 2;
            let binding = resources.activate_bound_datagram(ordinal, &driver)?;
            let reads = resources.take_packet_datagram_reads(ordinal)?;
            let granted = resources
                .open_bound_datagram_attempt(BoundDatagramRequest {
                    binding,
                    peer: endpoint,
                    expected_network_generation: self.options.binding.network_generation,
                })
                .map_err(|failure| failure.into_parts().0)?;
            pending
                .install(granted, reads, driver, checkpoint)
                .map_err(|source| failure(Stage::Prepare, source))?;
            tokio::time::timeout_at(deadline, device.wait_ready())
                .await
                .map_err(|source| error(Stage::Prepare, Reason::Timeout).with_source(source))?
                .map_err(|source| failure(Stage::Prepare, source))?;
        }
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.options.revoke();
    }
    async fn shutdown(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.options.revoke();
        let mut report = CloseReport::default();
        let mut first = None;
        for peer in &self.options.peers {
            match peer.owner.stop_users(deadline).await {
                Ok(users) => report.append(users),
                Err(source) => {
                    first.get_or_insert_with(|| failure(Stage::Close, source));
                }
            }
            match peer.owner.stop(deadline).await {
                Ok(joined) => report.append(joined),
                Err(source) => {
                    first.get_or_insert_with(|| failure(Stage::Close, source));
                }
            }
            // A retained task fault is independent of the actual matching
            // Scope joins. Even on that fault, return this original view once
            // its real driver has joined; preserve the error for the Host.
            if peer.owner.is_drained()
                && peer
                    .owner
                    .driver_witness()
                    .is_none_or(|actual| actual.is_joined())
            {
                if let Some(carrier) = peer.original.get() {
                    carrier.return_view().await;
                }
            } else {
                report.unfinished.push(Unfinished::Resource(
                    "same standalone wire Scope/view".into(),
                ));
            }
        }
        // Native close belongs to the very same HostResources Bound original.
        // It runs with Scope cleanup; waiting for it here would deadlock the
        // original view return. No NativeClosed receipt is minted by this owner.
        first.map_or(Ok(report), Err)
    }
}
