//! A non-listening Source uses the same Service owner and canonical Stream slot.
mod carrier;
mod retained;
use super::operation::{worker_launch, ServiceContext};
use crate::{
    component::ComponentRecipe, protocol_registry::TcpRuntimeServices,
    runtime::route_runtime::InboundRouteRuntime,
};
pub(super) use retained::Retained;
use std::sync::Arc;
use zero_core::component::{ComponentInstanceId, ComponentRoleIds, InboundId};
use zero_engine::EngineError;
use zero_platform_tokio::contract::{
    component::{BuildContext, ResourceAccess, RoleKind},
    inbound::DynInboundSource,
    lifecycle::DynResourceLifecycle,
    scope::TaskRegistrar,
};
#[derive(Clone)]
pub(crate) struct NativeServiceOperation {
    pub(crate) recipe: ComponentRecipe,
    pub(crate) outbound: String,
    pub(crate) tag: String,
    pub(crate) sniffing: Option<crate::runtime::sniff::SniffingPolicy>,
}
pub(super) struct Installed {
    pub(super) source: Box<dyn DynInboundSource>,
    pub(super) identity: ComponentInstanceId,
    pub(super) failures: zero_platform_tokio::AcceptedTaskFailures,
    pub(super) continuity: Arc<crate::runtime::route_runtime::DatagramContinuityHost>,
}
impl NativeServiceOperation {
    pub(super) fn install(
        &self,
        services: &TcpRuntimeServices,
        tasks: TaskRegistrar,
        retained: &Arc<Retained>,
    ) -> Result<Installed, EngineError> {
        let node = services.service_node(&self.outbound)?;
        let dependency = node.slot.instance_id();
        let recipe = self
            .recipe
            .clone()
            .with_inbound_dependencies(&[dependency])
            .map_err(std::io::Error::other)?;
        if recipe.spec().roles != [RoleKind::Inbound]
            || !recipe.spec().resources.is_empty()
            || recipe.spec().task_limit.is_some()
        {
            return Err(std::io::Error::other(
                "active Source must use its donor's actual native/task providers",
            )
            .into());
        }
        let identity = services.service_identity()?;
        let failures = zero_platform_tokio::AcceptedTaskFailures::prepare(
            identity,
            &tasks,
            &services
                .service_original_budgets()
                .packet
                .driver_metadata_view(),
        )
        .map_err(std::io::Error::other)?;
        let resources = crate::component::instance::HostResources::new_hosted(
            identity,
            &[],
            tasks.cancellation(),
            tasks.downgrade(),
            services.service_original_budgets(),
        )
        .map_err(std::io::Error::other)?;
        let access: Arc<dyn ResourceAccess> = resources;
        let source_cancel = tasks.cancellation().child_token();
        let mut instance = recipe
            .build(BuildContext {
                component: identity.component,
                generation: identity.generation,
                dependencies: Arc::from([dependency]),
                roles: ComponentRoleIds {
                    inbound: Some(InboundId(identity.component.0)),
                    ..Default::default()
                },
                resources: access,
                tasks: tasks.without_tasks(),
                cancel: source_cancel.clone(),
                background_carrier: Some(carrier::transport(services, node)),
            })
            .map_err(std::io::Error::other)?;
        let source = instance
            .roles
            .inbound
            .take()
            .ok_or_else(|| std::io::Error::other("active factory omitted its real Source"))?;
        let access = crate::runtime::inbound_owners::InboundTaskAccess::for_service_scope(
            identity,
            tasks.clone(),
            source_cancel.clone(),
            &retained.peers,
        );
        let limits = &services.config().runtime;
        let associations =
            std::num::NonZeroUsize::new(limits.effective_inbound_max_datagram_associations())
                .ok_or_else(|| std::io::Error::other("active association ceiling is zero"))?;
        let task_limit =
            std::num::NonZeroUsize::new(limits.effective_inbound_max_tasks_per_carrier(520))
                .ok_or_else(|| std::io::Error::other("active carrier task ceiling is zero"))?;
        let mut peers = crate::runtime::inbound_owners::InboundPeerTasks::with_limit(
            identity,
            source_cancel,
            services.service_inventory(),
            associations,
        );
        peers
            .bind_task_failures(failures.clone())
            .map_err(std::io::Error::other)?;
        let peers = Arc::new(peers);
        let continuity = Arc::new(crate::runtime::route_runtime::DatagramContinuityHost::new(
            access,
            peers.clone(),
            associations,
            task_limit,
            limits.effective_inbound_max_resources_per_carrier(257),
        ));
        retained.peers.lock().unwrap().push(peers);
        retained.install(instance.lifecycle, failures.clone());
        Ok(Installed {
            source,
            identity,
            failures,
            continuity,
        })
    }
}
impl NativeServiceOperation {
    pub(crate) async fn run(self, context: ServiceContext) -> Result<(), EngineError> {
        let Installed {
            mut source,
            identity,
            failures,
            continuity,
        } = context.native;
        let shared = context.ingress.clone();
        let publication = shared.configuration_publication();
        // A Ready candidate cannot consult the old Engine domain and
        // accidentally reselect its old component plan before final commit.
        // This is admission waiting, never peer readiness or a close proof.
        while !publication.is_committed() {
            tokio::select! {biased;
                _=context.cancel.cancelled()=>return Ok(()),
                _=tokio::time::sleep(std::time::Duration::from_millis(100))=>{},
            }
        }
        loop {
            context.retained.reap().await?;
            let Some(launch) =
                worker_launch(&context.tasks, &context.cancel, &context.retained).await?
            else {
                return Ok(());
            };
            // Keep the exact Source accept future between maintenance ticks.
            let next = source.next(zero_platform_tokio::contract::context::AcceptContext {
                deadline: None,
                cancel: context.cancel.clone(),
            });
            tokio::pin!(next);
            let pending = loop {
                tokio::select! {biased;
                    _=context.cancel.cancelled()=>return Ok(()),
                    result=&mut next=>break result,
                    _=tokio::time::sleep(std::time::Duration::from_millis(100))=>context.retained.reap().await?,
                }
            };
            let pending = match pending {
                Ok(Some(pending)) => pending,
                Ok(None) if context.cancel.is_cancelled() => return Ok(()),
                Ok(None) => {
                    return Err(std::io::Error::other("active Source ended unexpectedly").into())
                }
                Err(error) => {
                    if error.stage == zero_platform_tokio::contract::Stage::Close {
                        return Err(std::io::Error::other(error).into());
                    }
                    tracing::warn!(inbound_tag=%self.tag,%error,"active ingress connection failed");
                    continue;
                }
            };
            let facts = pending.carrier_facts().cloned().ok_or_else(|| {
                std::io::Error::other("active Source omitted actual carrier facts")
            })?;
            let original = pending.stream_carrier().cloned().ok_or_else(|| {
                std::io::Error::other("active Source omitted the actual sealed Stream loan origin")
            })?;
            let runtime =
                InboundRouteRuntime::new(shared.clone(), self.tag.clone(), facts.original_source)
                    .with_service_instance(identity)
                    .with_service_continuity(continuity.clone())
                    .with_service_sniffing(self.sniffing.clone())
                    .with_local_addr(facts.local_endpoint);
            let registrar = context.tasks.clone();
            let stop = context.cancel.child_token();
            let cancel = stop.clone();
            let (publish, installed) = tokio::sync::oneshot::channel();
            let mut liability = failures.reserve().map_err(std::io::Error::other)?;
            liability.begin_registration();
            let launched=launch.spawn_tracked(async move {
                    let graph:zero_platform_tokio::AcceptedPeerScope=installed.await.map_err(|_|super::lifecycle::failure(std::io::Error::other("original active worker publication interrupted").into()))?;
                    let runtime=runtime.with_accepted_tasks(graph.tasks())?;
                    let result=tokio::select!{biased;_=cancel.cancelled()=>Ok(()),result=runtime.complete_request(pending,cancel.clone())=>result};
                    cancel.cancel();
                    if let Err(error)=result {tracing::debug!(%error,"standard active ingress worker ended");}
                    Ok(())
                });
            let (_, witness) = match launched {
                Ok(actual) => actual,
                Err(error) => {
                    liability.known_not_registered();
                    return Err(std::io::Error::other(error).into());
                }
            };
            let original_fault = liability
                .registered(&witness)
                .map_err(std::io::Error::other)?;
            let task = witness.task_id().ok_or_else(|| {
                std::io::Error::other("actual active primary missing its task id")
            })?;
            let graph = zero_platform_tokio::AcceptedPeerScope::for_stream_carrier(
                &registrar,
                task,
                witness.clone(),
                stop,
                context.max_tasks,
                context.max_resources,
                &original,
            )
            .map_err(std::io::Error::other)?;
            graph
                .bind_task_failure_registration(&original_fault)
                .map_err(std::io::Error::other)?;
            // Strong graph custody precedes publication and all Rvs I/O.
            context.retained.install_group(graph.clone(), witness);
            publish.send(graph).map_err(|_| {
                std::io::Error::other("actual active primary exited before publication")
            })?;
        }
    }
}
