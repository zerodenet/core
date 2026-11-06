use super::*;
use crate::grpc::{pool::State, GrpcPool};
use std::sync::Arc;
use zero_platform_tokio::{
    contract::{
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
            ResourceAccess, ResourceDeclaration, ResourceOwner,
        },
        lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle, Unfinished},
        CloseReason,
    },
    controlled_carrier::CarrierOwner,
};
pub struct NativePoolFactory;
impl ComponentFactory for NativePoolFactory {
    type Options = NativePoolOptions;
    fn describe(&self, options: &NativePoolOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Endpoint("grpc.client_pool".into()),
            roles: vec![],
            dependencies: options
                .totals
                .iter()
                .map(|totals| totals.dependency)
                .chain(options.node_dns)
                .collect(),
            static_identity: options.identity.clone(),
            resources: options
                .ca
                .iter()
                .map(|file| ResourceDeclaration::File {
                    path: file.path().to_path_buf(),
                    max_bytes: file.max_bytes(),
                })
                .chain(std::iter::once(
                    ResourceDeclaration::ControlledCarrierPool {
                        endpoint: options.endpoint.clone(),
                        dial: options.settings.dial.clone(),
                        datagram: false,
                        max_sockets: options.carrier_limit,
                    },
                ))
                .collect(),
            task_limit: Some(options.carrier_limit),
        }
    }
    fn build(
        &self,
        options: Arc<NativePoolOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        #[cfg(feature = "tls")]
        {
            self.build_owned(options, context, None)
        }
        #[cfg(not(feature = "tls"))]
        {
            self.build_owned(options, context)
        }
    }
}
mod factory;

struct Owner {
    pool: Arc<State>,
    managed: Arc<Managed>,
    carriers: CarrierOwner,
    resources: Arc<dyn ResourceAccess>,
    attempted: bool,
    claimed: bool,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.revoke(CloseReason::Shutdown);
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if self.managed.ready() {
            return Ok(());
        }
        if self.attempted || self.managed.cancel.is_cancelled() {
            return Err(error(
                self.managed.instance,
                Stage::Prepare,
                Reason::Revoked,
            ));
        }
        if Instant::now() >= deadline {
            return Err(error(
                self.managed.instance,
                Stage::Prepare,
                Reason::Timeout,
            ));
        }
        self.attempted = true;
        self.resources.register_controlled_carrier(
            usize::from(self.managed.options.ca.is_some()),
            self.carriers.registration(),
            &self.managed.tasks,
        )?;
        // Read actual native/users/task ACK receipts outside the pool lock.
        let previous = match &*self.pool.ownership.lock().unwrap() {
            Ownership::Managed(old) => Some(old.clone()),
            _ => None,
        };
        let replaceable = self.managed.options.selected_background
            && previous
                .as_ref()
                .is_some_and(|old| old.instance != self.managed.instance && old.ready_to_replace());
        let retired = self.pool.retired.load(std::sync::atomic::Ordering::Acquire);
        let previous = {
            let mut ownership = self.pool.ownership.lock().unwrap();
            let allowed = match &*ownership {
                Ownership::Pristine => !retired || self.managed.options.selected_background,
                Ownership::Retired(receipt) => {
                    self.managed.options.selected_background && receipt.permits_background()
                }
                Ownership::Managed(old) => {
                    replaceable
                        && previous
                            .as_ref()
                            .is_some_and(|actual| Arc::ptr_eq(old, actual))
                }
                Ownership::LegacyUsed => false,
            };
            if !allowed || self.pool.cached.lock().unwrap().is_some() {
                return Err(error(
                    self.managed.instance,
                    Stage::Prepare,
                    Reason::StalePlan,
                ));
            }
            self.pool
                .retired
                .store(false, std::sync::atomic::Ordering::Release);
            let previous =
                std::mem::replace(&mut *ownership, Ownership::Managed(self.managed.clone()));
            self.claimed = true;
            previous
        };
        drop(previous);
        #[cfg(feature = "tls")]
        if self.managed.options.selected_background {
            let prepared = match &self.managed.options.ca {
                Some(file) => Some(
                    file.prepare(
                        self.managed.options.settings.tls.as_ref().ok_or_else(|| {
                            error(self.managed.instance, Stage::Prepare, Reason::StalePlan)
                        })?,
                        &self.managed.options.settings.server,
                        self.resources.as_ref(),
                        self.managed.instance,
                        &self.managed.cancel,
                        deadline,
                    )
                    .await?,
                ),
                None => self
                    .managed
                    .options
                    .settings
                    .tls
                    .as_ref()
                    .map(|tls| {
                        crate::tls::OwnedPreparedClient::from_material(
                            &super::ech::validation_profile(tls)?,
                            None,
                            &self.managed.options.settings.server,
                            None,
                        )
                    })
                    .transpose()
                    .map_err(|source| {
                        error(self.managed.instance, Stage::Prepare, Reason::InvalidConfig)
                            .with_source(source)
                    })?,
            };
            *self.managed.prepared_tls.lock().unwrap() =
                if super::ech::dynamic(&self.managed.options).map_err(|source| {
                    error(self.managed.instance, Stage::Prepare, Reason::InvalidConfig)
                        .with_source(source)
                })? {
                    None
                } else {
                    prepared
                };
        }
        self.carriers.start(deadline).await?;
        #[cfg(feature = "tls")]
        self.managed
            .options
            .record_file_material(self.managed.instance)?;
        self.managed.activate()
    }
    fn revoke(&self, _: CloseReason) {
        self.managed.stop();
        self.carriers.revoke();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        if self.claimed {
            GrpcPool(self.pool.clone()).retire();
        }
        let physical = self.carriers.shutdown(deadline).await;
        // Foreign originals live in these same physical records, including a
        // prefix that failed before its loan could be installed in our slot.
        let pending = self.managed.reap(deadline).await;
        self.managed.harvest();
        let mut report = physical?;
        pending?;
        if self.managed.physical_unconfirmed() && report.is_complete() {
            report.unfinished.push(Unfinished::Io(
                "gRPC original carrier lacks a matching close receipt".into(),
            ));
        }
        if self.managed.budget.retained() != 0 {
            report.unfinished.push(Unfinished::Resource(
                "gRPC pending/live/retiring logical streams".into(),
            ));
        }
        if report.is_complete() {
            self.pool.compact_closed_owner();
        }
        Ok(report)
    }
}
