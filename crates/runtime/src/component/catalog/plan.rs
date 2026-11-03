//! Dependency-ordered start and confirmed retirement of the actual owners.
mod carrier;
use super::*;
use zero_platform_tokio::contract::lifecycle::TaskFailure;

#[derive(Debug)]
pub(super) struct PlanTaskFailure(
    pub(super) TaskFailure,
    pub(super) zero_core::component::ComponentInstanceId,
    AtomicBool,
);
impl std::fmt::Display for PlanTaskFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "component task {} ({:?}) failed: {}",
            self.0.id.0, self.0.class, self.0.error
        )
    }
}
impl std::error::Error for PlanTaskFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0.error)
    }
}
impl PlanTaskFailure {
    pub(super) fn new(
        failure: TaskFailure,
        instance: zero_core::component::ComponentInstanceId,
    ) -> Self {
        Self(failure, instance, AtomicBool::new(false))
    }
    pub(super) fn reported(&self) -> bool {
        self.2.load(Ordering::Acquire)
    }
    pub(super) fn confirm_reported(&self) {
        let _ = self
            .2
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire);
    }
    pub(super) fn retained(self: &Arc<Self>) -> ChannelError {
        let original = &self.0.error;
        let mut error = ChannelError::new(original.stage, original.reason, original.impact);
        error.component = original.component;
        error.session = original.session;
        error.delivery = original.delivery;
        error.retry = original.retry;
        error.with_source(self.clone())
    }
}

impl std::fmt::Debug for ComponentPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentPlan")
            .field("outbound_count", &self.outbounds.len())
            .finish()
    }
}
impl ComponentPlan {
    /// Exact original-resource observation, including roleless dependencies.
    /// No role identity is fabricated and no owner is started by this lookup.
    pub fn original_carrier_owner(
        &self,
        instance: zero_core::component::ComponentInstanceId,
        ordinal: usize,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration> {
        self.order
            .iter()
            .find(|slot| slot.instance_id() == instance)?
            .original_carrier_owner(ordinal)
    }
    /// Register only actual current/preparing generations before a background
    /// acquisition. This grants no socket/task and is not a close receipt.
    pub fn authorize_carrier_prefix(
        &self,
        consumer: zero_core::component::ComponentInstanceId,
        prefix: &[zero_core::component::RoleInstanceId],
    ) -> Result<(), ChannelError> {
        let owner = self
            .order
            .iter()
            .find(|slot| slot.instance_id() == consumer)
            .ok_or_else(|| {
                invalid(
                    Reason::StalePlan,
                    "background owner is not in this actual plan",
                )
            })?;
        if !owner.recipe.spec().resources.iter().any(|resource| matches!(resource,
            zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool { .. } | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledNodeStream { .. })) {
            return Err(invalid(Reason::Unsupported, "background owner has no finite controlled declaration"));
        }
        let mut donors = Vec::new();
        for role in prefix {
            let slot = self
                .role_slot(*role)
                .ok_or_else(|| invalid(Reason::StalePlan, "background donor role changed"))?;
            if self.deferred_devices.contains(&slot.id) {
                return Err(invalid(
                    Reason::Unsupported,
                    "background borrowing cannot depend on deferred native device start",
                ));
            }
            let spec = slot.recipe.spec();
            // A resource/task-free role directly backed by this same pool
            // already charges that pool's actual user root. Native owners or
            // merely transitive dependencies cannot claim this exception.
            if spec.resources.is_empty()
                && spec.task_limit.is_none()
                && slot.dependency_instances().contains(&consumer)
            {
                continue;
            }
            donors.push(slot.admission_grant());
        }
        let declared = self
            .order
            .iter()
            .flat_map(|slot| {
                slot.dependency_instances()
                    .iter()
                    .map(move |dep| (slot.instance_id(), *dep))
            })
            .collect();
        self.borrowing
            .borrow_from(owner.admission_grant(), donors, &declared)
    }
    pub(super) fn retirement_dependencies(
        &self,
        slot: &ComponentSlot,
    ) -> std::collections::BTreeSet<zero_core::component::ComponentInstanceId> {
        slot.dependency_instances()
            .iter()
            .copied()
            .chain(
                self.borrowing
                    .loan_snapshot()
                    .1
                    .into_iter()
                    .filter(|(from, _)| *from == slot.instance_id())
                    .map(|(_, to)| to),
            )
            .collect()
    }
    /// Exact actual role-generation lookup; no protocol projection or factory
    /// construction is performed for a retained background path.
    pub fn role_slot(
        &self,
        role: zero_core::component::RoleInstanceId,
    ) -> Option<Arc<ComponentSlot>> {
        self.outbounds
            .values()
            .chain(std::iter::once(&self.builtin))
            .find(|slot| slot.role_id() == role)
            .cloned()
    }
    /// The configured name of an exact currently assembled role generation.
    pub fn role_tag(&self, role: zero_core::component::RoleInstanceId) -> Option<&str> {
        self.outbounds
            .iter()
            .find(|(_, slot)| slot.role_id() == role)
            .map(|(tag, _)| tag.as_str())
    }
    pub(super) fn can_reuse(&self) -> bool {
        self.order.iter().all(|slot| slot.can_reuse())
    }

    pub async fn start(&self, deadline: tokio::time::Instant) -> Result<(), ChannelError> {
        for slot in self.startable_before_devices() {
            slot.start(deadline).await?;
        }
        self.scan_tasks(deadline).await?;
        if self
            .order
            .iter()
            .filter(|slot| self.deferred_devices.contains(&slot.id))
            .all(|slot| slot.has_started())
        {
            self.started.store(true, Ordering::Release);
        }
        Ok(())
    }
    pub(super) async fn scan_tasks(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        for slot in &self.order {
            if self.deferred_devices.contains(&slot.id)
                && !slot.has_started()
                && slot.task_fault().is_none()
            {
                continue;
            }
            slot.scan_tasks(deadline).await?;
        }
        Ok(())
    }
    pub(super) fn startable_before_devices(&self) -> impl Iterator<Item = &Arc<ComponentSlot>> {
        self.order
            .iter()
            .filter(|slot| !self.deferred_devices.contains(&slot.id))
    }
    pub fn device_component(&self, id: ComponentId) -> Result<Arc<ComponentSlot>, ChannelError> {
        self.order
            .iter()
            .find(|slot| slot.id == id && self.deferred_devices.contains(&id))
            .cloned()
            .ok_or_else(|| {
                invalid(
                    Reason::StalePlan,
                    "no actual deferred device component in this union",
                )
            })
    }
    pub fn configured(&self, tag: &str) -> Option<Arc<ComponentSlot>> {
        self.outbounds.get(tag).cloned()
    }
    pub fn direct(&self, tag: Option<&str>) -> Result<Arc<ComponentSlot>, ChannelError> {
        match tag {
            None => Ok(self.builtin.clone()),
            Some(tag) => self.outbounds.get(tag).cloned().ok_or_else(|| {
                invalid(
                    Reason::StalePlan,
                    format!("no prepared component for outbound `{tag}`"),
                )
            }),
        }
    }
    pub async fn shutdown(&self, deadline: tokio::time::Instant) -> Result<(), ChannelError> {
        self.shutdown_except(deadline, None).await
    }
    pub(super) async fn shutdown_except(
        &self,
        deadline: tokio::time::Instant,
        keep: Option<&ComponentPlan>,
    ) -> Result<(), ChannelError> {
        let retiring = self
            .order
            .iter()
            .filter(|slot| {
                !keep.is_some_and(|plan| plan.order.iter().any(|active| Arc::ptr_eq(slot, active)))
            })
            .map(|slot| slot.instance_id())
            .collect();
        let declared = self.confirm_instances(&retiring, deadline).await;
        if let Some(failed) = self
            .failures
            .lock()
            .expect("component failures poisoned")
            .values()
            .next()
        {
            return Err(failed.retained());
        }
        declared
    }
}
