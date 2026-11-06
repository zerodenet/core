//! One canonical key map for both explicit library and actual Host callers.
use super::*;
pub(super) mod retirement;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::scope::{TaskRegistrar, TaskScopeIdentity};
#[derive(Default, Debug)]
pub(crate) struct PluginPool(Arc<Registry>);
#[derive(Default, Debug)]
pub(super) struct Registry {
    plans: Mutex<HashMap<Key, Entry>>,
}
#[derive(Debug)]
struct Entry {
    data: Arc<PlanData>,
    mode: Mode,
}
#[derive(Clone, Debug)]
enum Mode {
    Cold,
    Library,
    Native(ComponentInstanceId, TaskScopeIdentity),
}
impl Mode {
    fn same(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Cold, Self::Cold) | (Self::Library, Self::Library) => true,
            (Self::Native(a, sa), Self::Native(b, sb)) => a == b && sa.same_scope(sb),
            _ => false,
        }
    }
}
fn conflict(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, message)
}
impl PluginPool {
    pub(crate) fn plan(&self, host: &str, port: u16, config: PluginConfig) -> PluginPlan {
        let key = Key {
            host: host.into(),
            port,
            config,
        };
        let data = self
            .0
            .plans
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_insert_with(|| Entry {
                data: Arc::default(),
                mode: Mode::Cold,
            })
            .data
            .clone();
        PluginPlan {
            key,
            root: self.0.clone(),
            data,
        }
    }
    pub(crate) fn clear(&self) {
        // An actual exposed library Child has no tree proof. Reload must never
        // erase that debt. Cold/positive-native records may be removed only
        // after observing them outside the canonical admission lock.
        let entries: Vec<_> = self
            .0
            .plans
            .lock()
            .unwrap()
            .iter()
            .map(|(key, entry)| {
                (
                    key.clone(),
                    PluginPlan {
                        key: key.clone(),
                        root: self.0.clone(),
                        data: entry.data.clone(),
                    },
                    entry.mode.clone(),
                )
            })
            .collect();
        let ready: Vec<_> = entries
            .into_iter()
            .filter(|(_, plan, mode)| {
                matches!(mode, Mode::Cold) && plan.untouched()
                    || matches!(mode, Mode::Native(..)) && plan.confirmed_retired()
            })
            .collect();
        let mut removed = Vec::new();
        {
            let mut entries = self.0.plans.lock().unwrap();
            for (key, plan, mode) in ready {
                if entries.get(&key).is_some_and(|entry| {
                    Arc::ptr_eq(&entry.data, &plan.data) && mode.same(&entry.mode)
                }) {
                    removed.push(entries.remove(&key));
                }
            }
        }
        drop(removed);
    }
}
impl PluginPlan {
    fn new(key: Key, root: Arc<Registry>) -> Self {
        Self {
            key,
            root,
            data: Arc::default(),
        }
    }
    pub(super) fn current(&self) -> io::Result<Self> {
        let root = self.root.clone();
        let current = root
            .plans
            .lock()
            .unwrap()
            .get(&self.key)
            .map(|entry| PluginPlan {
                key: self.key.clone(),
                root: root.clone(),
                data: entry.data.clone(),
            })
            .ok_or_else(revoked)?;
        if !Arc::ptr_eq(&current.data, &self.data) {
            return Err(revoked());
        }
        Ok(current)
    }
    pub(in crate::transport::plugin) fn validate_native_transition(&self) -> io::Result<()> {
        let root = self.root.clone();
        let entries = root.plans.lock().unwrap();
        if entries
            .get(&self.key)
            .is_some_and(|entry| matches!(entry.mode, Mode::Library))
        {
            return Err(conflict(
                "legacy plugin tree remains unconfirmed; canonical executor cannot change",
            ));
        }
        Ok(())
    }
    pub(in crate::transport::plugin) fn select_native(
        &self,
        instance: ComponentInstanceId,
        tasks: &TaskRegistrar,
        endpoint_ordinal: Option<usize>,
        resources: &dyn zero_platform_tokio::contract::component::ResourceAccess,
    ) -> io::Result<Self> {
        let retirement =
            retirement::Retirement::declared(instance, tasks, endpoint_ordinal, resources)?;
        let root = self.root.clone();
        loop {
            let snapshot = root.plans.lock().unwrap().get(&self.key).map(|entry| {
                (
                    PluginPlan {
                        key: self.key.clone(),
                        root: root.clone(),
                        data: entry.data.clone(),
                    },
                    entry.mode.clone(),
                )
            });
            if matches!(&snapshot, Some((_, Mode::Library))) {
                return Err(conflict(
                    "legacy plugin tree remains unconfirmed; canonical executor cannot change",
                ));
            }
            if let Some((plan, Mode::Native(actual, scope))) = &snapshot {
                if *actual == instance {
                    return if scope.matches(tasks)
                        && plan.retirement.lock().unwrap().same(&retirement)
                    {
                        Ok(plan.clone())
                    } else {
                        Err(conflict(
                            "same plugin instance cannot move to a foreign Scope",
                        ))
                    };
                }
                if !plan.confirmed_retired() {
                    return Err(conflict(
                        "same canonical plugin native/user/actor ACK debt remains",
                    ));
                }
            }
            if let Some((plan, Mode::Cold)) = &snapshot {
                if !plan.untouched() {
                    return Err(conflict(
                        "same canonical plugin startup still owns an original",
                    ));
                }
            }
            let candidate = match &snapshot {
                Some((plan, Mode::Cold)) if !plan.cancel.is_cancelled() => plan.clone(),
                _ => PluginPlan::new(self.key.clone(), root.clone()),
            };
            let mut entries = root.plans.lock().unwrap();
            let unchanged = match (&snapshot, entries.get(&self.key)) {
                (None, None) => true,
                (Some((plan, mode)), Some(entry)) => {
                    Arc::ptr_eq(&plan.data, &entry.data) && mode.same(&entry.mode)
                }
                _ => false,
            };
            if !unchanged {
                drop(entries);
                continue;
            }
            let old_retirement = std::mem::replace(
                &mut *candidate.retirement.lock().unwrap(),
                retirement.clone(),
            );
            let old = entries.insert(
                self.key.clone(),
                Entry {
                    data: candidate.data.clone(),
                    mode: Mode::Native(instance, tasks.scope_identity()),
                },
            );
            drop(entries);
            drop(old);
            drop(old_retirement);
            return Ok(candidate);
        }
    }
    pub(super) fn claim_library(&self) -> io::Result<()> {
        let root = self.root.clone();
        let mut entries = root.plans.lock().unwrap();
        let entry = entries
            .get_mut(&self.key)
            .filter(|entry| Arc::ptr_eq(&entry.data, &self.data))
            .ok_or_else(revoked)?;
        match &entry.mode {
            Mode::Native(..)=>Err(conflict("actual Host must activate its canonical plugin before a library can borrow the ready Lease")),
            Mode::Cold=>{entry.mode=Mode::Library;Ok(())},
            Mode::Library=>Ok(()),
        }
    }
    pub(in crate::transport::plugin) fn bind_actual_actor(
        &self,
        actor: zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> io::Result<()> {
        let mut actual = self.actor.lock().unwrap();
        if let Some(previous) = actual.as_ref() {
            if !previous.same_registration(&actor) {
                return Err(conflict(
                    "canonical plugin retains a different actual actor",
                ));
            }
        } else {
            *actual = Some(actor);
        }
        Ok(())
    }
    fn untouched(&self) -> bool {
        if self.actor.lock().unwrap().is_some() {
            return false;
        }
        #[cfg(any(target_os = "linux", windows))]
        if self.pending_endpoint.lock().unwrap().is_some() {
            return false;
        }
        self.running
            .try_lock()
            .is_ok_and(|running| running.is_none())
    }
}
