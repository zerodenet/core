//! Actual accepted Source providers published with executable logical roles.
//! Only real Source callers, original resources and registered drivers enter it.
use super::{CarrierOrigin, CarrierOriginalTotals};
mod source;
use crate::{
    contract::{scope::TaskJoinWitness, ChannelError, Impact, Reason, Stage},
    AcceptedCaller, AcceptedResource,
};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use zero_core::component::ComponentInstanceId;
#[derive(Clone)]
struct Original {
    source: ComponentInstanceId,
    caller: AcceptedCaller,
    origin: Option<CarrierOrigin>,
    driver: TaskJoinWitness,
    resource: crate::AcceptedResourceObservation,
    control: Arc<AcceptedResource>,
    uses: Arc<std::sync::atomic::AtomicUsize>,
}
pub struct AcceptedCarrierProvider {
    instance: ComponentInstanceId,
    root: OnceLock<Weak<CarrierOriginalTotals>>,
    limit: std::num::NonZeroUsize,
    closing: std::sync::atomic::AtomicBool,
    returned: OnceLock<AcceptedSourcesReturned>,
    originals: Mutex<Vec<Original>>,
    retiring_sources: Mutex<std::collections::BTreeSet<ComponentInstanceId>>,
}
impl AcceptedCarrierProvider {
    /// Construction grants no IO, native close, task or Source identity.
    pub fn new(instance: ComponentInstanceId, limit: std::num::NonZeroUsize) -> Self {
        Self {
            instance,
            root: OnceLock::new(),
            limit,
            closing: std::sync::atomic::AtomicBool::new(false),
            returned: OnceLock::new(),
            originals: Mutex::new(Vec::new()),
            retiring_sources: Mutex::new(Default::default()),
        }
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    pub fn limit(&self) -> std::num::NonZeroUsize {
        self.limit
    }
    pub fn request_retirement(&self) {
        self.closing
            .store(true, std::sync::atomic::Ordering::Release);
    }
    fn settled(original: &Original) -> bool {
        original
            .caller
            .task_join_witness()
            .and_then(|caller| caller.try_joined())
            .is_some_and(|joined| joined.registration_released())
            && original
                .driver
                .try_joined()
                .is_some_and(|joined| joined.registration_released())
            && original.resource.confirmed_closed()
    }
    pub fn returned(&self) -> Option<&AcceptedSourcesReturned> {
        if self.root.get()?.upgrade().is_none()
            || !self.closing.load(std::sync::atomic::Ordering::Acquire)
        {
            return None;
        }
        let originals = self.originals.lock().unwrap().clone();
        if originals.iter().any(|original| !Self::settled(original)) {
            return None;
        }
        Some(self.returned.get_or_init(|| AcceptedSourcesReturned {
            instance: self.instance,
        }))
    }
    pub fn sources(&self) -> Vec<ComponentInstanceId> {
        self.originals
            .lock()
            .unwrap()
            .iter()
            .map(|row| row.source)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }
    pub fn originals(&self) -> Vec<CarrierOrigin> {
        self.originals
            .lock()
            .unwrap()
            .iter()
            .filter_map(|original| original.origin.clone())
            .collect()
    }

    /// The actual Slot seals publication to its original host budget. Binding
    /// cannot change generations or authorize a foreign accepted Source.
    pub fn bind_host(
        &self,
        instance: ComponentInstanceId,
        root: &Arc<CarrierOriginalTotals>,
    ) -> Result<(), ChannelError> {
        if instance != self.instance {
            return Err(failure());
        }
        let originals = self.originals.lock().unwrap().clone();
        if originals
            .iter()
            .any(|original| !original.caller.belongs_to_original_root(root))
        {
            return Err(failure());
        }
        if let Some(actual) = self.root.get() {
            if !actual.ptr_eq(&Arc::downgrade(root)) {
                return Err(failure());
            }
            return Ok(());
        }
        self.root.set(Arc::downgrade(root)).map_err(|_| failure())
    }
    pub fn shares_host(&self, other: &Self) -> bool {
        match (self.root.get(), other.root.get()) {
            (Some(actual), Some(other)) => actual.ptr_eq(other),
            _ => false,
        }
    }
    pub fn capacity(&self) -> Option<usize> {
        Some(self.root.get()?.upgrade()?.snapshot().limit.get())
    }
    /// Install before this exact driver is polled. The actual accepted resource
    /// remains in its Source graph. This retains that same logical controller;
    /// it creates no native owner, task registration or second cleanup ledger.
    pub fn retain(
        &self,
        caller: AcceptedCaller,
        origin: Option<CarrierOrigin>,
        driver: TaskJoinWitness,
        resource: &Arc<AcceptedResource>,
    ) -> Result<(), ChannelError> {
        caller.check_open()?;
        let source = caller.source_instance()?;
        if origin.is_none() {
            return Err(failure());
        }
        let retiring = self.retiring_sources.lock().unwrap();
        if retiring.contains(&source) {
            return Err(failure());
        }
        if self.root.get().is_none() {
            return Err(failure());
        }
        let actual = caller.carrier_origin()?;
        if !match (&actual, &origin) {
            (Some(actual), Some(origin)) => actual.same_original(origin),
            (None, None) => true,
            _ => false,
        } {
            return Err(failure());
        }
        if !resource.belongs_to(&caller)
            || !resource.names_driver(&driver)
            || driver.is_joined()
            || !driver.scope_identity().same_scope(&caller.scope_identity())
        {
            return Err(failure());
        }
        if let Some(root) = self.root.get() {
            let root = root.upgrade().ok_or_else(failure)?;
            if !caller.belongs_to_original_root(&root) {
                return Err(failure());
            }
        }
        // Inspect original receipt books outside the publication lock. Retired
        // rows are removed only by their exact registered driver identity.
        let snapshot = self.originals.lock().unwrap().clone();
        let settled = snapshot
            .into_iter()
            .filter(Self::settled)
            .map(|original| original.driver)
            .collect::<Vec<_>>();
        let mut originals = self.originals.lock().unwrap();
        let mut retired = Vec::new();
        let mut index = 0;
        while index < originals.len() {
            if settled
                .iter()
                .any(|driver| driver.same_registration(&originals[index].driver))
            {
                retired.push(originals.remove(index));
            } else {
                index += 1;
            }
        }
        let duplicate = originals.iter().any(|original| {
            original.driver.task_id() == driver.task_id()
                && original
                    .driver
                    .scope_identity()
                    .same_scope(&driver.scope_identity())
        });
        if duplicate
            || self.closing.load(std::sync::atomic::Ordering::Acquire)
            || originals.len() >= self.limit.get()
        {
            drop(originals);
            drop(retired);
            return Err(failure());
        }
        originals.push(Original {
            source,
            caller,
            origin,
            driver,
            resource: resource.observation(),
            control: resource.clone(),
            uses: Arc::default(),
        });
        drop(originals);
        drop(retiring);
        drop(retired);
        Ok(())
    }
    /// The token charges this exact retained driver row, never a supplied counter.
    pub fn borrow_driver(
        &self,
        driver: &TaskJoinWitness,
    ) -> Result<AcceptedCarrierUse, ChannelError> {
        let retiring = self.retiring_sources.lock().unwrap();
        let originals = self.originals.lock().unwrap();
        let row = originals
            .iter()
            .find(|row| row.driver.same_registration(driver))
            .ok_or_else(failure)?;
        if self.closing.load(std::sync::atomic::Ordering::Acquire)
            || driver.is_joined()
            || row.resource.confirmed_closed()
            || row.caller.check_open().is_err()
            || row
                .origin
                .as_ref()
                .is_none_or(|origin| origin.is_retired() || retiring.contains(&row.source))
        {
            return Err(failure());
        }
        let mut state = row.uses.load(std::sync::atomic::Ordering::Acquire);
        loop {
            if state >= USE_COUNT_MASK {
                return Err(failure());
            }
            match row.uses.compare_exchange_weak(
                state,
                state + 1,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => state = actual,
            }
        }
        Ok(AcceptedCarrierUse(row.uses.clone()))
    }
    pub fn driver_uses(&self, driver: &TaskJoinWitness) -> Option<AcceptedCarrierUses> {
        self.originals
            .lock()
            .unwrap()
            .iter()
            .find(|row| row.driver.same_registration(driver))
            .map(|row| AcceptedCarrierUses(row.uses.clone()))
    }
    pub fn contains(&self, origin: &CarrierOrigin) -> bool {
        let Some(root) = self.root.get().and_then(Weak::upgrade) else {
            return false;
        };
        let retiring = self.retiring_sources.lock().unwrap().clone();
        let originals = self.originals.lock().unwrap().clone();
        originals.iter().any(|original| {
            original
                .origin
                .as_ref()
                .is_some_and(|actual| actual.same_original(origin))
                && !retiring.contains(&original.source)
                && original.caller.belongs_to_original_root(&root)
                && original.caller.check_open().is_ok()
                && !original.driver.is_joined()
                && !original.resource.confirmed_closed()
        })
    }
}
fn failure() -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Component)
}

/// Only this provider's real Source resource return and original driver ACKs.
/// This is not native close of their independently owned socket/metadata parent.
pub struct AcceptedSourcesReturned {
    instance: ComponentInstanceId,
}
impl AcceptedSourcesReturned {
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
}

/// Actual logical IO custody of one retained accepted driver. Drop returns only
/// this use; neither Source/native close nor Scope/registration ACK is implied.
pub struct AcceptedCarrierUse(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for AcceptedCarrierUse {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

const USE_STOP: usize = 1usize << (usize::BITS - 1);
const USE_COUNT_MASK: usize = USE_STOP - 1;
/// Sealed logical custody/admission control of this same original driver's uses.
/// It contains no native IO/controller, task owner or native-close authority.
#[derive(Clone)]
pub struct AcceptedCarrierUses(Arc<std::sync::atomic::AtomicUsize>);
impl AcceptedCarrierUses {
    pub fn request_stop(&self) {
        self.0
            .fetch_or(USE_STOP, std::sync::atomic::Ordering::AcqRel);
    }
    pub fn returned(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Acquire) & USE_COUNT_MASK == 0
    }
}
