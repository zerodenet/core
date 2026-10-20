//! Actual adapter state for mutual exclusion, never a connection cache.
use super::*;
use zero_core::{request::Destination, Error};
use zero_traits::DialPolicy;
use zero_transport::profile::{OwnedClientTlsProfile, OwnedGrpcProfile};
#[derive(PartialEq, Eq)]
pub(super) struct Recipe {
    pub(super) tag: String,
    pub(super) endpoint: Destination,
    pub(super) server: String,
    pub(super) uuid: [u8; 16],
    pub(super) cipher: crate::VmessCipher,
    pub(super) tls: Option<OwnedClientTlsProfile>,
    pub(super) grpc: OwnedGrpcProfile,
    pub(super) source_dir: Option<std::path::PathBuf>,
    pub(super) ca: Option<(std::path::PathBuf, usize)>,
    pub(super) dial: DialPolicy,
    pub(super) cap: std::num::NonZeroUsize,
    pub(super) business: Option<super::options::Business>,
}
#[derive(Default)]
struct Values {
    recipe: Option<(Recipe, Arc<[u8]>)>,
    mode: Mode,
}
#[derive(Default)]
enum Mode {
    #[default]
    Pristine,
    // Negative evidence only: old AbortHandles/Drop do not prove closure.
    LegacyExposed,
    Claimed(ComponentInstanceId),
}
#[derive(Default)]
pub(crate) struct State(Mutex<Values>);
impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VmessGrpcCarrierAdmission")
            .finish_non_exhaustive()
    }
}
impl State {
    pub(super) fn recipe(&self, recipe: Recipe) -> Result<Arc<[u8]>, Error> {
        let mut values = self.0.lock().unwrap();
        if let Some((existing, identity)) = &values.recipe {
            if existing != &recipe {
                return Err(Error::Config(
                    "conflicting complete VMess gRPC carrier recipe",
                ));
            }
            return Ok(identity.clone());
        }
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let mut previous = NEXT.load(Ordering::Relaxed);
        let next = loop {
            let next = previous
                .checked_add(1)
                .ok_or(Error::Config("VMess gRPC recipe identity exhausted"))?;
            match NEXT.compare_exchange_weak(previous, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break next,
                Err(actual) => previous = actual,
            }
        };
        let identity: Arc<[u8]> = next.to_le_bytes().into();
        values.recipe = Some((recipe, identity.clone()));
        Ok(identity)
    }
    pub(crate) fn enter_legacy(&self) -> Result<(), zero_transport::RuntimeError> {
        let mut values = self.0.lock().unwrap();
        match values.mode {
            Mode::Pristine | Mode::LegacyExposed => {
                values.mode = Mode::LegacyExposed;
                Ok(())
            }
            Mode::Claimed(instance) => Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                error(instance, Stage::Prepare, Reason::StalePlan),
            )
            .into()),
        }
    }
    pub(super) fn claim(&self, instance: ComponentInstanceId) -> Result<(), ChannelError> {
        let mut values = self.0.lock().unwrap();
        if !matches!(values.mode, Mode::Pristine) {
            return Err(error(instance, Stage::Prepare, Reason::StalePlan));
        }
        values.mode = Mode::Claimed(instance);
        Ok(())
    }
    pub(super) fn current(&self, instance: ComponentInstanceId) -> bool {
        matches!(self.0.lock().unwrap().mode, Mode::Claimed(actual) if actual == instance)
    }
}
