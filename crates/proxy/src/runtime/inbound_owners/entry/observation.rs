//! A disposition travels with the existing real supervisor error message.
use super::*;
use std::sync::atomic::AtomicU8;
const UNREPORTED: u8 = 0;
const PUBLISHED: u8 = 1;
const OBSERVED: u8 = 2;
const COMPLETE: u8 = 3;
#[derive(Debug)]
pub(super) struct ObservationDisposition {
    identity: ComponentInstanceId,
    phase: AtomicU8,
}
impl ObservationDisposition {
    pub(super) fn new(identity: ComponentInstanceId) -> Self {
        Self {
            identity,
            phase: AtomicU8::new(UNREPORTED),
        }
    }
    pub(super) fn publish(&self) {
        let mut phase = self.phase.load(Ordering::Acquire);
        loop {
            let next = match phase {
                UNREPORTED => PUBLISHED,
                OBSERVED => COMPLETE,
                _ => return,
            };
            match self
                .phase
                .compare_exchange_weak(phase, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(actual) => phase = actual,
            }
        }
    }
    fn consume(&self) -> bool {
        let mut phase = self.phase.load(Ordering::Acquire);
        loop {
            let (next, reported) = match phase {
                UNREPORTED => (OBSERVED, false),
                PUBLISHED => (COMPLETE, true),
                OBSERVED | COMPLETE => return true,
                _ => unreachable!("private observation phase"),
            };
            match self
                .phase
                .compare_exchange_weak(phase, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return reported,
                Err(actual) => phase = actual,
            }
        }
    }
}
#[derive(Debug)]
struct ObservedInboundError {
    identity: ComponentInstanceId,
    disposition: Arc<ObservationDisposition>,
    source: EngineError,
}
impl std::fmt::Display for ObservedInboundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.source, f)
    }
}
impl std::error::Error for ObservedInboundError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl InboundOwner {
    pub(crate) fn observer_error(&self, source: EngineError) -> EngineError {
        std::io::Error::other(ObservedInboundError {
            identity: self.identity,
            disposition: self.observation.clone(),
            source,
        })
        .into()
    }
}
/// False is the first unreported real failure: the supervisor must retain it.
/// True acknowledges only this exact host-issued instance's existing message.
pub(crate) fn consume_reported_error(error: &EngineError) -> bool {
    let EngineError::Io(error) = error else {
        return false;
    };
    let Some(observed) = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<ObservedInboundError>())
    else {
        return false;
    };
    observed.identity == observed.disposition.identity && observed.disposition.consume()
}
