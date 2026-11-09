//! The actual std-thread unwind retains independent native drain faults too.
use super::*;
type Payload = Box<dyn std::any::Any + Send>;
struct PanicAndDrain {
    original: Payload,
    drain: Arc<io::Error>,
}
pub(super) fn resume(original: Payload, drain: io::Result<()>) -> ! {
    match drain {
        Ok(()) => std::panic::resume_unwind(original),
        Err(drain) => std::panic::resume_unwind(Box::new(PanicAndDrain {
            original,
            drain: Arc::new(drain),
        })),
    }
}
pub(in crate::controlled_pipe) fn split(payload: Payload) -> (Payload, Option<Arc<io::Error>>) {
    match payload.downcast::<PanicAndDrain>() {
        Ok(actual) => (actual.original, Some(actual.drain)),
        Err(original) => (original, None),
    }
}
#[cfg(test)]
impl Native {
    pub(in crate::controlled_pipe) fn retain_fault_for_test(&mut self, source: Arc<io::Error>) {
        self.faults.push(source);
    }
}
