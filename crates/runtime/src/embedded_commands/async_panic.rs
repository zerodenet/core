//! A panic caught while polling the original frontend continuation is not a
//! Tokio blocking failure or TaskJoined. Retain its actual opaque payload.
use super::*;
pub struct AsyncCommandPanic(Mutex<Box<dyn std::any::Any + Send>>);
impl AsyncCommandPanic {
    pub fn is<T: std::any::Any>(&self) -> bool {
        lock(&self.0).is::<T>()
    }
}
impl std::fmt::Debug for AsyncCommandPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalAsyncCommandPanic")
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for AsyncCommandPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let payload = lock(&self.0);
        if let Some(value) = payload.downcast_ref::<&str>() {
            f.write_str(value)
        } else if let Some(value) = payload.downcast_ref::<String>() {
            f.write_str(value)
        } else {
            f.write_str("original non-string async command panic")
        }
    }
}
impl std::error::Error for AsyncCommandPanic {}
pub(super) fn caught(payload: Box<dyn std::any::Any + Send>) -> Arc<ChannelError> {
    Arc::new(fault(Reason::Internal).with_source(AsyncCommandPanic(Mutex::new(payload))))
}
