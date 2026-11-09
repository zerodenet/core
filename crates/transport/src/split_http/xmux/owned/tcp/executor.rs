//! Hyper delegates execute only in that same actual TCP driver ledger.
use super::*;
#[derive(Clone)]
pub(super) struct Executor(pub(super) PacketMetadataWeak<State>);
impl<F> hyper::rt::Executor<F> for Executor
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    fn execute(&self, future: F) {
        let Some(state) = self.0.upgrade() else {
            return;
        };
        state.delegates.fetch_add(1, Ordering::AcqRel);
        let returned = DelegateReturn {
            state: PacketMetadataArc::downgrade(&state),
        };
        let work = PaidFuture::prepare_delegate(future, &state.runtime.memory, returned)
            .map(Work::Http2Delegate)
            .and_then(|work| state.runtime.ledger.push(work));
        if let Err(source) = work {
            state.causes.record(0, source);
            state.changed.notify_waiters();
        }
    }
}
