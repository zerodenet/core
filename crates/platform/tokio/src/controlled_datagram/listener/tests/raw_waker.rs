use super::*;
use std::{
    mem::ManuallyDrop,
    sync::{atomic::AtomicBool, Weak},
    task::{RawWaker, RawWakerVTable},
};
mod close;
mod native;
mod packet;
mod probe;
use probe::*;
fn exhaust() {
    for _ in 0..1024 {
        let mut budget = Box::pin(tokio::task::coop::consume_budget());
        if budget
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
        {
            assert!(!tokio::task::coop::has_budget_remaining());
            return;
        }
    }
    panic!("actual Tokio budget must exhaust without a scheduler turn");
}

// Handle::block_on supplies a real I/O runtime and cooperative budget, while
// deliberately leaving SchedulerContext absent. No reactor turn is needed for
// these cancellation-only cases: the original bound socket must suspend first.
// Positive packet I/O cases keep #[tokio::test] and its actual reactor driver.
fn synchronous_coop(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.handle().block_on(test);
}
