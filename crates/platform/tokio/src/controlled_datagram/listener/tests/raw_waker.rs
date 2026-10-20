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
