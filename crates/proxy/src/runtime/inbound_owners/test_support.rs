//! Assertions about the actual tracked exit, including its original shared source.
use super::HostPeerBinding;
use std::{error::Error, sync::Arc};
use zero_core::component::TaskId;
use zero_platform_tokio::contract::{scope::TaskClass, ChannelError};

pub(crate) fn assert_tracked_panic(
    binding: &HostPeerBinding,
    task: TaskId,
    class: TaskClass,
    forwarded: &ChannelError,
) {
    let witness = binding.join_witness().expect("actual tracked registration");
    let joined = witness
        .try_joined()
        .expect("original Scope consumed this task");
    assert_eq!(joined.id(), task);
    assert_eq!(joined.class(), class);
    let original = joined.result().expect_err("original task panicked");
    let observed_again = witness.try_joined().unwrap().result().unwrap_err();
    assert!(Arc::ptr_eq(&original, &observed_again));
    // The host's retained forwarding error and the witness's original error
    // are different layers, but the former must still reach this exact Arc.
    assert!(contains(forwarded, 0, &|error| {
        error
            .downcast_ref::<ChannelError>()
            .is_some_and(|error| std::ptr::eq(error, original.as_ref()))
    }));
    assert!(contains(original.as_ref(), 0, &|error| {
        error
            .downcast_ref::<tokio::task::JoinError>()
            .is_some_and(tokio::task::JoinError::is_panic)
    }));
}

fn contains(
    error: &(dyn Error + 'static),
    depth: usize,
    predicate: &impl Fn(&(dyn Error + 'static)) -> bool,
) -> bool {
    assert!(depth < 64, "bounded actual error chain");
    if predicate(error) {
        return true;
    }
    // io::Error::source can skip its boxed error; inspect the real box too.
    if let Some(inner) = error
        .downcast_ref::<std::io::Error>()
        .and_then(std::io::Error::get_ref)
    {
        if contains(inner, depth + 1, predicate) {
            return true;
        }
    }
    error
        .source()
        .is_some_and(|inner| contains(inner, depth + 1, predicate))
}
