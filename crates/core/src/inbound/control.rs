use crate::{Error, SessionAuth};
use alloc::boxed::Box;
use core::{future::Future, pin::Pin};

/// A protocol-owned connection continuation with no application destination.
/// The protocol owns its wire state; runtime grants device admission, registers
/// policy cancellation when authenticated, and polls it within the listener lifetime.
/// Dropping the run future must close all resources registered by this session.
pub trait InboundControlSession: Send + 'static {
    fn auth(&self) -> Option<&SessionAuth>;
    fn run(self: Box<Self>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>;
}
