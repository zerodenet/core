//! Explicit unscoped unit-library execution; never a production Source fallback.
use super::*;
pub(super) fn unscoped_native(
    execution: &crate::protocol_registry::TcpExecutionServices,
    io: &Arc<datagram::DatagramConnection>,
    closed: &Arc<AtomicBool>,
) -> Option<LogicalConnection> {
    if execution.invocation.as_ref().is_some_and(|call| {
        let tracked = call.caller_accepted.is_some() || call.caller_probe.is_some();
        #[cfg(feature = "raw-ip-runtime")]
        let tracked = tracked || call.caller_native.is_some();
        tracked
    }) {
        return None;
    }
    // Preserve the pre-existing standalone protocol/dispatcher fixtures. Any
    // actual Source caller always goes through NativeReceiver::prepare; its
    // validation errors are never converted to this unscoped fixture.
    Some(LogicalConnection(Arc::new(Connection {
        io: ConnectionIo::Datagram(io.clone()),
        reader: super::super::super::response_join::ResponseTask::new(),
        native: None,
        closed: closed.clone(),
    })))
}
