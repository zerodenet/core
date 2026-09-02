use super::command_can_run_concurrently;
use zero_api::{CommandRequest, DiagnosticsProbeOutboundCommand, ModeSetCommand};

#[test]
fn outbound_diagnostics_are_the_only_reordered_ipc_commands() {
    assert!(command_can_run_concurrently(
        &CommandRequest::DiagnosticsProbeOutbound(DiagnosticsProbeOutboundCommand::default())
    ));
    assert!(!command_can_run_concurrently(&CommandRequest::ModeSet(
        ModeSetCommand::default()
    )));
}
