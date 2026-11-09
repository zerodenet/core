//! One real native close attempt without borrowing a request's nonexistent timer.
use super::*;
impl ControlledTcpControl {
    /// No wait or retry loop. A pending synchronous lease, known-open retry or
    /// unknown disposition remains with this exact original controller.
    pub fn try_close_once(&mut self) -> Result<CloseReport, ChannelError> {
        self.shared.cancel.cancel();
        match self.shared.close_action_until(None) {
            CloseAction::Original(socket) => {
                self.shared.close_returned(socket.close().map(|()| true))
            }
            CloseAction::Retry(source) => self
                .shared
                .close_returned(TokioSocket::retry_close(&source)),
            CloseAction::Fault(source) => return Err(self.shared.close_fault(source)),
            CloseAction::Unfinished => {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::Io(self.shared.label.clone())],
                    ..Default::default()
                })
            }
            CloseAction::Closed => {}
        }
        let (closed, fault) = {
            let state = self.shared.state();
            (
                state.closed,
                state.unknown.clone().or_else(|| state.error.clone()),
            )
        };
        if closed {
            if let Some(fault) = self.shared.unreceived_close_fault() {
                return Err(fault);
            }
            let mut report = CloseReport::default();
            if !self.reported {
                self.reported = true;
                report.closed_io.push(self.shared.label.clone());
            }
            Ok(report)
        } else if let Some(fault) = fault {
            Err(self.shared.close_fault(fault))
        } else {
            Ok(CloseReport {
                unfinished: vec![Unfinished::Io(self.shared.label.clone())],
                ..Default::default()
            })
        }
    }
}
