//! Each actual accepted original keeps its own close capability and charge.
use super::*;
impl UnixOriginals {
    pub(super) fn before(
        &self,
        budget: &Arc<zero_runtime::configuration::ControlBudget>,
    ) -> io::Result<()> {
        let mut admission = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if admission.is_some() {
            return Err(io::Error::other("IPC retains an unknown native accept"));
        }
        *admission = Some(budget.reserve(zero_runtime::configuration::ControlCharge::Connection)?);
        Ok(())
    }
    pub(super) fn no_native(&self) {
        self.admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
    pub(super) async fn close(
        original: &UnixOriginal,
    ) -> io::Result<super::super::owner::ReportObservation> {
        let mut control = original.control.lock().await;
        control.cancel(CloseReason::Shutdown);
        let report = control
            .wait_closed(super::super::owner::close_deadline())
            .await
            .map_err(io::Error::other)?;
        let report = original
            .report_failures
            .receive("IPC accepted original", report);
        if report.complete {
            original
                .charge
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
        }
        // Fault delivery and native settlement remain independent. The caller
        // receives this same pure payload before removing the original row.
        Ok(report)
    }
    pub(super) async fn close_all(&self) -> io::Result<()> {
        let mut failures = Vec::new();
        let activating = self
            .activating
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(original) = activating {
            match Self::close(&original).await {
                Ok(report) => {
                    if report.complete {
                        self.activating
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take();
                    }
                    if let Err(error) = report.into_result() {
                        failures.push(error);
                    }
                }
                Err(error) => failures.push(error),
            }
        }
        let originals: Vec<_> = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(id, original)| (*id, original.clone()))
            .collect();
        for (id, original) in originals {
            match Self::close(&original).await {
                Ok(report) => {
                    if report.complete {
                        self.active
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .remove(&id);
                    }
                    if let Err(error) = report.into_result() {
                        failures.push(error);
                    }
                }
                Err(error) => failures.push(error),
            }
        }
        super::super::owner::finish_work(failures.into_iter().map(Err))
    }
}
impl UnixOriginals {
    pub(super) fn retain_terminal_faults(&self, failures: Vec<io::Error>) {
        // Only terminal unwind calls this, after accept has stopped. The
        // finite actual accepted originals bound these original cleanup faults.
        self.terminal_faults
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(failures.into_iter().map(Arc::new));
    }
    pub(super) fn terminal_faults(&self) -> io::Result<()> {
        let failures = self
            .terminal_faults
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        super::super::owner::finish_work(
            failures
                .into_iter()
                .map(|error| Err(io::Error::new(error.kind(), error))),
        )
    }
}
