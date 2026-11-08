//! Persist original sources before harvesting or replacing native reports.
use super::*;
impl MetadataOwner {
    /// Aggregate confirmed counts are committed before any new acquisition.
    /// Main and probe namespaces remain distinct in these actual owner reports.
    pub fn take_report(&mut self) -> MetadataOwnerReport {
        self.gather_report(false)
    }
    /// Revoke native I/O and drain its original gate before closing. This is
    /// native cleanup only: no Quinn user, task join or failure ACK is asserted.
    pub fn close_all(&mut self) -> MetadataOwnerReport {
        self.gather_report(true)
    }
    /// Extract and immediately persist this branch before calling another
    /// branch. Unknown originals and their original sources remain retained.
    pub fn take_main_report(&mut self) -> DatagramOwnerReport {
        self.capture_sources();
        self.pending_main
            .take()
            .unwrap_or_else(|| self.main.take_report())
    }
    pub fn take_probe_report(&mut self) -> DatagramOwnerReport {
        self.capture_sources();
        self.pending_probes
            .take()
            .unwrap_or_else(|| self.probes.owner.take_report())
    }
    /// None leaves all prior receipts/sources owned; an actual native gate or
    /// close lease is busy. No blocking second lookup follows a try-check.
    pub fn try_take_main_report(&mut self) -> Option<DatagramOwnerReport> {
        self.try_capture_sources();
        match self.pending_main.take() {
            Some(report) => Some(report),
            None => self.main.try_take_report(),
        }
    }
    pub fn try_take_probe_report(&mut self) -> Option<DatagramOwnerReport> {
        self.try_capture_sources();
        match self.pending_probes.take() {
            Some(report) => Some(report),
            None => self.probes.owner.try_take_report(),
        }
    }
    /// Main and probe native receipts are independent. Callers using these
    /// separate methods persist each result before the next operation.
    pub fn close_main(&mut self) -> DatagramOwnerReport {
        self.capture_sources();
        if let Some(report) = self.pending_main.take() {
            return report;
        }
        for shared in self.views.values() {
            shared.socket.entry.retired.cancel();
            shared.pollers.signal();
        }
        let report = self.main.close_all();
        self.capture_sources();
        report
    }
    pub fn close_probes(&mut self) -> DatagramOwnerReport {
        self.capture_sources();
        let report = self
            .pending_probes
            .take()
            .unwrap_or_else(|| self.probes.owner.close_all());
        self.capture_sources();
        report
    }
    /// None means an actual native call/close lease still owns its gate. No
    /// receipt is extracted; already completed originals remain in the ledger.
    pub fn try_close_main(&mut self) -> Option<DatagramOwnerReport> {
        self.try_capture_sources();
        if let Some(report) = self.pending_main.take() {
            return Some(report);
        }
        for shared in self.views.values() {
            shared.socket.entry.retired.cancel();
            shared.pollers.signal();
        }
        let report = try_close_owner(&mut self.main);
        if let (Some(sources), Some(report)) = (&self.close_sources, &report) {
            sources.report(
                super::super::super::metadata_pool::MetadataOriginalNamespace::Main,
                &self.main,
                report,
            );
        }
        report
    }
    pub fn try_close_probes(&mut self) -> Option<DatagramOwnerReport> {
        self.try_capture_sources();
        if let Some(report) = self.pending_probes.take() {
            return Some(report);
        }
        let report = try_close_owner(&mut self.probes.owner);
        if let (Some(sources), Some(report)) = (&self.close_sources, &report) {
            sources.report(
                super::super::super::metadata_pool::MetadataOriginalNamespace::Probe,
                &self.probes.owner,
                report,
            );
        }
        report
    }
    fn gather_report(&mut self, close: bool) -> MetadataOwnerReport {
        self.capture_sources();
        if self.pending_main.is_none() {
            let main = if close {
                self.close_main()
            } else {
                self.main.take_report()
            };
            self.pending_main = Some(main);
        }
        if self.pending_probes.is_none() {
            let probes = if close {
                self.probes.owner.close_all()
            } else {
                self.probes.owner.take_report()
            };
            self.pending_probes = Some(probes);
        }
        self.capture_sources();
        // A last-view/waker destructor can panic here. Both actual deltas
        // already belong to this persistent owner, rather than its stack.
        self.reap();
        MetadataOwnerReport {
            main: self.pending_main.take().expect("installed main report"),
            probes: self.pending_probes.take().expect("installed probe report"),
        }
    }
}

fn try_close_owner(owner: &mut ControlledDatagramOwner) -> Option<DatagramOwnerReport> {
    for entry in owner.entries.values() {
        let _result = entry.try_close()?;
    }
    owner.try_take_report()
}
