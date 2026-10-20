use super::*;
impl ControlledIcmpOwner {
    pub fn take_report(&mut self) -> IcmpOwnerReport {
        self.reap();
        let mut report = IcmpOwnerReport {
            first_raw_permission_source: self.first_raw_permission_source.clone(),
            first_acquisition_failure: self.first_acquisition_failure.clone(),
            first_close_failure: self.first_close_failure.clone(),
            confirmed_closed: std::mem::take(&mut self.confirmed_closed),
            known_no_native: std::mem::take(&mut self.known_no_native),
            ..Default::default()
        };
        for entry in self.entries.values() {
            match &*entry.native() {
                Native::Unknown(source) => {
                    let metadata = entry.metadata();
                    report.unconfirmed.push(UnconfirmedIcmp {
                        reservation: entry.id,
                        source: source.clone(),
                        acquisition_source: metadata.acquisition_source.clone(),
                        raw_permission_source: metadata.raw_permission_source.clone(),
                        close_source: metadata.close_source.clone(),
                    });
                }
                _ => report.retained_live += 1,
            }
        }
        report
    }
    /// Same-entry gates retire pending I/O first. Task joins and transferred
    /// users are separate host obligations and are never asserted here.
    pub fn close_all(&mut self) -> IcmpOwnerReport {
        self.revoke();
        for entry in self.entries.values() {
            let _ = entry.close();
            if let Some(source) = entry.metadata().close_source.clone() {
                self.first_close_failure.get_or_insert(source);
            }
        }
        self.take_report()
    }
}
