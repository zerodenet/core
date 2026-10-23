//! Partial native receipts and their original fault are separate obligations.
use super::*;
impl InboundOwner {
    pub(in crate::runtime::inbound_owners) fn record_native_progress(
        &self,
        identity: ComponentInstanceId,
        report: CloseReport,
        source: Option<SharedError>,
    ) {
        assert_eq!(
            identity,
            self.identity(),
            "same actual native resource origin"
        );
        self.report
            .lock()
            .expect("inbound close report poisoned")
            .append(report);
        if let Some(source) = source {
            self.failure
                .lock()
                .expect("inbound failure poisoned")
                .get_or_insert(source);
        }
    }
}
