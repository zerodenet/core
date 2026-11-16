//! Exact physical-wire closure, separate from the retained protocol Root.
use super::*;
#[derive(Clone)]
pub struct MetadataWireClosed {
    original: MetadataPoolOrigin,
    report: Arc<MetadataOwnerReport>,
    acquired: bool,
}
impl MetadataWireClosed {
    pub fn matches(&self, original: &MetadataPoolOrigin) -> bool {
        self.original.same_original(original)
    }
    pub fn had_native_acquisition(&self) -> bool {
        self.acquired
    }
    pub fn is_complete(&self) -> bool {
        self.report.is_complete()
    }
}
impl MetadataPoolOrigin {
    pub fn native_wire_closed(&self) -> Option<MetadataWireClosed> {
        let slot = self.slot.upgrade()?;
        let native = slot.native().ok()?;
        let report = native
            .report
            .as_ref()
            .filter(|report| native.wire_closed && report.is_complete())?
            .clone();
        Some(MetadataWireClosed {
            original: self.clone(),
            report,
            acquired: native.acquired_confirmed,
        })
    }
}
