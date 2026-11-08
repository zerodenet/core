//! Same bounded owner; installation is internal to a paid MetadataPool slot.
use super::super::super::metadata_pool::{MetadataOriginalNamespace, MetadataOriginalSources};
use super::*;
impl MetadataOwner {
    pub(in crate::controlled_datagram) fn install_close_sources(
        &mut self,
        sources: MetadataOriginalSources,
    ) {
        assert!(self.close_sources.is_none() && self.main.next == 0 && self.probes.owner.next == 0);
        self.probes.close_sources = Some(sources.clone());
        self.close_sources = Some(sources);
    }
    pub(super) fn capture_sources(&self) {
        if let Some(sources) = &self.close_sources {
            sources.capture(MetadataOriginalNamespace::Main, &self.main);
            sources.capture(MetadataOriginalNamespace::Probe, &self.probes.owner);
        }
    }
    pub(super) fn try_capture_sources(&self) {
        if let Some(sources) = &self.close_sources {
            sources.try_capture(MetadataOriginalNamespace::Main, &self.main);
            sources.try_capture(MetadataOriginalNamespace::Probe, &self.probes.owner);
        }
    }
    pub(super) fn capture_acquisition(&self, source: &DatagramAcquisitionError) {
        if let Some(sources) = &self.close_sources {
            if let Some(reservation) = source.reservation {
                sources.acquisition(MetadataOriginalNamespace::Main, reservation, &source.source);
                sources.mark_failed();
            }
        }
        self.capture_sources();
    }
}
