//! A same-Host fixed promise acquired before the first UDP/ICMP native IO.
use super::*;
use crate::contract::component::PacketDriverMetadataView;
pub struct MetadataIcmpMemory {
    root: PacketDriverMetadataView,
    credit: PacketMetadataCredit,
}
impl MetadataIcmpMemory {
    pub fn reserve(root: &PacketDriverMetadataView) -> Result<Self, ChannelError> {
        let layout = PacketMetadataArc::<Attempt>::layout()
            .and_then(|layout| {
                layout
                    .checked_add(IcmpAttemptObservation::metadata_layout()?)?
                    .checked_add(PacketMetadataArc::<
                        crate::controlled_icmp::IcmpAcquisitionError,
                    >::layout()?)
            })
            .map_err(|source| {
                error(Reason::Capacity, "ICMP attempt metadata layout overflow").with_source(source)
            })?;
        let credit = root.reserve(layout).map_err(|source| {
            error(Reason::Capacity, "ICMP attempt metadata refused").with_source(source)
        })?;
        Ok(Self {
            root: root.clone(),
            credit,
        })
    }
    pub(super) fn take(
        mut self,
        actual: &PacketDriverMetadataView,
    ) -> Result<
        (
            PacketMetadataCredit,
            IcmpAttemptObservation,
            PacketMetadataCredit,
        ),
        ChannelError,
    > {
        if !self.root.same_root(actual) {
            return Err(error(
                Reason::StalePlan,
                "ICMP attempt metadata belongs to a different Host",
            ));
        }
        let attempt = self
            .credit
            .split(PacketMetadataArc::<Attempt>::layout().expect("fixed attempt layout"))
            .expect("actual attempt prepaid");
        let observer = self
            .credit
            .split(
                IcmpAttemptObservation::metadata_layout().expect("fixed ICMP observation layout"),
            )
            .expect("actual observation prepaid");
        let observer = IcmpAttemptObservation::from_prepaid(observer)
            .unwrap_or_else(|_| panic!("exact observation prepaid"));
        let failure = self
            .credit
            .split(
                PacketMetadataArc::<crate::controlled_icmp::IcmpAcquisitionError>::layout()
                    .expect("fixed actual acquisition outcome"),
            )
            .expect("actual outcome prepaid");
        Ok((attempt, observer, failure))
    }
}
