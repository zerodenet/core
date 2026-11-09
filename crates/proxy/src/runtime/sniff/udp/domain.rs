//! The next original only: later Ready packets never consume its Domain ticket.
use super::*;
use zero_platform_tokio::contract::component::PacketDriverMetadataView;
use zero_traits::packet_metadata::{PacketMemoryError, PacketMetadataCredit};
pub(super) struct Held {
    dispatch: Option<InboundUdpDispatch>,
    pub(super) decision: Decision,
    applied: bool,
    // In case of any unexpected attach refusal, the constructed original and
    // this actual credit remain together; no unpaid String or original escapes.
    credit: Option<PacketMetadataCredit>,
}
impl Held {
    pub(super) fn new(dispatch: InboundUdpDispatch, decision: Decision) -> Self {
        Self {
            dispatch: Some(dispatch),
            decision,
            applied: false,
            credit: None,
        }
    }
    pub(super) fn prepare(
        &mut self,
        memory: Option<&PacketDriverMetadataView>,
        policy: &SniffingPolicy,
    ) -> Result<(), PacketMemoryError> {
        let Decision::Domain {
            name,
            protocol,
            source,
            fake_dns_fallback,
        } = self.decision
        else {
            return Ok(());
        };
        let route_only =
            policy.route_only() && protocol != SniffedProtocol::FakeDns && !fake_dns_fallback;
        if let Some(memory) = memory {
            if self.credit.is_none() {
                let layout = self
                    .dispatch
                    .as_ref()
                    .expect("same original queued packet")
                    .sniffed_domain_metadata_layout(name.as_str().len(), route_only)?;
                self.credit = Some(memory.reserve(layout)?);
            }
        }
        let mut original = self.dispatch.take().expect("same original queued packet");
        if !self.applied {
            // Exact requested backing; the same allocation becomes String.
            let bytes = Box::<[u8]>::from(name.as_str().as_bytes()).into_vec();
            original = original.with_sniffed_domain(
                String::from_utf8(bytes).expect("same ASCII name"),
                source,
                route_only,
            );
            self.applied = true;
        }
        if let Some(credit) = self.credit.take() {
            original = match original.with_domain_metadata(credit) {
                Ok(original) => original,
                Err((source, original, credit)) => {
                    self.dispatch = Some(original);
                    self.credit = Some(credit);
                    return Err(source);
                }
            };
        }
        self.dispatch = Some(original);
        self.decision = Decision::Fallback;
        Ok(())
    }
    pub(super) fn into_dispatch(mut self) -> InboundUdpDispatch {
        assert!(
            matches!(self.decision, Decision::Fallback),
            "same Domain owner was prepared before delivery"
        );
        self.dispatch.take().expect("one actual delivered original")
    }
}
