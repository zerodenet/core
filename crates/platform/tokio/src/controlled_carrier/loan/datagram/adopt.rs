use super::*;
use crate::{
    contract::{
        network::{CarrierContext, UpstreamBinding},
        ChannelError, DeliveryState, Reason, Stage,
    },
    controlled_carrier::{error, slot::Slot, CarrierDriver, CarrierGrant},
};

pub struct BorrowedDatagram {
    io: LoanDatagramIo,
    slot: Arc<Slot>,
}
impl BorrowedDatagram {
    /// A domain's eventual family is unknown here; retain conservative IPv6
    /// packet overhead without inventing a native peer or local socket.
    pub fn ipv6_overhead(&self) -> bool {
        match &self.io.endpoint().address {
            zero_core::Address::Ipv4(_) => false,
            zero_core::Address::Ipv6(_) => true,
            zero_core::Address::Domain(host) => !matches!(
                zero_core::address::parse_ip_literal(host),
                Some(std::net::IpAddr::V4(_))
            ),
        }
    }
    pub async fn send(&self, bytes: &[u8]) -> io::Result<()> {
        self.io.send(bytes).await
    }
    pub async fn receive(&self, bytes: &mut [u8]) -> io::Result<usize> {
        self.io.receive(bytes).await
    }
    pub fn into_driver(self) -> CarrierDriver<Self> {
        CarrierDriver {
            slot: self.slot.clone(),
            io: self,
        }
    }
}
impl Drop for BorrowedDatagram {
    fn drop(&mut self) {
        self.slot.state().view_live = false;
        self.slot.cancel.cancel();
        self.slot.notify();
    }
}
impl CarrierGrant {
    pub fn adopt_datagram_loan(
        mut self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        context: &CarrierContext,
        loan: DatagramLoan,
    ) -> Result<BorrowedDatagram, ChannelError> {
        let original = loan.owner.original().clone();
        {
            let mut state = self.slot.state();
            assert!(
                state.control.is_none() && state.packet_owner.is_none() && state.loan.is_none(),
                "sole unused carrier grant"
            );
            state.loan = Some(Arc::new(Mutex::new(loan.owner)));
            state.loan_origin = Some(original.clone());
            state.binding = Some(binding.clone());
            state.endpoint = Some(endpoint.clone());
            state.view_live = true;
        }
        crate::controlled_carrier::users::acquired(&self.slot);
        self.transferred = true;
        let io = BorrowedDatagram {
            io: loan.io,
            slot: self.slot.clone(),
        };
        let bound = self
            .slot
            .owner
            .upgrade()
            .and_then(|owner| owner.state().binding.clone());
        if binding.prefix.is_empty()
            || !io.io.targets(&endpoint)
            || context.session.is_some()
            || context.cancel.is_cancelled()
            || context
                .deadline
                .is_some_and(|end| tokio::time::Instant::now() >= end)
            || self.slot.cancel.is_cancelled()
            || original.is_retired()
            || bound.is_none_or(|bound| {
                bound.pool_kind() != Some(true)
                    || !bound.permits_native(binding, &endpoint, context)
            })
        {
            return Err(error(
                self.instance(),
                Stage::Dial,
                Reason::StalePlan,
                DeliveryState::Unknown,
            ));
        }
        Ok(io)
    }
}
