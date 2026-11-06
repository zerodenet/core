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
    /// Re-transfer this actual receiver's unique Tx/Rx and user lease, while
    /// its original donor controller remains retained in the same child slot.
    pub fn into_datagram_loan(
        mut self,
        view: crate::controlled_carrier::CarrierUserView,
        association: AssociationId,
    ) -> Result<DatagramLoan, ChannelError> {
        let adoption = crate::controlled_carrier::CarrierAdoption {
            slot: self.slot.clone(),
        };
        if !view.matches(&adoption) {
            return Err(error(
                self.slot.instance,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            ));
        }
        let original = adoption.transferred_origin()?;
        let endpoint = self.io.endpoint.clone();
        let limits = self.io.limits.clone();
        let tx = self.io.tx.get_mut().take().expect("live packet sender");
        let rx = self.io.rx.get_mut().take().expect("live packet receiver");
        let custody = Arc::new(Transferred {
            _original: self,
            _view: view,
        });
        let session = crate::contract::datagram::DatagramSession {
            tx: Box::pin(TransferredTx {
                inner: tx,
                _custody: custody.clone(),
            }) as Tx,
            rx: Box::pin(TransferredRx {
                inner: rx,
                _custody: custody,
            }) as Rx,
            control: Box::new(super::super::native::NativeControl(adoption))
                as Box<dyn crate::contract::lifecycle::DynChannelControl>,
            limits,
        };
        Ok(DatagramLoan::new(session, original, association, endpoint))
    }
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
// The original views are dropped before either shared custody reference. Only
// the final actual Tx/Rx return drops BorrowedDatagram and its original loan IO.
struct Transferred {
    _original: BorrowedDatagram,
    _view: crate::controlled_carrier::CarrierUserView,
}
struct TransferredTx {
    inner: Tx,
    _custody: Arc<Transferred>,
}
struct TransferredRx {
    inner: Rx,
    _custody: Arc<Transferred>,
}
impl DatagramTx for TransferredTx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        frame: &zero_core::packet::Datagram,
    ) -> std::task::Poll<Result<crate::contract::SendReceipt, crate::contract::SendFailure>> {
        self.get_mut().inner.as_mut().poll_send(cx, frame)
    }
}
impl DatagramRx for TransferredRx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<zero_core::packet::Datagram>, ChannelError>> {
        self.get_mut().inner.as_mut().poll_recv(cx)
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
                let related = self
                    .slot
                    .state()
                    .related_parent
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade);
                if let Some(parent) = related {
                    !bound.permits_related()
                        || crate::controlled_carrier::related::check_parent(
                            &crate::controlled_carrier::CarrierAdoption { slot: parent },
                            self.instance(),
                            binding,
                        )
                        .is_err()
                } else {
                    bound.pool_kind() != Some(true)
                        || !bound.permits_native(binding, &endpoint, context)
                }
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
