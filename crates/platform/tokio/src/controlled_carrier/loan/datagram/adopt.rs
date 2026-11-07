use super::*;
mod transferred;
use crate::{
    contract::{
        network::{CarrierContext, UpstreamBinding},
        ChannelError, DeliveryState, Reason, Stage,
    },
    controlled_carrier::{error, slot::Slot, CarrierDriver, CarrierGrant},
};
use transferred::{Transferred, TransferredRx, TransferredTx};

pub struct BorrowedDatagram {
    io: LoanDatagramIo,
    slot: Arc<Slot>,
}
impl BorrowedDatagram {
    /// Observe this exact receiver slot; this grants no acquisition or closure.
    pub fn adoption(&self) -> crate::controlled_carrier::CarrierAdoption {
        crate::controlled_carrier::CarrierAdoption {
            slot: self.slot.clone(),
        }
    }
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
        let wire_association = self.io.wire_association;
        let tx = self.io.tx.get_mut().take().expect("live packet sender");
        let rx = self.io.rx.get_mut().take().expect("live packet receiver");
        let observer = self.io.observer.clone();
        let tx_fence = crate::controlled_carrier::related::ViewCancellation::new(&self.slot);
        let rx_fence = crate::controlled_carrier::related::ViewCancellation::new(&self.slot);
        let custody = Arc::new(Transferred {
            original: self,
            _view: view,
        });
        let session = crate::contract::datagram::DatagramSession {
            tx: Box::pin(TransferredTx {
                inner: tx,
                fence: tx_fence,
                send_pending: false,
                observer: observer.clone(),
                custody: custody.clone(),
            }) as Tx,
            rx: Box::pin(TransferredRx {
                inner: rx,
                fence: rx_fence,
                observer,
                custody: custody,
            }) as Rx,
            control: (Box::new(super::super::native::NativeControl(adoption.clone()))
                as Box<dyn crate::contract::lifecycle::DynChannelControl>)
                .into(),
            limits,
        };
        let mut loan = DatagramLoan::new(session, original, association, endpoint);
        // Same original Tx/Rx still expect their original wire association.
        // A new logical receiver never rewrites a borrowed immutable payload.
        loan.io.wire_association = wire_association;
        loan.owner.observe_native_control(adoption)?;
        Ok(loan)
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
        let parent = self.slot.related_cancellation();
        let related = async {
            match parent {
                Some(parent) => parent.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! { biased;
            _ = self.slot.cancel.cancelled() => Err(io::ErrorKind::Interrupted.into()),
            _ = related => Err(io::ErrorKind::Interrupted.into()),
            result = self.io.send(bytes) => result,
        }
    }
    pub async fn receive(&self, bytes: &mut [u8]) -> io::Result<usize> {
        let parent = self.slot.related_cancellation();
        let related = async {
            match parent {
                Some(parent) => parent.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! { biased;
            _ = self.slot.cancel.cancelled() => Err(io::ErrorKind::Interrupted.into()),
            _ = related => Err(io::ErrorKind::Interrupted.into()),
            result = self.io.receive(bytes) => result,
        }
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
        self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        context: &CarrierContext,
        loan: DatagramLoan,
    ) -> Result<BorrowedDatagram, ChannelError> {
        self.adopt_datagram_loan_inner(binding, endpoint, context, loan, false, |_| Ok(None))
    }
    /// The original per-connection Service adopts its actual strictly shorter
    /// Datagram prefix. This grants no native socket or cached-driver custody.
    pub fn adopt_service_datagram_loan(
        self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        context: &CarrierContext,
        loan: DatagramLoan,
    ) -> Result<BorrowedDatagram, ChannelError> {
        self.adopt_datagram_loan_inner(binding, endpoint, context, loan, true, |_| Ok(None))
    }
    pub(in crate::controlled_carrier::loan::datagram) fn adopt_service_target_loan<F>(
        self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        context: &CarrierContext,
        loan: DatagramLoan,
        selected: F,
    ) -> Result<BorrowedDatagram, ChannelError>
    where
        F: FnOnce(&CarrierGrant) -> Result<Destination, ChannelError>,
    {
        self.adopt_datagram_loan_inner(binding, endpoint, context, loan, true, |grant| {
            selected(grant).map(Some)
        })
    }
    fn adopt_datagram_loan_inner<F>(
        mut self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        context: &CarrierContext,
        loan: DatagramLoan,
        service: bool,
        selected: F,
    ) -> Result<BorrowedDatagram, ChannelError>
    where
        F: FnOnce(&CarrierGrant) -> Result<Option<Destination>, ChannelError>,
    {
        let original = loan.owner.original().clone();
        let related = loan.owner.related_control();
        {
            let mut state = self.slot.state();
            assert!(
                state.control.is_none() && state.packet_owner.is_none() && state.loan.is_none(),
                "sole unused carrier grant"
            );
            state.loan = Some(super::super::receiver::install_owner(
                &self.slot, loan.owner,
            ));
            state.loan_origin = Some(original.clone());
            state.loan_related = related;
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
        // The unique lower controller and view are already installed above.
        // A rejected selected target must not drop its actual LoanOwner before
        // Root custody, or claim no-submission after a donor opened its wire.
        let selected = selected(&self).map_err(|mut source| {
            source.stage = Stage::Dial;
            source.delivery = DeliveryState::Unknown;
            source.retry = crate::contract::RetryClass::Never;
            source
        })?;
        let wire_target = selected.as_ref().unwrap_or(&endpoint);
        if binding.prefix.is_empty()
            || !io.io.targets(wire_target)
            || context.session.is_some()
            || context.cancel.is_cancelled()
            || context
                .deadline
                .is_some_and(|end| tokio::time::Instant::now() >= end)
            || self.slot.acquisition_sealed()
            || original.is_retired()
            || bound.is_none_or(|bound| {
                let related = self
                    .slot
                    .state()
                    .related_parent
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade);
                if service {
                    !bound.permits_service_datagram_loan(binding, &endpoint, context)
                } else if let Some(parent) = related {
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
