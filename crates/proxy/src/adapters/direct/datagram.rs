//! Direct delegates only to its bound upstream, retaining that session's owner.
use super::factory::{during_open, Outbound};
use std::pin::Pin;
use zero_platform_tokio::contract::{
    context::{with_deadline, ConnectFact, OpenContext},
    datagram::{DatagramOutbound, DatagramRequest, DatagramRx, DatagramSession, DatagramTx},
    lifecycle::{ChannelControl, CloseReport},
    network::CarrierContext,
    ChannelError, CloseReason, DeliveryState, Reason, Stage,
};
pub(super) struct Control(zero_platform_tokio::contract::lifecycle::BoxChannelControl);
impl ChannelControl for Control {
    fn cancel(&self, reason: CloseReason) {
        self.0.cancel(reason);
    }
    async fn wait_closed(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.0.wait_closed(deadline).await
    }
}
impl DatagramOutbound for Outbound {
    type Tx = Pin<Box<dyn DatagramTx>>;
    type Rx = Pin<Box<dyn DatagramRx>>;
    type Control = Control;
    async fn open_carrier(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<zero_platform_tokio::controlled_carrier::loan::DatagramLoan, ChannelError> {
        let session = self.state.check(&context)?;
        if session.is_some() {
            return Err(self.state.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        self.open_original(request, context).await
    }
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        let session = self.state.check(&context)?;
        let opened = if self.control.is_some() {
            // Transfer the existing original TX/RX/controller and its one
            // return barrier. No packet pump, native owner or executor is made.
            self.open_original(request, context.clone())
                .await
                .map_err(|source| self.state.attach(source, session))?
                .into_session()
                .0
        } else {
            // Explicit standalone library roles use the caller's own upstream;
            // production decoding always installs the controlled provider.
            if context.upstream.binding().dial != self.options.policy {
                return Err(self.state.error(
                    session,
                    Stage::Prepare,
                    Reason::StalePlan,
                    DeliveryState::NotSubmitted,
                ));
            }
            let carrier = CarrierContext {
                session,
                deadline: context.deadline,
                cancel: context.cancel.clone(),
            };
            tokio::select! {biased;
                _=self.state.cancelled()=>return Err(during_open(&self.state,session,Reason::Revoked)),
                _=context.cancel.cancelled()=>return Err(during_open(&self.state,session,Reason::Cancelled)),
                result=with_deadline(context.deadline,context.upstream.open_datagram(request,carrier))=>result.map_err(|source|during_open(&self.state,session,Reason::Timeout).with_source(source))?,
            }.map_err(|error|self.state.attach(error,session))?
        };
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
        }
        Ok(DatagramSession {
            tx: opened.tx,
            rx: opened.rx,
            control: Control(opened.control),
            limits: opened.limits,
        })
    }
}
impl Outbound {
    async fn open_original(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<zero_platform_tokio::controlled_carrier::loan::DatagramLoan, ChannelError> {
        let session = self.state.check(&context)?;
        if context.upstream.binding().dial != self.options.policy {
            return Err(self.state.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let zero_core::request::DatagramTargets::Fixed(destination) = request.targets else {
            return Err(self.state.error(
                session,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        };
        let control = self.control.as_ref().ok_or_else(|| {
            self.state.error(
                session,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            )
        })?;
        let opening = context.upstream.open_controlled_datagram_loan(
            zero_platform_tokio::contract::network::ControlledDatagramRequest {
                destination,
                association: request.association,
                context: CarrierContext {
                    session,
                    deadline: context.deadline,
                    cancel: context.cancel.clone(),
                },
                grants: control.grants.clone(),
                tasks: control.tasks.clone(),
                retirement: control.retirement,
            },
        );
        tokio::select! { biased;
            _ = self.state.cancelled() => Err(during_open(&self.state, session, Reason::Revoked)),
            _ = context.cancel.cancelled() => Err(during_open(&self.state, session, Reason::Cancelled)),
            result = with_deadline(context.deadline, opening) => result.map_err(|source| during_open(&self.state, session, Reason::Timeout).with_source(source))?,
        }
    }
}
