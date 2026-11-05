//! UDP ASSOCIATE is another logical user of the same native pool and driver.
use super::*;
use std::sync::Arc;
use zero_platform_tokio::contract::{
    context::{with_deadline, ConnectFact, OpenContext},
    datagram::{DatagramLimits, DatagramOutbound, DatagramRequest, DatagramSession},
};
mod control;
mod rx;
mod shared;
mod tx;

impl DatagramOutbound for Outbound {
    type Tx = tx::Tx;
    type Rx = rx::Rx;
    type Control = control::Control;
    async fn open_carrier(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<zero_platform_tokio::controlled_carrier::loan::DatagramLoan, ChannelError> {
        let zero_core::request::DatagramTargets::Fixed(endpoint) = request.targets.clone() else {
            return Err(self.admission.error(
                context.session_identity()?,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        };
        let association = request.association;
        let session = self.open(request, context).await?;
        let original = session.control.original().clone();
        Ok(
            zero_platform_tokio::controlled_carrier::loan::DatagramLoan::new(
                zero_platform_tokio::contract::datagram::erase_session(session),
                original,
                association,
                endpoint,
            ),
        )
    }
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        let session = self.admission.check(&context)?;
        let prepared = &self.options.prepared;
        let key = prepared.key();
        let (pool, owner) = self
            .options
            .actual_pool(self.pool)
            .map_err(|source| self.pool_failure(source, session))?;
        let carrier = owner
            .carrier(&key, &prepared.options)
            .map_err(|source| self.pool_failure(source, session))?;
        if (!owner.dynamic && context.upstream.binding() != carrier.upstream.binding())
            || context.upstream.binding().dial != prepared.dial
        {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let stream = tokio::select! {biased;
            _=self.admission.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result=pool.open_managed_via_observed(self.pool,key,&prepared.options,Some(context.upstream.binding().clone()),context.deadline,context.upstream.observed_pending_carrier())=>result.map_err(|source|self.pool_failure(source,session))?,
        };
        if let Some(origin) = stream.original_carrier() {
            context.upstream.observe_carrier_origin(origin);
        }
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
        }
        let (mut stream, observed) = crate::client::native_io::Capture::new(stream);
        let result = tokio::select! {biased;
            _=self.admission.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result=with_deadline(context.deadline,crate::tunnel::request_udp_associate(&mut stream))=>result.map_err(|source|self.admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        };
        if let Some(observer) = &context.observer {
            if observed.written() > 0 {
                observer.record(session, ConnectFact::RequestSubmitted);
            }
            observer.protocol_traffic(session, 0, 0);
            if result.is_ok() {
                observer.record(session, ConnectFact::PeerConfirmed);
            } else if !observed.has_error() {
                observer.record(session, ConnectFact::PeerRejected);
            }
        }
        result.map_err(|wire| {
            let source = observed.recover(std::io::Error::other(wire));
            crate::client::NativeTcpStream::driver_failure(&source, session).unwrap_or_else(|| {
                self.admission
                    .error(
                        session,
                        Stage::Handshake,
                        if observed.has_error() {
                            Reason::Io
                        } else {
                            Reason::Protocol
                        },
                        DeliveryState::Unknown,
                    )
                    .with_source(source)
            })
        })?;
        let limits = DatagramLimits {
            max_payload: 65535 - 262,
            targets: request.targets,
            queued_bytes: 65539,
        };
        let loan = stream
            .into_inner()
            .into_carrier_loan()
            .map_err(|source| self.pool_failure(source, session))?;
        let shared = shared::Shared::new(loan, self.admission.clone(), session, context.cancel);
        Ok(DatagramSession {
            tx: tx::Tx::new(shared.clone(), request.association, limits.clone()),
            rx: rx::Rx::new(shared.clone(), request.association, limits.clone()),
            control: control::Control::new(shared),
            limits,
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/component/datagram.rs"]
mod tests;
