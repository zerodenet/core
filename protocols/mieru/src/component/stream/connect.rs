use super::*;
use crate::{
    client::{native_io::Capture, PoolKey},
    tunnel,
};
use std::{error::Error, fmt, io};
use zero_platform_tokio::contract::{
    context::{with_deadline, ConnectFact, OpenContext},
    stream::{BoxStream, ConnectRequest, Connected, PeerConfirmation, StreamOutbound},
};
#[derive(Debug)]
struct ProtocolFailure(zero_core::Error);
impl fmt::Display for ProtocolFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
impl Error for ProtocolFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}
impl StreamOutbound for Outbound {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let session = self.admission.check(&context)?;
        if request.destination.port == 0 {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        tunnel::build_request(1, &request.destination.address, request.destination.port).map_err(
            |source| {
                self.admission
                    .error(
                        session,
                        Stage::Prepare,
                        Reason::InvalidConfig,
                        DeliveryState::NotSubmitted,
                    )
                    .with_source(source)
            },
        )?;
        let (pool, owner) = self.options.actual_pool(self.pool).map_err(|source| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
        })?;
        if context.upstream.binding() != owner.upstream.binding()
            || context.upstream.binding().dial != self.options.prepared.dial
        {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let prepared = &self.options.prepared;
        let key = PoolKey::new(
            &prepared.tag,
            &prepared.server,
            prepared.port,
            &prepared.username,
            &prepared.password,
            false,
        );
        let opening = pool.open_managed_tcp(self.pool, key, &prepared.options, context.deadline);
        let stream = tokio::select! {biased;
            _=self.admission.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result=opening=>result.map_err(|source|self.pool_failure(source,session))?,
        };
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
        }
        let (mut stream, observed) = Capture::new(stream);
        let operation = tunnel::request_tcp_connect(
            &mut stream,
            &request.destination.address,
            request.destination.port,
        );
        let result = tokio::select! {biased;
            _=self.admission.cancelled()=>Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result=with_deadline(context.deadline,operation)=>match result {
                Err(source)=>Err(self.admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source)),
                Ok(result)=>result.map_err(|source|{
                    let source=observed.recover(io::Error::other(ProtocolFailure(source)));
                    crate::client::NativeTcpStream::driver_failure(&source,session).unwrap_or_else(||
                        self.admission.error(session,Stage::Handshake,if observed.has_error() {Reason::Io}else {Reason::Protocol},DeliveryState::Unknown).with_source(source))
                }),
            },
        };
        if let Some(observer) = &context.observer {
            if observed.written() > 0 {
                observer.record(session, ConnectFact::RequestSubmitted);
            }
            // The old Mieru SocketTcpHandshake returns explicit zero traffic.
            // Shared authentication, padding and encrypted underlay stay Outer.
            observer.protocol_traffic(session, 0, 0);
            if result.is_ok() {
                observer.record(session, ConnectFact::PeerConfirmed);
            } else if result
                .as_ref()
                .is_err_and(|error| error.reason == Reason::Protocol)
            {
                observer.record(session, ConnectFact::PeerRejected);
            }
        }
        result.map_err(|mut error| {
            if error.impact == zero_platform_tokio::contract::Impact::Session
                && observed.written() == 0
                && context.upstream.binding().prefix.is_empty()
            {
                error.delivery = DeliveryState::NotSubmitted;
                error.retry = zero_platform_tokio::contract::RetryClass::BeforeSubmission;
            }
            error
        })?;
        Ok(Connected {
            io: Box::pin(stream),
            peer_confirmation: PeerConfirmation::Confirmed,
        })
    }
}
impl Outbound {
    fn pool_failure(
        &self,
        source: io::Error,
        session: Option<zero_core::component::SessionId>,
    ) -> ChannelError {
        let (stage, reason, delivery) = match source.kind() {
            io::ErrorKind::WouldBlock => (
                Stage::Prepare,
                Reason::Capacity,
                DeliveryState::NotSubmitted,
            ),
            io::ErrorKind::NotConnected => {
                (Stage::Prepare, Reason::Revoked, DeliveryState::NotSubmitted)
            }
            io::ErrorKind::PermissionDenied => {
                (Stage::Handshake, Reason::Protocol, DeliveryState::Unknown)
            }
            io::ErrorKind::InvalidInput => (
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ),
            io::ErrorKind::TimedOut => (Stage::Handshake, Reason::Timeout, DeliveryState::Unknown),
            io::ErrorKind::Interrupted => (
                Stage::Handshake,
                if self.options.actual_pool(self.pool).is_err() {
                    Reason::Revoked
                } else {
                    Reason::Cancelled
                },
                DeliveryState::Unknown,
            ),
            _ => (Stage::Handshake, Reason::Io, DeliveryState::Unknown),
        };
        if source
            .get_ref()
            .is_some_and(|source| source.is::<ChannelError>())
        {
            // Preserve the opener's actual stage, reason, delivery and source.
            let source = source
                .into_inner()
                .unwrap()
                .downcast::<ChannelError>()
                .unwrap();
            return self.admission.attach(*source, session);
        }
        if source
            .get_ref()
            .is_some_and(|source| source.is::<zero_core::Error>())
        {
            let source = source
                .into_inner()
                .unwrap()
                .downcast::<zero_core::Error>()
                .unwrap();
            return self
                .admission
                .error(
                    session,
                    Stage::Handshake,
                    Reason::Protocol,
                    DeliveryState::Unknown,
                )
                .with_source(ProtocolFailure(*source));
        }
        if source
            .get_ref()
            .is_some_and(|source| source.is::<zero_platform_tokio::contract::scope::SpawnError>())
        {
            use zero_platform_tokio::contract::scope::SpawnError;
            let source = source
                .into_inner()
                .unwrap()
                .downcast::<SpawnError>()
                .unwrap();
            let reason = match *source {
                SpawnError::Capacity => Reason::Capacity,
                SpawnError::NotStarted => Reason::InvalidConfig,
                SpawnError::Closing => Reason::Revoked,
                SpawnError::IdentityExhausted => Reason::Internal,
            };
            return self
                .admission
                .error(session, Stage::Prepare, reason, DeliveryState::Unknown)
                .with_source(*source);
        }
        self.admission
            .error(session, stage, reason, delivery)
            .with_source(source)
    }
}
