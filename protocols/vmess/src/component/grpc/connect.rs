use super::{
    state::{Body, Managed, Opening, Record},
    *,
};
use std::sync::atomic::AtomicBool;
use zero_platform_tokio::contract::{
    context::{with_deadline, OpenContext},
    network::CarrierContext,
    scope::TaskClass,
    stream::{BoxStream, ConnectRequest, Connected, StreamOutbound},
};
pub(super) struct Outbound(pub(super) Arc<Managed>);
impl StreamOutbound for Outbound {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let owner = &self.0;
        let session = owner.admission.check(&context)?;
        owner
            .check()
            .map_err(|source| owner.admission.attach(source, session))?;
        let upstream = match (&owner.options.business, &owner.upstream) {
            (Some(business), None)
                if context.upstream.binding() == &business.binding && session.is_some() =>
            {
                &context.upstream
            }
            (None, Some(upstream)) if context.upstream.binding() == upstream.binding() => upstream,
            _ => {
                return Err(owner.admission.error(
                    session,
                    Stage::Prepare,
                    Reason::StalePlan,
                    DeliveryState::NotSubmitted,
                ))
            }
        };
        crate::shared::validate_request_target(&request.destination.address).map_err(|source| {
            owner
                .admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
        })?;
        let cleanup_deadline = match &owner.options.business {
            Some(business) => Instant::now()
                .checked_add(business.cleanup_budget)
                .ok_or_else(|| {
                    owner.admission.error(
                        session,
                        Stage::Prepare,
                        Reason::InvalidConfig,
                        DeliveryState::NotSubmitted,
                    )
                })?,
            None => context.deadline.ok_or_else(|| {
                owner.admission.error(
                    session,
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    DeliveryState::NotSubmitted,
                )
            })?,
        };
        let prepared_tls = owner.prepared.lock().unwrap().clone();
        if owner.options.base.ca_file.is_some() && prepared_tls.is_none() {
            return Err(owner.admission.error(
                session,
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            ));
        }
        owner
            .reap(cleanup_deadline)
            .await
            .map_err(|source| owner.admission.attach(source, session))?;
        let mut grant = owner
            .grants
            .reserve()
            .map_err(|source| owner.admission.attach(source, session))?;
        // This one lease starts before physical preparation, remains with this
        // inline opening on error/cancel, and moves into the actual logical view.
        let user_view = if owner.options.business.is_some() {
            Some(
                grant
                    .claim_user_view()
                    .map_err(|source| owner.admission.attach(source, session))?,
            )
        } else {
            None
        };
        let record = Arc::new(Record {
            adoption: grant.adoption(),
            witness: Mutex::new(None),
            received: Mutex::new(None),
            accepted: AtomicBool::new(false),
            stop: owner.cancel.child_token(),
            opening: AtomicBool::new(true),
            done: AtomicBool::new(false),
            user_live: AtomicBool::new(false),
        });
        owner.records.lock().unwrap().push(record.clone());
        let _opening = Opening(record.clone(), Arc::downgrade(owner));
        // Persistent same-slot record precedes native socket creation and await.
        // Identity is passed unchanged; a Service-only provider rejects business.
        let prepared = upstream
            .prepare_controlled_stream(
                owner.options.base.endpoint.clone(),
                CarrierContext {
                    session,
                    deadline: context.deadline,
                    cancel: record.stop.clone(),
                },
                grant,
            )
            .map_err(|source| owner.admission.attach(source, session))?;
        let ready = owner
            .wait(
                &context,
                Stage::Dial,
                prepared.open_with_deadline(context.deadline),
            )
            .await?;
        let (io, _) = ready.into_parts();
        let preparing = io.prepare_driver(|io| async {
            let io = zero_transport::TcpRelayStream::new(super::super::carrier::Carrier::new(
                Box::pin(io),
            ));
            let io = if let Some(prepared) = prepared_tls {
                prepared.connect(io, &owner.options.base.server).await?
            } else if let Some(tls) = &owner.options.base.tls {
                zero_transport::tls::connect_tls_stream(io, tls, None, &owner.options.base.server)
                    .await?
            } else {
                io
            };
            let settings = io
                .application_settings()
                .filter(|settings| settings.protocol == b"h2")
                .map(|settings| settings.peer.clone());
            zero_transport::grpc::PreparedSingleClient::prepare(
                io,
                &owner.options.grpc,
                &owner.options.authority,
                settings.as_deref(),
            )
            .await
        });
        let tag = owner
            .wait(&context, Stage::Handshake, async {
                preparing.await.map_err(|source| {
                    owner
                        .admission
                        .error(
                            session,
                            Stage::Handshake,
                            Reason::Io,
                            DeliveryState::NotSubmitted,
                        )
                        .with_source(source)
                })
            })
            .await?;
        let (io, driver) = tag.split(zero_transport::grpc::PreparedSingleClient::into_parts);
        let body = Body(record.clone());
        let stop = record.stop.clone();
        let scope_stop = owner.tasks.cancellation();
        let instance = owner.instance;
        let (_,witness) = record.adoption.spawn_driver(&owner.tasks,TaskClass::Auxiliary,driver,|driver|async move {
            let _body = body;
            tokio::select! { biased;
                _=stop.cancelled()=>Ok(()),
                _=scope_stop.cancelled()=>Ok(()),
                result=driver=>result.map_err(|source|{
                    let mut error = error(instance,Stage::Receive,Reason::Io).with_source(source);
                    error.delivery = DeliveryState::Unknown; error.session = session; error
                }),
            }
        }).map_err(|source|owner.admission.attach(source,session))?;
        *record.witness.lock().unwrap() = Some(witness);
        owner
            .check()
            .map_err(|source| owner.admission.attach(source, session))?;
        let io =
            zero_transport::TcpRelayStream::new(super::wire::Stream::new(io, record, user_view));
        super::super::submit::submit(
            &owner.options.base,
            &owner.admission,
            request,
            context,
            session,
            io,
        )
        .await
    }
}
impl Managed {
    async fn wait<T>(
        &self,
        context: &OpenContext,
        stage: Stage,
        future: impl std::future::Future<Output = Result<T, ChannelError>>,
    ) -> Result<T, ChannelError> {
        let session = context.session_identity()?;
        let scope_cancel = self.tasks.cancellation();
        tokio::select! { biased;
            _=self.admission.cancelled()=>Err(self.admission.error(session,stage,Reason::Revoked,DeliveryState::Unknown)),
            _=scope_cancel.cancelled()=>Err(self.admission.error(session,stage,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>Err(self.admission.error(session,stage,Reason::Cancelled,DeliveryState::Unknown)),
            result=with_deadline(context.deadline,future)=>result.map_err(|source|
                self.admission.error(session,stage,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }
    }
}
