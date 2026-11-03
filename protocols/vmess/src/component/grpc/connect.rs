use super::{
    state::{Body, Managed, Opening, Record},
    *,
};
use std::sync::atomic::AtomicBool;
use zero_platform_tokio::contract::{
    context::{with_deadline, OpenContext},
    network::CarrierContext,
    scope::TaskClass,
};
mod stream;
mod wait;
pub(super) struct Outbound(pub(super) Arc<Managed>);
impl Managed {
    pub(super) async fn open_transport(
        self: &Arc<Self>,
        destination: &zero_core::request::Destination,
        context: &OpenContext,
    ) -> Result<(zero_transport::TcpRelayStream, Arc<Record>), ChannelError> {
        let owner = self;
        let session = owner.admission.check(context)?;
        owner
            .check()
            .map_err(|source| owner.admission.attach(source, session))?;
        let upstream = match (&owner.options.business, &owner.upstream) {
            (Some(business), None)
                if session.is_some()
                    && (business.binding.as_ref() == Some(context.upstream.binding())
                        || (owner.options.node()
                            && context.upstream.binding().dial == owner.options.base.dial)) =>
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
        crate::shared::validate_request_target(&destination.address).map_err(|source| {
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
        let mut network = if owner.options.node() {
            Some(owner.network.lock().await)
        } else {
            None
        };
        let selected = context.upstream.binding().network_generation;
        let dns_generation = context.upstream.carrier_dns_generation();
        if network
            .as_ref()
            .is_some_and(|network| network.is_some_and(|old| old != selected))
        {
            // Do not open on a new network while originals on the old network
            // still have logical users, drivers or unconfirmed native controls.
            owner.reap(cleanup_deadline).await?;
            if !owner.records.lock().unwrap().is_empty() {
                return Err(owner.admission.error(
                    session,
                    Stage::Close,
                    Reason::StalePlan,
                    DeliveryState::NotSubmitted,
                ));
            }
        }
        owner
            .reap(cleanup_deadline)
            .await
            .map_err(|source| owner.admission.attach(source, session))?;
        if let Some(network) = network.as_mut() {
            **network = Some(selected);
        }
        let mut last_connect: Option<ChannelError> = None;
        loop {
            let mut grant = owner
                .grants
                .reserve()
                .map_err(|source| owner.admission.attach(source, session))?;
            // This one lease starts before physical preparation, remains with this
            // inline opening on error/cancel, and moves into the actual logical view.
            let user_view = if owner.options.business.is_some() {
                Some(
                    (if owner.options.node() {
                        grant.claim_transferred_view()
                    } else {
                        grant.claim_user_view()
                    })
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
            let acquiring = upstream.acquire_controlled_stream(
                owner.options.base.endpoint.clone(),
                CarrierContext {
                    session,
                    deadline: context.deadline,
                    cancel: record.stop.clone(),
                },
                grant,
            );
            let io = match owner
                .wait(context, Stage::Dial, acquiring)
                .await
                .map_err(|error| owner.admission.attach(error, session))
            {
                Ok(ready) => ready,
                Err(error)
                    if owner.options.node()
                        && (matches!(
                            owner.options.base.endpoint.address,
                            zero_core::Address::Domain(_)
                        ) || !context.upstream.binding().prefix.is_empty())
                        && zero_platform_tokio::controlled_carrier::original_connect_error(
                            &error,
                        )
                        .is_some()
                        && (context.upstream.binding().prefix.is_empty()
                            || error.retry
                                == zero_platform_tokio::contract::RetryClass::BeforeSubmission)
                        && matches!(error.reason, Reason::Io | Reason::Timeout)
                        && !context.cancel.is_cancelled()
                        && context.deadline.is_none_or(|end| Instant::now() < end) =>
                {
                    record.stop.cancel();
                    drop(user_view);
                    let end =
                        Instant::now() + owner.options.business.as_ref().unwrap().cleanup_budget;
                    let report = record.retire(end).await?;
                    if !report.is_complete() {
                        return Err(owner.admission.error(
                            session,
                            Stage::Close,
                            Reason::Timeout,
                            DeliveryState::Unknown,
                        ));
                    }
                    last_connect = Some(error);
                    continue;
                }
                Err(error) => {
                    return Err(if error.reason == Reason::Io {
                        last_connect.unwrap_or(error)
                    } else {
                        error
                    })
                }
            };
            let preparing = io.prepare_driver(|io| async {
                let io = zero_transport::TcpRelayStream::new(super::super::carrier::Carrier::new(
                    Box::pin(io),
                ));
                let io = if let Some(prepared) = prepared_tls {
                    prepared.connect(io, &owner.options.base.server).await?
                } else if let Some(tls) = &owner.options.base.tls {
                    zero_transport::tls::connect_tls_stream(
                        io,
                        tls,
                        None,
                        &owner.options.base.server,
                    )
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
                .wait(context, Stage::Handshake, async {
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
            if owner.options.node()
                && (
                    context.upstream.binding().network_generation,
                    context.upstream.carrier_dns_generation(),
                ) != (selected, dns_generation)
            {
                record.stop.cancel();
                return Err(owner.admission.error(
                    session,
                    Stage::Handshake,
                    Reason::StalePlan,
                    DeliveryState::NotSubmitted,
                ));
            }
            let io = zero_transport::TcpRelayStream::new(super::wire::Stream::new(
                io,
                record.clone(),
                user_view,
            ));
            return Ok((io, record));
        }
    }
}
