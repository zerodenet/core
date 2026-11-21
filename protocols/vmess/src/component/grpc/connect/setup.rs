//! Sequential setup and driver share this same original's one Scope credit.
use super::{prepare::Prepared, *};
use std::{future::Future, sync::atomic::Ordering};
use zero_platform_tokio::{
    contract::{dns::DnsGeneration, network::UpstreamTransport, scope::TaskJoinWitness},
    controlled_carrier::{CarrierGrant, CarrierPreparation, CarrierUserView},
};
pub(super) struct View {
    pub(super) user_view: Option<CarrierUserView>,
    pub(super) preparation: Option<CarrierPreparation>,
    pub(super) caller: Option<TaskJoinWitness>,
}
struct CancelOnDrop(Option<zero_platform_tokio::contract::CancellationToken>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancel) = &self.0 {
            cancel.cancel();
        }
    }
}
impl Managed {
    pub(super) fn claim_view(
        &self,
        grant: &mut CarrierGrant,
        context: &OpenContext,
        upstream: &UpstreamTransport,
        service: bool,
    ) -> Result<View, ChannelError> {
        let session = context.session_identity()?;
        let tracked = session.is_some() && upstream.has_tracked_business_caller();
        let caller = if self.options.node() && tracked {
            Some(upstream.packet_caller(
                &context.identity,
                &CarrierContext {
                    session,
                    deadline: context.deadline,
                    cancel: context.cancel.clone(),
                },
            )?)
        } else {
            None
        };
        let (user_view, preparation) = if self.options.business.is_some() {
            if self.options.node() && (service || tracked) {
                let (view, preparation) = match &caller {
                    Some(caller) => grant.claim_preparing_user_view(caller)?,
                    None => grant.claim_preparing_transferred_view()?,
                };
                (Some(view), Some(preparation))
            } else {
                (
                    Some(if self.options.node() && !tracked {
                        grant.claim_transferred_view()?
                    } else {
                        grant.claim_user_view()?
                    }),
                    None,
                )
            }
        } else {
            (None, None)
        };
        Ok(View {
            user_view,
            preparation,
            caller,
        })
    }
    pub(super) async fn prepare_on_scope(
        &self,
        record: &Arc<Record>,
        preparation: &CarrierPreparation,
        context: &OpenContext,
        dns_generation: Option<DnsGeneration>,
        operation: impl Future<Output = Result<Prepared, ChannelError>> + Send + 'static,
    ) -> Result<Prepared, ChannelError> {
        let (send, mut receive) = tokio::sync::oneshot::channel();
        let (begin, started) = tokio::sync::oneshot::channel::<
            zero_platform_tokio::contract::scope::TaskJoinWitness,
        >();
        let stop = record.stop.clone();
        let result_owner = preparation.clone();
        let upstream = context.upstream.clone();
        let (_, witness) = record.adoption.spawn_auxiliary(&self.tasks, async move {
            let Ok(caller) = started.await else {
                return Ok(());
            };
            let result = tokio::select! { biased;
                _ = stop.cancelled() => {
                    let _ = send.send(Err(Arc::new(ChannelError::new(
                        Stage::Prepare, Reason::Cancelled,
                        zero_platform_tokio::contract::Impact::Session,
                    ))));
                    return Ok(());
                },
                result = operation => result,
            };
            match result {
                Ok(prepared) => {
                    let _ = send.send(Ok(prepared));
                    Ok(())
                }
                Err(source) => {
                    let source = Arc::new(source);
                    if zero_platform_tokio::controlled_carrier::original_connect_error(&source)
                        .is_some()
                    {
                        // This actual failed native attempt is the caller's
                        // result. Retain the same Arc and original Aux before
                        // its exit, so its later native/Scope cleanup cannot
                        // reclassify a consumed candidate as a component fault.
                        result_owner.retain_connect_rejection_result(&caller, source.clone())?;
                    } else if old_root_dns(&source, &upstream, dns_generation) {
                        // The real provider moved to a new network before this
                        // root dial submitted anything. Deliver the original
                        // request result as in the stop branch. The DNS query,
                        // grant and Auxiliary still retire through their owners.
                        let _ = send.send(Err(source));
                        return Ok(());
                    }
                    let _ = send.send(Err(source.clone()));
                    Err(ChannelError::retained(source))
                }
            }
        })?;
        let mut cancel = CancelOnDrop(Some(record.stop.clone()));
        if let Some(observer) = context.upstream.observed_pending_carrier() {
            preparation.bind_pending_observer(observer)?;
        }
        let _ = begin.send(witness.clone());
        let session = context.session_identity()?;
        let received = self
            .wait(context, Stage::Prepare, async {
                (&mut receive).await.map_err(|source| {
                    self.admission
                        .error(
                            session,
                            Stage::Prepare,
                            Reason::Revoked,
                            DeliveryState::Unknown,
                        )
                        .with_source(source)
                })
            })
            .await;
        let result = match received {
            Ok(result) => result,
            Err(source) => {
                return Err(match receive.try_recv() {
                    Ok(Err(original)) => ChannelError::retained(original),
                    _ => source,
                })
            }
        };
        let mut first = result.as_ref().err().cloned();
        let joined = match self
            .wait(context, Stage::Prepare, record.adoption.accept_auxiliary())
            .await
        {
            Ok(Some(joined)) if witness.matches_joined(&joined) => joined,
            Ok(_) => {
                return Err(first.map_or_else(
                    || {
                        self.admission.error(
                            session,
                            Stage::Prepare,
                            Reason::Internal,
                            DeliveryState::Unknown,
                        )
                    },
                    ChannelError::retained,
                ))
            }
            Err(source) => return Err(first.map_or(source, ChannelError::retained)),
        };
        if let Err(source) = joined.result() {
            first.get_or_insert(source);
        }
        // Body completion/acceptance alone does not release the original Scope
        // credit. The persistent host must consume this exact registration.
        let released = self
            .wait(context, Stage::Prepare, async {
                Ok(witness.wait_registration_released().await)
            })
            .await;
        match released {
            Ok(released)
                if witness.matches_joined(&released) && released.registration_released() => {}
            Ok(_) => {
                return Err(first.map_or_else(
                    || {
                        self.admission.error(
                            session,
                            Stage::Prepare,
                            Reason::Internal,
                            DeliveryState::Unknown,
                        )
                    },
                    ChannelError::retained,
                ))
            }
            Err(source) => return Err(first.map_or(source, ChannelError::retained)),
        }
        if let Some(source) = first {
            return Err(ChannelError::retained(source));
        }
        // This merely stops the cancellation guard. Publication remains the
        // exact preparation's sealed one-way operation after driver install.
        drop(cancel.0.take());
        debug_assert!(!record.published.load(Ordering::Acquire));
        result.map_err(ChannelError::retained)
    }
}

fn old_root_dns(
    error: &ChannelError,
    upstream: &UpstreamTransport,
    expected: Option<DnsGeneration>,
) -> bool {
    let binding = upstream.binding();
    error.stage == Stage::Dial
        && error.reason == Reason::StalePlan
        && error.delivery == DeliveryState::NotSubmitted
        && error.impact == zero_platform_tokio::contract::Impact::Session
        && binding.prefix.is_empty()
        && expected.is_some_and(|old| {
            old.network == binding.network_generation
                && upstream
                    .carrier_dns_generation()
                    .is_some_and(|current| current.network.0 > old.network.0)
        })
}
