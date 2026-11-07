//! Sequential setup and driver share this same original's one Scope credit.
use super::{prepare::Prepared, *};
use std::{future::Future, sync::atomic::Ordering};
use zero_platform_tokio::{
    contract::{network::UpstreamTransport, scope::TaskJoinWitness},
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
        operation: impl Future<Output = Result<Prepared, ChannelError>> + Send + 'static,
    ) -> Result<Prepared, ChannelError> {
        let (send, mut receive) = tokio::sync::oneshot::channel();
        let (begin, started) = tokio::sync::oneshot::channel();
        let stop = record.stop.clone();
        let (_, witness) = record.adoption.spawn_auxiliary(&self.tasks, async move {
            if started.await.is_err() {
                return Ok(());
            }
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
                    let _ = send.send(Err(source.clone()));
                    Err(ChannelError::retained(source))
                }
            }
        })?;
        let mut cancel = CancelOnDrop(Some(record.stop.clone()));
        if let Some(observer) = context.upstream.observed_pending_carrier() {
            preparation.bind_pending_observer(observer)?;
        }
        let _ = begin.send(());
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
