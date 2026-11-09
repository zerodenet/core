//! One original call's whole Node answer survives genuine sequential attempts.
use super::*;
use zero_core::request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts};
use zero_platform_tokio::contract::scope::TaskJoinWitness;
use zero_platform_tokio::{
    contract::context::{RequestIdentity, ServiceFactsOwner},
    controlled_carrier::{loan::BorrowedDatagramCandidates, CarrierCandidates, CarrierGrant},
};
pub(super) struct Demand {
    pub upstream: UpstreamTransport,
    pub call: CarrierContext,
    identity: RequestIdentity,
    native: Mutex<Option<PacketMetadataArc<CarrierCandidates>>>,
    borrowed: Mutex<Option<PacketMetadataArc<BorrowedDatagramCandidates>>>,
}
impl Demand {
    pub async fn new(
        generation: &Generation,
        context: &OpenContext,
    ) -> Result<PacketMetadataArc<Self>, ChannelError> {
        let call = CarrierContext {
            session: None,
            deadline: context.deadline,
            cancel: context.cancel.clone(),
        };
        // Keep the actual waiting caller's existing finite pending ledger when
        // rebuilding the owning physical prefix. A fresh NodeOpener with no
        // observer cannot receive its lower originals before loan delivery.
        let waiting = context.upstream.observed_pending_carrier();
        let selected = async {
            match waiting {
                Some(waiting) => {
                    generation
                        .upstream
                        .select_background_carrier_observed(
                            Some(context.upstream.binding().clone()),
                            call.clone(),
                            waiting,
                        )
                        .await
                }
                None => {
                    generation
                        .upstream
                        .select_background_carrier(
                            Some(context.upstream.binding().clone()),
                            call.clone(),
                        )
                        .await
                }
            }
        };
        let upstream = with_deadline(context.deadline, selected)
            .await
            .map_err(|source| error(Stage::Prepare, Reason::Timeout).with_source(source))??;
        let credit = generation.memory(
            ServiceFactsOwner::paid_layout()
                .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?,
        )?;
        let identity = RequestIdentity::Service(
            ServiceFactsOwner::from_prepaid(
                ServiceRequestFacts {
                    origin: ServiceOrigin::Runtime,
                    purpose: ServicePurpose::BackgroundControl,
                },
                credit,
            )
            .map_err(|(source, _, _)| {
                error(Stage::Prepare, Reason::Capacity).with_source(source)
            })?,
        );
        let credit = generation.memory(
            PacketMetadataArc::<Self>::layout()
                .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?,
        )?;
        PacketMetadataArc::from_prepaid(
            Self {
                upstream,
                call,
                identity,
                native: Mutex::new(None),
                borrowed: Mutex::new(None),
            },
            credit,
        )
        .map_err(|(source, _, _)| error(Stage::Prepare, Reason::Capacity).with_source(source))
    }
    pub async fn native(
        &self,
        generation: &Generation,
    ) -> Result<PacketMetadataArc<CarrierCandidates>, ChannelError> {
        let previous = self.native.lock().unwrap().clone();
        if let Some(previous) = previous {
            return Ok(previous);
        }
        let credit = generation.memory(
            PacketMetadataArc::<CarrierCandidates>::layout()
                .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?,
        )?;
        let candidates = self
            .upstream
            .resolve_controlled_carrier(
                generation.options.protocol.endpoint.clone(),
                self.call.clone(),
            )
            .await?;
        let candidates =
            PacketMetadataArc::from_prepaid(candidates, credit).map_err(|(source, _, _)| {
                error(Stage::Prepare, Reason::Capacity).with_source(source)
            })?;
        *self.native.lock().unwrap() = Some(candidates.clone());
        Ok(candidates)
    }
    pub async fn borrowed(
        &self,
        generation: &Generation,
        grant: &CarrierGrant,
        actor: &TaskJoinWitness,
    ) -> Result<PacketMetadataArc<BorrowedDatagramCandidates>, ChannelError> {
        let previous = self.borrowed.lock().unwrap().clone();
        if let Some(previous) = previous {
            return Ok(previous);
        }
        let credit = generation.memory(
            PacketMetadataArc::<BorrowedDatagramCandidates>::layout()
                .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?,
        )?;
        let issuer = self.issuer(generation, grant, actor)?;
        let candidates = self
            .upstream
            .resolve_service_datagram_candidates(issuer)
            .await?;
        let candidates =
            PacketMetadataArc::from_prepaid(candidates, credit).map_err(|(source, _, _)| {
                error(Stage::Prepare, Reason::Capacity).with_source(source)
            })?;
        *self.borrowed.lock().unwrap() = Some(candidates.clone());
        Ok(candidates)
    }
    pub fn issuer(
        &self,
        generation: &Generation,
        grant: &CarrierGrant,
        actor: &TaskJoinWitness,
    ) -> Result<
        zero_platform_tokio::controlled_carrier::loan::ServiceDatagramTargetIssuer,
        ChannelError,
    > {
        generation.resources.service_datagram_target_issuer(
            grant,
            actor,
            self.upstream.binding(),
            &generation.options.protocol.endpoint,
            &self.identity,
            &self.call,
        )
    }
    pub fn len(&self) -> usize {
        if self.upstream.binding().prefix.is_empty() {
            self.native
                .lock()
                .unwrap()
                .as_ref()
                .map_or(0, |candidates| candidates.len())
        } else {
            self.borrowed
                .lock()
                .unwrap()
                .as_ref()
                .map_or(0, |candidates| candidates.len())
        }
    }
}
pub(super) fn connection_failure(source: &ChannelError) -> bool {
    use std::error::Error;
    let mut current: Option<&(dyn Error + 'static)> = Some(source);
    for _ in 0..32 {
        let Some(error) = current else { return false };
        if let Some(original) = error.downcast_ref::<zero_transport::quic::QuicConnectionError>() {
            return !matches!(
                original,
                zero_transport::quic::QuicConnectionError::LocallyClosed
            );
        }
        current = error.source();
    }
    false
}
