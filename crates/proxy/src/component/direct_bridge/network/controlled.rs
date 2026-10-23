use super::*;
use crate::component::{
    direct_bridge::controlled::{ControlledCandidates, ControlledDemand},
    instance::retained,
};
use zero_platform_tokio::contract::network::ControlledStreamRequest;
impl DirectOpener {
    pub(super) async fn open_controlled(
        &self,
        binding: UpstreamBinding,
        request: ControlledStreamRequest,
    ) -> Result<BoxStream, ChannelError> {
        if !binding.prefix.is_empty()
            || binding.network_generation.0 != self.generation()
            || request.destination
                != Destination::new(self.session.target.clone(), self.session.port)
        {
            return Err(invalid(
                Reason::StalePlan,
                "controlled Direct differs from its original destination/policy generation",
            ));
        }
        let business = self.business.clone().ok_or_else(|| {
            invalid(
                Reason::Unsupported,
                "controlled Direct requires its original admitted business invocation",
            )
        })?;
        business.check_direct_destination(&request.destination)?;
        business.check_direct_owner(
            &binding.dial,
            &request.grants.registration(),
            &request.tasks,
        )?;
        tracing::trace!(
            session = self.session.id,
            "Direct controlled request authorized"
        );
        let candidates = ControlledCandidates::new(
            ControlledDemand {
                request: business,
                grants: request.grants,
                tasks: request.tasks,
                retirement: request.retirement,
            },
            request.context,
            self.services.clone(),
            binding.clone(),
        )?;
        match self
            .services
            .connect_direct_using(&self.session, &binding.dial, &candidates)
            .await
        {
            Ok(connection) => {
                if binding.network_generation.0 != self.generation() {
                    return Err(invalid(
                        Reason::StalePlan,
                        "network changed during controlled Direct establishment",
                    ));
                }
                *self.evidence.lock().expect("Direct observation poisoned") = Some(Ok(Evidence {
                    remote: connection.remote,
                    local: connection.local,
                    network: connection.network,
                }));
                Ok(Box::pin(connection.socket) as BoxStream)
            }
            Err(failure) => {
                *self.evidence.lock().expect("Direct observation poisoned") =
                    Some(Err(FailureEvidence {
                        stage: failure.stage,
                        network: failure.network,
                        legacy_error: failure.error.clone(),
                    }));
                match failure.source {
                    Some(source) => {
                        if let Some(original) =
                            crate::component::direct_bridge::controlled::original_channel(&source)
                        {
                            return Err(retained(original));
                        }
                        Err(ChannelError::new(
                            Stage::Dial,
                            if source.kind() == std::io::ErrorKind::TimedOut {
                                Reason::Timeout
                            } else {
                                Reason::Io
                            },
                            Impact::Session,
                        )
                        .with_source(source))
                    }
                    None => Err(ChannelError::new(Stage::Dial, Reason::Io, Impact::Session)
                        .with_source(failure.error)),
                }
            }
        }
    }
}
