//! One real actor's short execution view. Retained state holds only the weak
//! existing Host service owner and its sealed caller; never Engine or snapshots.
use super::*;
mod receive;
pub(crate) use receive::PacketCodecReceiveGroups;
use zero_platform_tokio::contract::component::GrantedPacketServiceCaller;
#[derive(Clone)]
pub(crate) struct PacketPathExecutionSource {
    services: Weak<CurrentServiceSource>,
    caller: Arc<GrantedPacketServiceCaller>,
    resources: Weak<dyn zero_platform_tokio::contract::component::ResourceAccess>,
    receive: Option<PacketCodecReceiveGroups>,
}
impl PacketPathExecutionSource {
    pub(crate) fn receive_cleanup_failure(
        &self,
        source: Arc<ChannelError>,
    ) -> Result<bool, ChannelError> {
        self.resources
            .upgrade()
            .ok_or_else(|| {
                invalid(
                    Reason::Revoked,
                    "same Packet original ResourceAccess retired",
                )
            })?
            .receive_packet_driver_failure(&self.caller, source)
    }

    pub(super) fn new(
        services: Weak<CurrentServiceSource>,
        caller: GrantedPacketServiceCaller,
        resources: Weak<dyn zero_platform_tokio::contract::component::ResourceAccess>,
    ) -> Self {
        Self {
            services,
            caller: Arc::new(caller),
            resources,
            receive: None,
        }
    }
    pub(crate) fn with_receive_workspaces(
        mut self,
        groups: Vec<crate::protocol_registry::packet_memory::PacketCodecReceiveMemory>,
    ) -> Result<Self, ChannelError> {
        if groups
            .iter()
            .any(|group| !group.matches_caller(&self.caller))
        {
            return Err(invalid(
                Reason::StalePlan,
                "codec workspaces belong to another actual Source Kernel Root",
            ));
        }
        let memory = self
            .resources
            .upgrade()
            .ok_or_else(|| invalid(Reason::Revoked, "actual Packet Source resources retired"))?
            .packet_driver_metadata()?;
        self.receive = Some(PacketCodecReceiveGroups::from_values(&memory, groups)?);
        Ok(self)
    }
    pub(crate) fn with_prepaid_receive(
        mut self,
        groups: PacketCodecReceiveGroups,
    ) -> Result<Self, ChannelError> {
        if groups
            .iter()
            .any(|group| !group.matches_caller(&self.caller))
        {
            return Err(invalid(
                Reason::StalePlan,
                "codec workspaces belong to another actual Source Kernel Root",
            ));
        }
        self.receive = Some(groups);
        Ok(self)
    }
    pub(crate) fn caller(&self) -> &zero_platform_tokio::contract::scope::TaskJoinWitness {
        self.caller.caller()
    }
    pub(crate) fn spawn_reconnect<F, Fut>(
        &self,
        tasks: &zero_platform_tokio::contract::scope::WeakTaskRegistrar,
        make: F,
    ) -> Result<
        (
            zero_core::component::TaskId,
            zero_platform_tokio::contract::scope::TaskJoinWitness,
        ),
        zero_platform_tokio::contract::ChannelError,
    >
    where
        F: FnOnce(Self) -> Fut + Send + 'static,
        Fut: std::future::Future<
                Output = Result<(), zero_platform_tokio::contract::lifecycle::ComponentError>,
            > + Send
            + 'static,
    {
        let services = self.services.clone();
        let receive = self.receive.clone();
        let resources = self.resources.clone();
        self.caller.spawn_reconnect(tasks, move |caller| {
            make(Self {
                services,
                caller: Arc::new(caller),
                resources,
                receive,
            })
        })
    }
    pub(crate) fn request_carrier_retirement(&self) {
        self.caller.request_carrier_retirement();
    }

    pub(crate) fn receive_reconnect_failures(&self) -> Result<(), ChannelError> {
        if let Some(source) = self.caller.receive_reconnect_failures() {
            self.receive_cleanup_failure(Arc::new(source))?;
        }
        Ok(())
    }

    pub(crate) fn cleanup_failures_received(&self) -> bool {
        self.caller.cleanup_failures_received()
    }

    /// Drive all actual attempts of this carrier call, including failed and cancelled
    /// setup from earlier reconnect actors. Donor native/Scope receipts stay
    /// with their actual Host dependency; no physical-close claim is minted.
    pub(crate) async fn retire_carriers(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<zero_platform_tokio::contract::lifecycle::CloseReport, ChannelError> {
        self.caller.retire_carrier_attempts(deadline).await
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.caller.cancellation().is_cancelled()
    }
    pub(crate) async fn cancelled(&self) {
        self.caller.cancellation().cancelled().await
    }

    pub(crate) async fn build_carrier(
        &self,
        recipe: &crate::component::stream_bridge::datagram::packet_path::RolePacketPath,
        endpoint: std::net::SocketAddr,
    ) -> Result<
        Arc<dyn crate::runtime::udp_flow::packet_path::PacketPathCarrier>,
        zero_engine::EngineError,
    > {
        let source = self
            .services
            .upgrade()
            .ok_or_else(|| std::io::Error::other("Packet driver Host retired"))?;
        let services = source
            .packet_driver_services(&self.caller)
            .map_err(std::io::Error::other)?;
        let end = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        let attempt = Arc::new(
            self.caller
                .for_attempt(end)
                .map_err(std::io::Error::other)?,
        );
        let execution = crate::protocol_registry::PacketPathExecutionServices::from_tcp_execution(
            &services.execution(),
        )
        .with_packet_driver(attempt.clone())
        .with_packet_target(zero_core::request::Destination::new(
            match endpoint.ip() {
                std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
                std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
            },
            endpoint.port(),
        ));
        let execution = match &self.receive {
            Some(groups) => execution.with_packet_driver_receive(groups.clone()),
            None => execution,
        };
        match recipe.build(execution).await {
            Ok(path) => Ok(path),
            Err(source) => {
                // Cleanup uses the same original attempt and immutable end.
                // Incomplete/unknown debt stays in the Source owner and blocks
                // the next attempt; no parent actor self-join is required.
                let cleanup = attempt.retire_attempt(end).await;
                match cleanup {
                    Ok(report) if report.is_complete() => Err(source),
                    Ok(_) => Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        AttemptRetained {
                            opening: source,
                            cleanup: None,
                            receiving: None,
                        },
                    )
                    .into()),
                    Err(cleanup) => {
                        let cleanup = Arc::new(cleanup);
                        let receiving = self.receive_cleanup_failure(cleanup.clone()).err();
                        Err(std::io::Error::other(AttemptRetained {
                            opening: source,
                            cleanup: Some(ChannelError::retained(cleanup)),
                            receiving,
                        })
                        .into())
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
struct AttemptRetained {
    opening: zero_engine::EngineError,
    cleanup: Option<zero_platform_tokio::contract::ChannelError>,
    receiving: Option<zero_platform_tokio::contract::ChannelError>,
}
impl std::fmt::Display for AttemptRetained {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "original Packet attempt: {}; cleanup: {:?}; receiver: {:?}",
            self.opening, self.cleanup, self.receiving
        )
    }
}
impl std::error::Error for AttemptRetained {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.receiving
            .as_ref()
            .or(self.cleanup.as_ref())
            .map(|source| source as _)
            .or(Some(&self.opening))
    }
}
