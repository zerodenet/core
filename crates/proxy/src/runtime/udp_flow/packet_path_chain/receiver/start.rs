//! Bind the existing caller; Accepted already has its real association driver.
use super::*;
impl Receiver {
    pub(super) fn start(
        services: &TcpExecutionServices,
        responses: &mut super::super::response_join::ResponseSetLease,
    ) -> Result<Self, EngineError> {
        let invocation = services
            .invocation
            .as_ref()
            .ok_or_else(|| failure(rejected(Reason::InvalidConfig)))?;
        let caller = caller(services)?;
        let host = services
            .current_source
            .upgrade()
            .ok_or_else(|| failure(rejected(Reason::Revoked)))?;
        let (buffers, memory) = host.packet_path_memory().map_err(EngineError::from)?;
        let layout = PacketMetadataArc::<Failures>::layout().map_err(memory_failure)?;
        let credit = memory.reserve(layout).map_err(memory_failure)?;
        let failures = PacketMetadataArc::from_prepaid(Failures::default(), credit)
            .unwrap_or_else(|_| panic!("same exact prepaid response-source layout"));
        let shared_credit = memory
            .reserve(PacketMetadataArc::<Shared>::layout().map_err(memory_failure)?)
            .map_err(memory_failure)?;
        let shared = PacketMetadataArc::from_prepaid(
            Shared {
                pending: Mutex::new(PacketMetadataVec::default()),
                next: std::sync::atomic::AtomicU64::new(0),
                inline: Default::default(),
                changed: Default::default(),
                // Cleanup only. The owning manager is the actual association,
                // not the first Business session. Original grant/Scope fences keep
                // governing IO; manager Drop requests this one actor to return RX.
                stop: CancellationToken::new(),
                failures,
            },
            shared_credit,
        )
        .unwrap_or_else(|_| panic!("exact original response actor prepaid"));
        let receiver = if invocation.caller_accepted.is_some() {
            // The actual Critical association driver already occupies the second
            // declared SOCKS task. Poll this RX work in that same original task.
            if caller.native_task_id() != tokio::task::try_id() {
                return Err(failure(rejected(Reason::StalePlan)));
            }
            let actual = shared.clone();
            let memory = crate::protocol_registry::packet_memory::business::BusinessMemory {
                bytes: buffers.clone(),
                metadata: memory.clone(),
            };
            let inline = responses.install_packet_receiver(
                &memory,
                caller.clone(),
                shared.stop.clone(),
                shared.failures.clone().into_error(),
                run(actual),
            )?;
            shared
                .inline
                .set(inline)
                .unwrap_or_else(|_| panic!("one original inline RX registration"));
            caller.clone()
        } else {
            #[cfg(feature = "raw-ip-runtime")]
            {
                let actual = shared.clone();
                let native = invocation
                    .caller_native
                    .as_ref()
                    .ok_or_else(|| failure(rejected(Reason::Unsupported)))?;
                let (_, witness) = native
                    .spawn_datagram_receiver(move |_| run(actual))
                    .map_err(failure)?;
                witness
            }
            #[cfg(not(feature = "raw-ip-runtime"))]
            {
                return Err(failure(rejected(Reason::Unsupported)));
            }
        };
        if !caller
            .scope_identity()
            .same_scope(&receiver.scope_identity())
        {
            shared.stop.cancel();
            return Err(failure(rejected(Reason::StalePlan)));
        }
        Ok(Self {
            original: caller,
            witness: receiver,
            shared,
            memory,
            max_paths: services.retired_datagrams().capacity(),
            buffers,
        })
    }
}
