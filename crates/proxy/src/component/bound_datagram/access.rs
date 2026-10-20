use super::*;
use std::path::PathBuf;
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{
        component::{GrantedBoundDatagram, ResourceAccess},
        packet::BoxPacketSession,
        BoxFuture,
    },
    controlled_datagram::AcquisitionOutcome,
    EgressSelection,
};
impl ResourceAccess for HostBoundDatagramResources {
    fn select_bound_outbound_datagram(
        &self,
        request: BoundDatagramRequest,
    ) -> Result<EgressSelection, ChannelError> {
        let binding = self.binding(&request)?;
        binding.select(request.peer, request.expected_network_generation)
    }
    fn open_bound_outbound_datagram(
        &self,
        request: BoundDatagramRequest,
    ) -> Result<GrantedBoundDatagram, ChannelError> {
        let binding = self.binding(&request)?;
        let selected = binding.select(request.peer, request.expected_network_generation)?;
        let mut state = self.state();
        self.ensure_available(&state, true)?;
        if !state
            .bindings
            .get(&request.binding.sequence())
            .is_some_and(|actual| Arc::ptr_eq(actual, &binding))
        {
            return Err(error(
                self.instance,
                Reason::StalePlan,
                "host UDP binding changed during selection",
            ));
        }
        binding.check_current(request.expected_network_generation)?;
        let acquired = state.owner.acquire(
            request.peer,
            binding.preferred_port(),
            binding.policy(),
            &selected,
        );
        let socket = match acquired {
            Ok(socket) => {
                state
                    .originals
                    .insert(socket.reservation(), request.binding.sequence());
                self.collect(&mut state);
                socket
            }
            Err(source) => {
                if matches!(source.outcome, AcquisitionOutcome::Unknown(_)) {
                    if let Some(id) = source.reservation {
                        state.originals.insert(id, request.binding.sequence());
                    }
                }
                self.collect(&mut state);
                return Err(error(
                    self.instance,
                    Reason::Io,
                    "host UDP original acquisition failed",
                )
                .with_source(source));
            }
        };
        drop(state);
        // The original already resides in the persistent native owner. A stale
        // completion is closed there, never dropped or replayed via legacy I/O.
        if let Err(failure) = binding.check_current(request.expected_network_generation) {
            let _ = socket.close();
            self.collect(&mut self.state());
            return Err(failure);
        }
        Ok(GrantedBoundDatagram::from_host(
            socket,
            request.binding,
            selected,
            binding,
        ))
    }
    fn read_file(&self, _: PathBuf, _: usize) -> BoxFuture<'_, Result<Vec<u8>, ChannelError>> {
        Box::pin(async {
            Err(error(
                self.instance,
                Reason::Unsupported,
                "UDP grant does not authorize files",
            ))
        })
    }
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, ChannelError>> {
        Box::pin(async {
            Err(error(
                self.instance,
                Reason::Unsupported,
                "UDP grant does not authorize TCP listeners",
            ))
        })
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedDatagramListener, ChannelError>,
    > {
        Box::pin(async {
            Err(error(
                self.instance,
                Reason::Unsupported,
                "outbound UDP grant is not an inbound listener grant",
            ))
        })
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, ChannelError>> {
        Box::pin(async {
            Err(error(
                self.instance,
                Reason::Unsupported,
                "UDP grant does not authorize devices",
            ))
        })
    }
}
