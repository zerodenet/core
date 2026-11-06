use super::*;
use std::path::PathBuf;
use zero_core::request::Destination;
use zero_platform_tokio::{
    EgressSelection,
    contract::{
        BoxFuture,
        component::{BoundDatagramAttemptFailure, GrantedBoundDatagram, ResourceAccess},
        packet::BoxPacketSession,
    },
    controlled_datagram::AcquisitionOutcome,
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
        self.open_bound_datagram_attempt(request)
            .map_err(BoundDatagramAttemptFailure::into_error)
    }
    fn open_bound_datagram_attempt(
        &self,
        request: BoundDatagramRequest,
    ) -> Result<GrantedBoundDatagram, BoundDatagramAttemptFailure> {
        let binding = self
            .binding(&request)
            .map_err(BoundDatagramAttemptFailure::denied)?;
        let selected = binding
            .select(request.peer, request.expected_network_generation)
            .map_err(BoundDatagramAttemptFailure::denied)?;
        let mut state = self.state();
        self.ensure_available(&state, true)
            .map_err(BoundDatagramAttemptFailure::denied)?;
        if !state
            .bindings
            .get(&request.binding.sequence())
            .is_some_and(|actual| Arc::ptr_eq(actual, &binding))
        {
            return Err(BoundDatagramAttemptFailure::denied(error(
                self.instance,
                Reason::StalePlan,
                "host UDP binding changed during selection",
            )));
        }
        binding
            .check_current(request.expected_network_generation)
            .map_err(BoundDatagramAttemptFailure::denied)?;
        self.collect(&mut state);
        let native = state.owner.occupied();
        let tracked_native = state
            .originals
            .keys()
            .filter(|id| state.owner.retains(**id))
            .count();
        // Count the union, including an actual retained failed attempt whose
        // caller unwound before publishing its record, and all retiring views.
        let retained = state
            .originals
            .len()
            .checked_add(native.saturating_sub(tracked_native))
            .ok_or_else(|| {
                BoundDatagramAttemptFailure::denied(error(
                    self.instance,
                    Reason::Capacity,
                    "original UDP responsibility count exhausted",
                ))
            })?;
        if retained >= self.max_sockets.get() {
            return Err(BoundDatagramAttemptFailure::denied(error(
                self.instance,
                Reason::Capacity,
                "original UDP capacity includes pending/retiring views and drivers",
            )));
        }
        let acquired = state.owner.acquire(
            request.peer,
            binding.preferred_port(),
            binding.policy(),
            &selected,
        );
        let socket = match acquired {
            Ok(socket) => {
                let control = state
                    .owner
                    .take_control(&socket)
                    .expect("fresh original UDP view has one unissued controller");
                state.originals.insert(
                    socket.reservation(),
                    Original {
                        binding: request.binding.sequence(),
                        control: Some(Arc::new(control)),
                        view: None,
                    },
                );
                self.collect(&mut state);
                socket
            }
            Err(source) => {
                state
                    .first_acquisition_source
                    .get_or_insert_with(|| source.source.clone());
                if matches!(source.outcome, AcquisitionOutcome::Unknown(_)) {
                    if let Some(id) = source.reservation {
                        state.originals.insert(
                            id,
                            Original {
                                binding: request.binding.sequence(),
                                control: None,
                                view: None,
                            },
                        );
                    }
                }
                self.collect(&mut state);
                return Err(self.issuer.acquisition_failure(
                    &request,
                    error(
                        self.instance,
                        Reason::Io,
                        "host UDP original acquisition failed",
                    ),
                    source,
                    &state.owner,
                ));
            }
        };
        let control = state
            .originals
            .get(&socket.reservation())
            .and_then(|original| original.control.clone())
            .expect("actual successful original retains its same controller");
        drop(state);
        if let Some(scope) = &self.scope {
            let driver = self
                .driver
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            let registered = control
                .register_provider(self.instance, scope.clone())
                .and_then(|_| {
                    let driver = driver.as_ref().ok_or_else(|| {
                        std::io::Error::other("actual UDP driver is not installed")
                    })?;
                    control
                        .register_driver(&control.identity(), driver)
                        .map(|_| ())
                });
            if let Err(source) = registered {
                let closed = control.close();
                let mut state = self.state();
                self.collect(&mut state);
                let failure = if let Err(close) = closed {
                    let mut failure = error(
                        self.instance,
                        Reason::Io,
                        "original UDP provider registration cleanup remains unknown",
                    )
                    .with_source(close);
                    failure.delivery = DeliveryState::Unknown;
                    failure
                } else {
                    error(
                        self.instance,
                        Reason::StalePlan,
                        "actual UDP provider registration rejected",
                    )
                    .with_source(source)
                };
                return Err(BoundDatagramAttemptFailure::denied(failure));
            }
        }
        // The original already resides in the persistent native owner. A stale
        // completion is closed there, never dropped or replayed via legacy I/O.
        if let Err(failure) = binding.check_current(request.expected_network_generation) {
            let _ = socket.close();
            self.collect(&mut self.state());
            return Err(BoundDatagramAttemptFailure::denied(failure));
        }
        let id = socket.reservation();
        let granted = GrantedBoundDatagram::from_host(socket, request.binding, selected, binding);
        let view = granted.socket.return_observation();
        let mut state = self.state();
        self.ensure_available(&state, true)
            .map_err(BoundDatagramAttemptFailure::denied)?;
        let original = state.originals.get_mut(&id).ok_or_else(|| {
            BoundDatagramAttemptFailure::denied(error(
                self.instance,
                Reason::StalePlan,
                "original UDP registration was removed before view transfer",
            ))
        })?;
        if !original
            .control
            .as_ref()
            .is_some_and(|control| view.matches_control(control))
        {
            return Err(BoundDatagramAttemptFailure::denied(error(
                self.instance,
                Reason::StalePlan,
                "UDP view does not belong to this original controller",
            )));
        }
        original.view = Some(view);
        drop(state);
        Ok(granted)
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
