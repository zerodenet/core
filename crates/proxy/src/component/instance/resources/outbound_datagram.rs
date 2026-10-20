//! One finite native outbound grant for an already installed dynamic child.
mod access;
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
};
use zero_core::component::{ComponentInstanceId, NetworkGeneration, TaskId};
use zero_platform_tokio::{
    contract::{
        component::{
            GrantedOutboundDatagram, OutboundDatagramRequest, ResourceAccess, ResourceDeclaration,
        },
        lifecycle::CloseReport,
        CancellationToken, ChannelError, DeliveryState, Impact, Reason, Stage,
    },
    EgressInterfaceControl, TokioDatagramSocket,
};

/// The host retains this provider with its actual child owner and peer permit.
/// It grants neither a task nor close confirmation for a transferred socket.
/// The host alone activates it after binding the actual peer TaskId and revokes
/// it on that task's exit. A protocol receives only ResourceAccess authority.
pub(crate) struct HostOutboundDatagramResources {
    instance: ComponentInstanceId,
    peer: SocketAddr,
    generation: NetworkGeneration,
    egress: EgressInterfaceControl,
    cancel: CancellationToken,
    state: Mutex<State>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Cold,
    Live(TaskId),
    Revoked,
}
struct State {
    phase: Phase,
    attempted: bool,
    fault: Option<Arc<ChannelError>>,
    native_unconfirmed: bool,
}
impl HostOutboundDatagramResources {
    pub(crate) fn new(
        instance: ComponentInstanceId,
        declaration: &ResourceDeclaration,
        egress: EgressInterfaceControl,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>, ChannelError> {
        let ResourceDeclaration::OutboundDatagram {
            peer,
            network_generation,
            max_sockets,
        } = declaration
        else {
            return Err(error(
                instance,
                Reason::InvalidConfig,
                "not an outbound datagram declaration",
            ));
        };
        if max_sockets.get() != 1 {
            return Err(error(
                instance,
                Reason::Unsupported,
                "outbound datagram provider requires one socket and one attempt",
            ));
        }
        Ok(Arc::new(Self {
            instance,
            peer: *peer,
            generation: *network_generation,
            egress,
            cancel: cancel.child_token(),
            state: Mutex::new(State {
                phase: Phase::Cold,
                attempted: false,
                fault: None,
                native_unconfirmed: false,
            }),
        }))
    }
    fn state(&self) -> MutexGuard<'_, State> {
        // A native registration panic must not erase the preinstalled unknown
        // outcome. Its actual JoinError remains the peer task owner's evidence.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(crate) fn activate(&self, task: TaskId) -> Result<(), ChannelError> {
        let mut state = self.state();
        if self.cancel.is_cancelled() || state.phase == Phase::Revoked {
            return Err(error(
                self.instance,
                Reason::Revoked,
                "outbound datagram owner was revoked",
            ));
        }
        match state.phase {
            Phase::Cold => {
                state.phase = Phase::Live(task);
                Ok(())
            }
            Phase::Live(bound) if bound == task => Ok(()),
            _ => Err(error(
                self.instance,
                Reason::StalePlan,
                "outbound datagram grant cannot move to another peer task",
            )),
        }
    }
    pub(crate) fn revoke(&self) {
        self.state().phase = Phase::Revoked;
        self.cancel.cancel();
    }
    /// Permanent first failure, including known pre-native failures. No new
    /// attempt is admitted after this failure even if topology later recovers.
    pub(crate) fn fault(&self) -> Option<Arc<ChannelError>> {
        self.state().fault.clone()
    }
    pub(crate) fn native_unconfirmed(&self) -> bool {
        self.state().native_unconfirmed
    }
    /// Synchronous authority retirement only. Success reports no socket closed:
    /// transferred originals require their protocol owner and actual task exit.
    /// Pre-native failures have acquired no business socket for this grant;
    /// existing host network-selection probes remain in their original scope.
    /// Native acquisition errors/unwinding remain permanent unknown outcomes;
    /// they must retain the host's peer permit and child cleanup responsibility.
    pub(crate) fn shutdown_io(&self) -> Result<CloseReport, ChannelError> {
        self.revoke();
        let state = self.state();
        if state.native_unconfirmed {
            let mut failure = error(
                self.instance,
                Reason::Io,
                "native outbound datagram acquisition remains unconfirmed",
            )
            .with_source(state.fault.clone().expect("preinstalled native outcome"));
            failure.stage = Stage::Close;
            failure.delivery = DeliveryState::Unknown;
            return Err(failure);
        }
        Ok(CloseReport::default())
    }
    fn acquire(
        &self,
        request: OutboundDatagramRequest,
    ) -> Result<GrantedOutboundDatagram, ChannelError> {
        let mut state = self.state();
        if self.cancel.is_cancelled() || state.phase == Phase::Revoked {
            return Err(error(
                self.instance,
                Reason::Revoked,
                "outbound datagram owner was revoked",
            ));
        }
        if state.phase == Phase::Cold {
            return Err(error(
                self.instance,
                Reason::InvalidConfig,
                "outbound datagram access requires the bound live peer task",
            ));
        }
        if let Some(fault) = &state.fault {
            return Err(crate::component::instance::retained(fault.clone()));
        }
        if request.peer != self.peer || request.expected_network_generation != self.generation {
            return Err(error(
                self.instance,
                Reason::InvalidConfig,
                "outbound datagram request differs from its exact declaration",
            ));
        }
        if state.attempted {
            return Err(error(
                self.instance,
                Reason::Capacity,
                "outbound datagram acquisition attempt is exhausted",
            ));
        }
        state.attempted = true;
        let policy = zero_traits::DialPolicy::default();
        let prepared = if NetworkGeneration(self.egress.generation()) != self.generation {
            Err(error(
                self.instance,
                Reason::StalePlan,
                "outbound datagram network generation changed before native acquisition",
            ))
        } else {
            self.egress
                .select_for_peer_with_policy(self.peer, &policy)
                .map_err(|source| {
                    error(
                        self.instance,
                        Reason::Io,
                        "outbound datagram host selection failed",
                    )
                    .with_source(source)
                })
                .and_then(|selection| {
                    if NetworkGeneration(selection.generation()) != self.generation {
                        Err(error(
                            self.instance,
                            Reason::StalePlan,
                            "outbound datagram selection changed before native acquisition",
                        ))
                    } else {
                        Ok(selection)
                    }
                })
        };
        let selection = match prepared {
            Ok(selection) => selection,
            Err(failure) => {
                state.fault = Some(Arc::new(failure));
                return Err(crate::component::instance::retained(
                    state.fault.clone().unwrap(),
                ));
            }
        };
        // Install the conservative outcome BEFORE any native allocation. If
        // Tokio registration unwinds, the host still has this unknown record
        // plus its real peer JoinError. No poisoned mutex or Drop can erase it.
        state.native_unconfirmed = true;
        state.fault = Some(Arc::new(error(
            self.instance,
            Reason::Internal,
            "native outbound datagram acquisition did not complete",
        )));
        match TokioDatagramSocket::bind_for_peer_with_policy_preserving_port_sync(
            self.peer, None, &policy, &selection,
        ) {
            Ok(socket) => {
                state.native_unconfirmed = false;
                state.fault = None;
                // Never reject a successfully acquired original after a stale
                // generation/cancel check: transfer it to the installed owner
                // first, so that owner can retire it through its exact ledger.
                Ok(GrantedOutboundDatagram { socket, selection })
            }
            Err(source) => {
                let failure = Arc::new(
                    error(
                        self.instance,
                        Reason::Io,
                        "native outbound datagram acquisition failed",
                    )
                    .with_source(source),
                );
                state.fault = Some(failure.clone());
                Err(crate::component::instance::retained(failure))
            }
        }
    }
}
fn error(instance: ComponentInstanceId, reason: Reason, message: &'static str) -> ChannelError {
    let mut failure = ChannelError::new(Stage::Prepare, reason, Impact::Component)
        .with_source(std::io::Error::other(message));
    failure.component = Some(instance.component);
    failure.delivery = DeliveryState::NotSubmitted;
    failure
}

#[cfg(test)]
#[path = "../../tests/outbound_resources/mod.rs"]
mod tests;
