//! Exact preinstalled native original and paired Packet/Quinn ownership.
use super::*;

impl Generation {
    pub(in crate::transport::owned::pool) async fn open_candidate(
        &self,
        state: &mut EntryState,
        profile: usize,
        index: usize,
        client: &quinn::ClientConfig,
        end: Instant,
    ) -> Result<OwnedConnection, ChannelError> {
        let options = &self.profiles[profile];
        if state.permit.is_none() || state.retirement.is_none() {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let port = options
            .leaf
            .node
            .udp_hop
            .as_ref()
            .map(|hop| hop.next_port());
        let mut logical = state
            .candidates
            .as_ref()
            .unwrap()
            .peer(index)
            .ok_or_else(|| error(Stage::Prepare, Reason::InvalidConfig))?;
        logical.set_port(options.leaf.port);
        // The original N is already held. Prepay actual scratch/codec/copy
        // before opening any native candidate; no second byte book is created.
        let (socket, queues) =
            PacketSocket::prepare_with_budget(logical, self.buffers.clone(), &options.masks())
                .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let (peer, grant) = state.candidates.as_ref().unwrap().open_metadata(
            index,
            port,
            state.permit.as_ref().unwrap(),
            state.caller.as_ref().unwrap(),
            end,
        )?;
        state.unbound = Some(grant);
        state.logical = Some(logical);
        state.next_hop = options
            .leaf
            .node
            .udp_hop
            .as_ref()
            .map(|hop| Instant::now() + hop.next_interval());
        let limits = OwnedEndpointLimits {
            drivers: n(3),
            users: self.endpoint_users,
        };
        let segment = match PacketSegment::prepare_with_endpoint(
            state.unbound.take().unwrap(), queues, self.tasks.clone(), peer, logical, limits,
        ) {
            Ok(segment) => segment,
            Err(rejected) => {
                let rejected = *rejected;
                state.unbound = Some(rejected.grant);
                state.queues = Some(rejected.queues);
                return Err(error(Stage::Prepare, Reason::Io).with_source(rejected.error));
            }
        };
        state.segment = Some(segment);
        let (owner, receiver) = OwnedEndpointOwner::new_packet_endpoint(
            socket, state.segment.as_mut().unwrap(), self.tasks.clone(), limits,
        )?;
        state.owner = Some(owner);
        state.receiver = Some(receiver);
        state.segment.as_mut().unwrap().start()?;
        let handle =
            state
                .owner
                .as_mut()
                .unwrap()
                .start(Default::default(), None, Some(client.clone()))?;
        if let Some(retirement) = &state.retirement {
            let receiver = state
                .owner
                .as_ref()
                .unwrap()
                .prepare_native_endpoint_receiver(
                    state.segment.as_ref().unwrap(),
                    state.origin.as_ref().unwrap(),
                    retirement,
                )?;
            receiver.confirm()?;
        }
        tokio::time::timeout_at(
            end,
            handle.connect(
                logical,
                options
                    .leaf
                    .server_name
                    .as_deref()
                    .unwrap_or(&options.leaf.server),
            ),
        )
        .await
        .map_err(|source| error(Stage::Dial, Reason::Timeout).with_source(source))?
    }
}
