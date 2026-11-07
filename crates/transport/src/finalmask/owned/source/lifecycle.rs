use super::*;
impl ResourceLifecycle for Lifecycle {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
        validate(&self.masks).map_err(|e| failure(Reason::InvalidConfig).with_source(e))?;
        let mut state = self.state.lock().await;
        if self.cancel.is_cancelled() || state.native.is_some() {
            return Err(failure(Reason::Revoked));
        }
        let mut grant = self
            .resources
            .listen_metadata_datagram(self.request)
            .await?;
        let Some(claim) = grant.users_claim.take() else {
            state.rejected = Some(grant);
            return Err(failure(Reason::Unsupported));
        };
        let native = match crate::quic::owned::HostedMetadataSocket::new(grant, &self.tasks) {
            Ok(native) => native,
            Err(rejected) => {
                let mut grant = rejected.grant;
                grant.users_claim = Some(claim);
                state.return_unstarted(grant)?;
                return Err(failure(Reason::InvalidConfig).with_source(rejected.error));
            }
        };
        if !native.matches_claim(&claim) {
            state.return_unstarted(native.into_unstarted(claim))?;
            return Err(failure(Reason::InvalidConfig));
        }
        let (graph, receiver) = match claim.bind_users(
            self.tasks.clone(),
            NonZeroUsize::new(driver_count(&self.masks).max(1)).unwrap(),
            self.peers,
        ) {
            Ok(value) => value,
            Err(claim) => {
                state.return_unstarted(native.into_unstarted(claim))?;
                return Err(failure(Reason::Revoked));
            }
        };
        state.completion = Some(graph.completion());
        state.graph = Some(graph);
        state.receiver = Some(receiver);
        // Retain the original before any raw ICMP acquisition or worker spawn.
        state.native = Some(native);
        let icmp = icmp_bind(&self.masks)
            .map_err(|e| failure(Reason::InvalidConfig).with_source(e))?
            .map(|bind| self.resources.bind_inbound_icmp(bind))
            .transpose()?;
        let drivers = state
            .native
            .as_mut()
            .unwrap()
            .prepare_masks(&self.masks, icmp)
            .map_err(|e| failure(Reason::Io).with_source(e))?;
        state.stops.extend(
            drivers
                .iter()
                .map(|driver| (driver.ordinal, driver.work.cancel.clone())),
        );
        for driver in drivers {
            let origin = (driver.kind, driver.ordinal);
            state
                .graph
                .as_ref()
                .unwrap()
                .drivers()
                .spawn(
                    origin,
                    zero_platform_tokio::contract::scope::TaskClass::Critical,
                    |slot| async move {
                        let outcome = slot.record(driver.work.future.await.err().map(Arc::new));
                        match outcome.as_ref() {
                            Some(source) => {
                                Err(failure(Reason::Io).with_source(OriginalIo(source.clone())))
                            }
                            None => Ok(()),
                        }
                    },
                )
                .map_err(|e| failure(Reason::Capacity).with_source(e))?;
        }
        state.socket = Some(crate::finalmask::packet_socket::from_socket(
            state.native.as_ref().unwrap().socket(),
        ));
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.cancel.cancel();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.cancel.cancel();
        let mut state = tokio::time::timeout_at(deadline, self.state.lock())
            .await
            .map_err(|e| failure(Reason::Timeout).with_source(e))?;
        state.socket = None;
        state.peers.clear();
        if let Some(graph) = &state.graph {
            graph.users().cancellation().cancel();
            graph.drivers().revoke_registration();
        }
        if let Some(graph) = &state.graph {
            graph.users().wait_empty(deadline).await?;
            let mut ids = graph.drivers().tasks();
            ids.sort_by_key(|id| {
                std::cmp::Reverse(graph.drivers().origin(*id).map(|(_, ordinal)| ordinal))
            });
            for id in ids {
                let ordinal = state
                    .graph
                    .as_ref()
                    .unwrap()
                    .drivers()
                    .origin(id)
                    .unwrap()
                    .1;
                if let Some((_, stop)) = state.stops.iter().find(|(index, _)| *index == ordinal) {
                    stop.cancel();
                }
                let delivery = state
                    .receiver
                    .as_mut()
                    .unwrap()
                    .receive(id, deadline)
                    .await?;
                state.last_delivery = Some(delivery);
                let State {
                    receiver,
                    last_delivery,
                    ..
                } = &mut *state;
                receiver
                    .as_mut()
                    .unwrap()
                    .release_body(last_delivery.as_ref().unwrap())?;
            }
            state
                .graph
                .as_ref()
                .unwrap()
                .users()
                .wait_empty(deadline)
                .await?;
            state.native = None;
        }
        if let Some(completion) = &state.completion {
            let receipt = completion
                .try_owned_closed()
                .ok_or_else(|| failure(Reason::Timeout))?;
            self.resources
                .confirm_metadata_users(self.request.ordinal, &receipt, &self.tasks)?;
            state.completion = None;
            state.graph = None;
            state.receiver = None;
        }
        if state.rejected.is_some() {
            return Err(failure(Reason::InvalidConfig));
        }
        if let Some(completion) = &state.unstarted {
            let receipt = completion
                .try_closed()
                .ok_or_else(|| failure(Reason::Timeout))?;
            self.resources
                .confirm_metadata_users(self.request.ordinal, &receipt, &self.tasks)?;
            state.unstarted = None;
        }
        Ok(CloseReport::default())
    }
}
impl State {
    fn return_unstarted(&mut self, grant: GrantedMetadataListener) -> Result<(), ChannelError> {
        match grant.return_unstarted() {
            Ok(completion) => {
                self.unstarted = Some(completion);
                Ok(())
            }
            Err(grant) => {
                self.rejected = Some(grant);
                Err(failure(Reason::InvalidConfig))
            }
        }
    }
}
