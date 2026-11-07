//! Start and retire the same canonical linked graph and original worker Scope.
use super::*;

impl ComponentFactory for LinkedStackFactory {
    type Options = LinkedStackOptions;
    fn describe(&self, options: &LinkedStackOptions) -> ComponentSpec {
        options.spec.clone()
    }
    fn build(
        &self,
        options: Arc<LinkedStackOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        // No task runs in build. The host-installed lifecycle owns Options
        // before start, and each peer retains only this original weak authority.
        options
            .bind_dependencies(&context.dependencies)
            .map_err(prepare_error)?;
        options
            .bind(context.tasks.downgrade())
            .map_err(prepare_error)?;
        let carrier = Arc::new(super::super::carrier::Carrier {
            options: super::super::carrier::Graph::Linked(options.clone()),
            resources: context.resources,
            scope: context.tasks.scope_identity(),
        });
        options.carrier.set(Arc::downgrade(&carrier)).map_err(|_| {
            prepare_error(invalid("linked roles cannot bind a second actual graph"))
        })?;
        Ok(ComponentInstance {
            roles: ComponentRoles {
                stream: Some(carrier.clone()),
                datagram: Some(carrier.clone()),
                packet: Some(carrier),
                ..Default::default()
            },
            lifecycle: erase_lifecycle(Owner { options }),
        })
    }
}
struct Owner {
    options: Arc<LinkedStackOptions>,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: tokio::time::Instant) -> Result<(), BuildError> {
        #[cfg(test)]
        eprintln!(
            "actual linked graph {} start before original workers",
            self.options.tag
        );
        self.options.start_graph().map_err(prepare_error)?;
        #[cfg(test)]
        eprintln!(
            "actual linked graph {} workers registered; waiting real Ready",
            self.options.tag
        );
        for peer in 0..self.options.peers {
            let device = self
                .options
                .pending
                .peer_stack(peer)
                .and_then(|owner| owner.device())
                .ok_or_else(|| {
                    prepare_error(invalid("same linked software driver was not installed"))
                })?;
            tokio::time::timeout_at(deadline, device.wait_ready())
                .await
                .map_err(|source| {
                    ChannelError::new(Stage::Prepare, Reason::Timeout, Impact::Component)
                        .with_source(source)
                })?
                .map_err(prepare_error)?;
        }
        #[cfg(test)]
        eprintln!(
            "actual linked graph {} original workers Ready",
            self.options.tag
        );
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.options.pending.revoke();
    }
    async fn shutdown(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, BuildError> {
        let closed = self.options.pending.stop_peer_resources(deadline).await;
        if let Some(source) = closed.failures.first() {
            #[cfg(test)]
            eprintln!(
                "same original linked peer graph {} cleanup source: {source:?}",
                self.options.tag
            );
            tracing::warn!(tag = %self.options.tag, ?source, "same original linked peer cleanup failed");
            return Err(
                ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                    .with_source(source.clone()),
            );
        }
        let mut report = CloseReport::default();
        if !closed.is_complete() {
            report.unfinished.push(Unfinished::Resource(format!(
                "linked peer graph {}",
                self.options.tag
            )));
        }
        // Real task IDs/failures are delivered by the same slot's TaskMonitor.
        // This virtual graph cannot issue its listener's physical-close receipt.
        Ok(report)
    }
}
