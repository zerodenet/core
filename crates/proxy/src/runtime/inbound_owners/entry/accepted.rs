//! Actual owning receiver, original Scope identity and exit dispatch.
use super::*;
impl InboundOwner {
    pub(in crate::runtime::inbound_owners) fn preparation_tasks(
        &self,
    ) -> Result<TaskRegistrar, EngineError> {
        if self.cancel.is_cancelled() || *self.shutdown.borrow() {
            return Err(self.ready_stopped());
        }
        self.scope
            .try_lock()
            .map_err(|_| std::io::Error::other("preparation Scope is already joining"))?
            .enable_registration()
            .map_err(std::io::Error::other)?;
        Ok(self.registrar.clone())
    }
    pub(in crate::runtime::inbound_owners) async fn join_preparation(
        &self,
        witness: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<(), EngineError> {
        let mut scope = self.scope.lock().await;
        while !witness.is_joined() {
            let exit = scope.next_exit().await.ok_or_else(|| {
                std::io::Error::other("actual preparation task has no join receipt")
            })?;
            if self.record(exit) {
                self.stop();
            }
        }
        Ok(())
    }
    pub(in crate::runtime::inbound_owners) fn accepted_registrar(&self) -> &TaskRegistrar {
        &self.registrar
    }
    pub(in crate::runtime::inbound_owners) fn install_accepted_receiver(
        &self,
        receiver: zero_platform_tokio::AcceptedTcpTaskReceiver,
    ) -> Result<(), EngineError> {
        let mut receivers = self.accepted_receiver.lock().unwrap();
        if receivers.len() >= 3 {
            return Err(std::io::Error::other("declared accepted listener limit exceeded").into());
        }
        receivers.push(receiver);
        Ok(())
    }
    pub(crate) async fn observe_driver(&self) -> Result<(), EngineError> {
        if self.driver.get().is_none() {
            return Err(std::io::Error::other("inbound driver was not accepted").into());
        }
        let mut scope = self.scope.lock().await;
        #[cfg(feature = "managed-datagram-runtime")]
        {
            self.observe_peer_driver(&mut scope).await
        }
        #[cfg(not(feature = "managed-datagram-runtime"))]
        {
            while !self.driver_finished.load(Ordering::Acquire) {
                let exit = scope.next_exit().await.ok_or_else(|| {
                    std::io::Error::other("listener driver exit remains unobserved")
                })?;
                let failed = self.record(exit); // persist the actual ID/source before any next await
                if failed {
                    self.stop();
                }
            }
            self.result()
        }
    }
}

impl InboundOwner {
    /// The production listener observer outlives its orchestration borrower.
    /// A primary driver's exit alone cannot discard active peers' join handles.
    pub(crate) async fn observe_listener(&self) -> Result<(), EngineError> {
        self.observe_listener_inner(
            #[cfg(feature = "raw-ip-runtime")]
            None,
        )
        .await
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::runtime::inbound_owners) async fn observe_system_tun(
        &self,
        projection: Option<crate::inbound::tun::exit::SystemTunExitProjection>,
    ) -> Result<(), EngineError> {
        self.observe_listener_inner(projection).await
    }
    async fn observe_listener_inner(
        &self,
        #[cfg(feature = "raw-ip-runtime")] projection: Option<
            crate::inbound::tun::exit::SystemTunExitProjection,
        >,
    ) -> Result<(), EngineError> {
        let result = self.observe_driver().await;
        if result.is_ok() && !self.driver_was_stopping.get().copied().unwrap_or(false) {
            // Preserve the old unexpected-success supervision behavior while
            // this same observer continues to join remaining actual children.
            let mut source = ChannelError::new(Stage::Close, Reason::Internal, Impact::Component)
                .with_source(EngineError::InboundTaskExited);
            source.component = Some(self.identity.component);
            self.failure
                .lock()
                .expect("inbound failure poisoned")
                .get_or_insert_with(|| Arc::new(source).into());
            // This is a listener supervision fault, not a fabricated failed
            // TaskExit. The real successful task receipt remains unchanged.
            self.ready.wake();
        }
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(projection) = projection {
            let outcome = self.result();
            projection.record(
                self.identity(),
                result.as_ref().err().or(outcome.as_ref().err()),
            );
        }
        self.stop();
        {
            let mut scope = self.scope.lock().await;
            scope.begin_shutdown();
            while let Some(exit) = scope.next_exit().await {
                self.record(exit); // exact native exit persisted before the next await
            }
            self.scope_closed.store(true, Ordering::Release);
        }
        // Scope completion proves only actual tasks. Retained native and
        // transferred-user owners still require their separate close receipts.
        result.and(self.result())
    }
}
