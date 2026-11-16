//! Install original accepted custody and the real driver before publication.
use super::*;
impl Portal {
    /// Called only after runtime has granted admission to the authenticated
    /// control session. The returned guard owns removal and transport teardown.
    pub(crate) fn attach<S>(
        &self,
        stream: S,
        backlog: MuxResponseBacklogPolicy,
    ) -> Result<PortalRegistration, Error>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (registration, driver) = self.prepare_attach(stream, backlog, None)?;
        let task = tokio::spawn(driver);
        registration.worker.custody.library(task);
        registration.worker.custody.publish();
        self.publish_worker(registration.worker.clone())?;
        Ok(registration)
    }

    pub(crate) fn attach_scoped<S>(
        &self,
        stream: S,
        backlog: MuxResponseBacklogPolicy,
        caller: &zero_platform_tokio::AcceptedCaller,
    ) -> Result<PortalRegistration, Error>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (registration, driver) = self.prepare_attach(stream, backlog, Some(caller.clone()))?;
        let resource = Arc::new(
            caller
                .retain_logical_resource(zero_platform_tokio::contract::lifecycle::erase_lifecycle(
                    ownership::WorkerOwner(
                        registration.worker.custody.clone(),
                        registration.worker.connection.clone(),
                    ),
                ))
                .map_err(|_| Error::Io("Rvs original custody registration failed"))?,
        );
        let _ = registration.worker.resource.set(resource.clone());
        let (publish, installed) = tokio::sync::oneshot::channel();
        let actual = caller.spawn(
            zero_platform_tokio::contract::scope::TaskClass::Auxiliary,
            move |tasks| {
                let bound = resource.bind_driver(&tasks);
                async move {
                    bound?;
                    installed.await.map_err(|source| {
                        zero_platform_tokio::contract::ChannelError::new(
                            zero_platform_tokio::contract::Stage::Prepare,
                            zero_platform_tokio::contract::Reason::Revoked,
                            zero_platform_tokio::contract::Impact::Component,
                        )
                        .with_source(source)
                    })?;
                    driver.await;
                    Ok(())
                }
            },
        );
        let (_, witness) =
            actual.map_err(|_| Error::Io("Rvs original driver registration failed"))?;
        registration.worker.custody.hosted(witness.clone());
        if let Some(provider) = self.0.provider.get() {
            provider
                .retain(
                    caller.clone(),
                    registration.worker.origin.clone(),
                    witness.clone(),
                    &registration.worker.resource.get().unwrap(),
                )
                .map_err(|_| Error::Io("Rvs actual Source provider registration failed"))?;
            registration.worker.custody.source_uses(
                provider
                    .driver_uses(&witness)
                    .ok_or(Error::Io("Rvs original Source uses absent"))?,
            );
        }

        registration.worker.custody.publish();
        publish
            .send(())
            .map_err(|_| Error::Io("Rvs original driver publication failed"))?;
        self.publish_worker(registration.worker.clone())?;
        Ok(registration)
    }

    fn publish_worker(&self, worker: Arc<Worker>) -> Result<(), Error> {
        let mut workers = self.0.workers.lock().unwrap();
        // Retirement and selection use this same gate. An already acquired
        // resource stays with its real Source even if publication is fenced.
        if self.0.retired.load(Ordering::Acquire) {
            drop(workers);
            worker.connection.close();
            return Err(Error::Io("Rvs portal retired before worker publication"));
        }
        workers.push(worker);
        Ok(())
    }

    fn prepare_attach<S>(
        &self,
        stream: S,
        backlog: MuxResponseBacklogPolicy,
        caller: Option<zero_platform_tokio::AcceptedCaller>,
    ) -> Result<(PortalRegistration, zero_transport::pooled_driver::Driver), Error>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let mut workers = self.0.workers.lock().unwrap();
        if self.0.managed.load(Ordering::Acquire) && !self.0.ready.load(Ordering::Acquire) {
            return Err(Error::Io("Rvs provider has not started"));
        }
        if self.0.retired.load(Ordering::Acquire) {
            return Err(Error::Io("Rvs portal retired"));
        }
        workers.retain(|worker| !worker.custody.confirmed());
        let (mut connection, driver) =
            MuxPoolConn::prepare(stream, &[0; 16], u16::MAX as u32, None, backlog, false);
        let original = caller
            .as_ref()
            .map(|caller| caller.carrier_origin())
            .transpose()
            .map_err(|_| Error::Io("Rvs original carrier observation failed"))?
            .flatten();
        connection.inbound_caller = caller;
        let worker = Arc::new(Worker {
            connection: Arc::new(connection),
            draining: AtomicBool::new(false),
            custody: Arc::default(),
            resource: std::sync::OnceLock::new(),
            origin: original,
        });
        let sid = worker
            .connection
            .try_reserve_stream_id()
            .ok_or(Error::Io("Rvs control admission failed"))?;
        let (sender, receiver, native_control) = if worker.connection.inbound_caller.is_some() {
            let control = crate::mux_pool::native::reverse_control(worker.connection.clone(), sid)
                .map_err(|_| Error::Io("Rvs original control reservation failed"))?;
            (None, None, Some(control))
        } else {
            let (_, sender, receiver) = worker.connection.open_udp_stream_with_id(sid, None)?;
            (Some(sender), Some(receiver), None)
        };
        if let Some(caller) = &worker.connection.inbound_caller {
            worker.custody.primary(
                caller
                    .task_join_witness()
                    .ok_or(Error::Io("Rvs original caller witness absent"))?,
            );
        }
        drop(workers);
        Ok((
            PortalRegistration {
                worker,
                sender,
                receiver,
                native_control,
            },
            driver,
        ))
    }
}
