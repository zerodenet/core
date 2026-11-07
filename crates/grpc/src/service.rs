//! The existing typed control handlers; protocol JSON and ACK semantics are unchanged.
use super::*;

async fn error_status(
    original: &Option<Arc<messages::Record>>,
    code: tonic::Code,
    prefix: &str,
    error: &(impl std::fmt::Display + Sync),
) -> Status {
    match original {
        Some(original) => original.error_status(code, prefix, error).await,
        None => Status::new(code, format!("{prefix}{error}")),
    }
}

pub(super) struct ControlService<H> {
    handle: H,
    auth: Option<GrpcServerAuth>,
    subscriptions: Arc<Subscriptions>,
    engine: Option<zero_engine::EngineHandle>,
}

impl<H> ControlService<H> {
    pub(super) fn new(
        handle: H,
        auth: Option<GrpcServerAuth>,
        subscriptions: Arc<Subscriptions>,
    ) -> Self {
        Self {
            handle,
            auth,
            subscriptions,
            engine: None,
        }
    }
    pub(super) fn with_engine(mut self, engine: Option<zero_engine::EngineHandle>) -> Self {
        self.engine = engine;
        self
    }

    fn is_authorized<T>(&self, request: &Request<T>) -> bool {
        match &self.auth {
            Some(auth) => auth.is_authorized(request),
            None => true,
        }
    }
}

#[tonic::async_trait]
impl<H> Control for ControlService<H>
where
    H: CommandService + QueryService + EventSource + Clone + Send + Sync + 'static,
{
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        if !self.is_authorized(&request) {
            return Err(Status::unauthenticated(
                "missing or invalid bearer authorization",
            ));
        }
        let original = messages::request(&request)?;
        let req = request.into_inner();
        let query: zero_api::QueryRequest = match serde_json::from_slice(&req.payload) {
            Ok(query) => query,
            Err(error) => {
                return Err(error_status(
                    &original,
                    tonic::Code::InvalidArgument,
                    "query parse error: ",
                    &error,
                )
                .await)
            }
        };
        if let (Some(engine), Some(original)) = (&self.engine, &original) {
            if let Some(result) = original.config_query(engine, &query).await {
                return result.map(|payload| Response::new(QueryResponse { payload }));
            }
            if let Some(result) = original.runtime_query(engine, &query) {
                return result.map(|payload| Response::new(QueryResponse { payload }));
            }
        }
        let result = match self.handle.query(query) {
            Ok(result) => result,
            Err(error) => {
                return Err(error_status(&original, tonic::Code::Internal, "", &error).await)
            }
        };
        let json = match original {
            Some(original) => original.json(messages::Payload::Query(&result)).await?,
            None => serde_json::to_vec(&result).map_err(|e| Status::internal(e.to_string()))?,
        };
        Ok(Response::new(QueryResponse { payload: json }))
    }

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        if !self.is_authorized(&request) {
            return Err(Status::unauthenticated(
                "missing or invalid bearer authorization",
            ));
        }
        let original = messages::request(&request)?;
        let req = request.into_inner();
        let cmd: zero_api::CommandRequest = match serde_json::from_slice(&req.payload) {
            Ok(command) => command,
            Err(error) => {
                return Err(error_status(
                    &original,
                    tonic::Code::InvalidArgument,
                    "command parse error: ",
                    &error,
                )
                .await)
            }
        };
        let result = match &original {
            Some(original) => original.execute(self.handle.clone(), cmd).await?,
            None => self
                .handle
                .execute_acknowledged(cmd)
                .await
                .map_err(|e: zero_api::ApiError| Status::internal(e.to_string()))?,
        };
        let json = match original {
            Some(original) => original.json(messages::Payload::Command(&result)).await?,
            None => serde_json::to_vec(&result).map_err(|e| Status::internal(e.to_string()))?,
        };
        Ok(Response::new(ExecuteResponse { payload: json }))
    }

    type SubscribeStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<PbEvent, Status>> + Send + 'static>,
    >;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        if !self.is_authorized(&request) {
            return Err(Status::unauthenticated(
                "missing or invalid bearer authorization",
            ));
        }
        let original = messages::request(&request)?;
        let req = request.into_inner();
        let filter = EventFilter {
            event_types: req.event_types,
            principal_keys: Vec::new(),
            inbound_tags: Vec::new(),
        };

        if let Err(error) = self.subscriptions.reap_completed().await {
            return Err(error_status(&original, tonic::Code::Internal, "", &error).await);
        }
        let subscription_charge = self
            .subscriptions
            .budget
            .reserve(zero_runtime::configuration::ControlCharge::Subscription)
            .map_err(|error| Status::resource_exhausted(error.to_string()))?;
        let subscribed = match &self.engine {
            Some(engine) => engine
                .subscribe(filter)
                .map(event_source::Subscriber::Engine),
            None => self
                .handle
                .subscribe(filter)
                .map(event_source::Subscriber::Standalone),
        };
        let subscriber = match subscribed {
            Ok(subscriber) => subscriber,
            Err(error) => {
                return Err(error_status(
                    &original,
                    tonic::Code::Internal,
                    "subscribe failed: ",
                    &error,
                )
                .await)
            }
        };

        let (tx, rx) = mpsc::channel::<budget::QueuedEvent>(64);

        let subscriptions = self.subscriptions.clone();
        let mut jobs = subscriptions.jobs.lock().unwrap();
        if subscriptions.stop.load(Ordering::Acquire) {
            return Err(Status::unavailable("control server is stopping"));
        }
        let owner = subscriptions.clone();
        // The retained job waits with a finite idle wake and keeps a full
        // event until the bounded channel accepts it. Stop and disconnected
        // consumers never depend on another source event to unblock recv.
        let worker = Arc::new(ControlTask::empty());
        let registration = match worker.reserve() {
            Ok(registration) => registration,
            Err(error) => return Err(Status::internal(error.to_string())),
        };
        jobs.push(worker.clone());
        let exiting = subscriptions::WorkerExit {
            original: worker.clone(),
            source: subscriptions.clone(),
        };
        let (begin, started) = std::sync::mpsc::sync_channel(1);
        let task = tokio::task::spawn_blocking(move || {
            let _exiting = exiting;
            if started.recv().is_err() {
                return Ok(());
            }
            while !owner.stop.load(Ordering::Acquire) && !tx.is_closed() {
                let event = match subscriber.receive(Duration::from_millis(25)) {
                    Ok(event) => event,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                };
                let bytes = budget::original_frame_len(&event)?;
                let Some((charge, encoded)) =
                    budget::reserve_message(&owner, original.as_ref(), bytes, || tx.is_closed())?
                else {
                    return Ok(());
                };
                // The original Engine queue reservation is still held here;
                // reserve_message prepaid all copies before the first clone.
                let message = PbEvent {
                    event_type: event.event_type.clone(),
                    event_id: event.event_id.clone(),
                    occurred_at: event.occurred_at_unix_ms,
                    payload: serde_json::to_vec(&event.payload).map_err(std::io::Error::other)?,
                };
                drop(event);
                let mut pending = budget::QueuedEvent {
                    event: Ok(message),
                    _charge: charge,
                    encoded: encoded
                        .map(|lease| (original.as_ref().unwrap().clone(), bytes, lease)),
                };
                loop {
                    if owner.stop.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    match tx.try_send(pending) {
                        Ok(()) => break,
                        Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
                        Err(mpsc::error::TrySendError::Full(event)) => pending = event,
                    }
                    std::thread::sleep(Duration::from_millis(25));
                }
            }
            Ok(())
        });
        registration.install_charged(task, subscription_charge);
        drop(jobs);
        let _ = begin.send(());

        let stream = budget::Events(rx);
        Ok(Response::new(Box::pin(stream) as Self::SubscribeStream))
    }
}
