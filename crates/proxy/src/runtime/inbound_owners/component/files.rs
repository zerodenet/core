//! Bounded file sessions use the actual accepted graph and Scope. Blocking jobs
//! remain in the original owner if a waiter, driver, or startup is interrupted.
use super::*;
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};
use zero_platform_tokio::{
    contract::scope::{TaskClass, TaskJoinWitness},
    file::tree::{FileTreeCommand, FileTreeNative, FileTreeOutput, FileTreeSession},
    AcceptedCaller,
};
type Job = tokio::task::JoinHandle<std::io::Result<FileTreeOutput>>;
#[cfg(test)]
type BeforeFileOperation =
    Arc<dyn Fn(&zero_platform_tokio::file::tree::FileTreeOperation) + Send + Sync>;
struct State {
    job: Option<Job>,
    driver: Option<TaskJoinWitness>,
    failure: Option<Arc<ChannelError>>,
    spawning: bool,
}
#[derive(Clone)]
pub(super) struct FileTrees {
    root: PathBuf,
    permits: Arc<Semaphore>,
    scope: zero_platform_tokio::contract::scope::TaskScopeIdentity,
    cancel: CancellationToken,
    initial: Arc<AsyncMutex<Option<tokio::task::JoinHandle<std::io::Result<()>>>>>,
    initial_phase: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    before: Arc<Mutex<Option<BeforeFileOperation>>>,
    #[cfg(test)]
    issued: Arc<Mutex<Option<std::sync::Weak<FileTreeNative>>>>,
}
impl FileTrees {
    pub(super) fn new(
        declaration: &ResourceDeclaration,
        scope: zero_platform_tokio::contract::scope::TaskScopeIdentity,
        cancel: CancellationToken,
    ) -> Result<Self, EngineError> {
        let ResourceDeclaration::FileTree { root, max_sessions } = declaration else {
            return Err(std::io::Error::other(denied()).into());
        };
        Ok(Self {
            root: root.clone(),
            permits: Arc::new(Semaphore::new(max_sessions.get())),
            scope,
            cancel,
            initial: Arc::new(AsyncMutex::new(None)),
            initial_phase: Arc::new(std::sync::atomic::AtomicU8::new(0)),
            #[cfg(test)]
            before: Arc::default(),
            #[cfg(test)]
            issued: Arc::default(),
        })
    }
    pub(super) async fn start(&self, deadline: Instant) -> Result<(), BuildError> {
        let mut job = self.initial.lock().await;
        if self.cancel.is_cancelled() {
            return Err(denied());
        }
        if self
            .initial_phase
            .compare_exchange(
                0,
                1,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_err()
        {
            return Err(denied());
        }
        let root = self.root.clone();
        *job = Some(tokio::task::spawn_blocking(move || {
            FileTreeNative::new(root).validate_root()
        }));
        self.initial_phase
            .store(2, std::sync::atomic::Ordering::Release);
        // Keep the original native job handle under this retained owner while
        // awaiting it. A cancelled caller cannot detach a filesystem operation.
        let result = tokio::time::timeout_at(deadline, job.as_mut().unwrap())
            .await
            .map_err(|e| denied().with_source(e))?;
        *job = None;
        self.initial_phase
            .store(3, std::sync::atomic::Ordering::Release);
        result
            .map_err(|e| denied().with_source(e))?
            .map_err(|e| denied().with_source(e))
    }
    pub(super) async fn shutdown(&self, deadline: Instant) -> Result<CloseReport, BuildError> {
        let mut job = tokio::time::timeout_at(deadline, self.initial.lock())
            .await
            .map_err(|e| denied().with_source(e))?;
        if job.is_none()
            && self
                .initial_phase
                .load(std::sync::atomic::Ordering::Acquire)
                == 1
        {
            return Ok(CloseReport {
                unfinished: vec![
                    zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                        "file root job registration remains unknown".into(),
                    ),
                ],
                ..Default::default()
            });
        }
        if let Some(pending) = job.as_mut() {
            let joined = tokio::time::timeout_at(deadline, pending)
                .await
                .map_err(|e| denied().with_source(e))?;
            *job = None;
            self.initial_phase
                .store(3, std::sync::atomic::Ordering::Release);
            joined
                .map_err(|e| denied().with_source(e))?
                .map_err(|e| denied().with_source(e))?;
        }
        Ok(CloseReport::default())
    }
    pub(super) fn open(
        &self,
        caller: AcceptedCaller,
        root: PathBuf,
    ) -> Result<FileTreeSession, BuildError> {
        caller.check_open()?;
        if self.cancel.is_cancelled()
            || root != self.root
            || !self.scope.same_scope(&caller.scope_identity())
        {
            return Err(denied());
        }
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|e| denied().with_source(e))?;
        let state = Arc::new(AsyncMutex::new(State {
            job: None,
            driver: None,
            failure: None,
            spawning: false,
        }));
        let native = Arc::new(FileTreeNative::new(self.root.clone()));
        #[cfg(test)]
        {
            *self.issued.lock().unwrap() = Some(Arc::downgrade(&native));
        }
        #[cfg(test)]
        let before = self.before.lock().unwrap().clone();
        let stop = self.cancel.child_token();
        let returned = CancellationToken::new();
        let resource = Arc::new(caller.retain_logical_resource(erase_lifecycle(Owner {
            state: state.clone(),
            native: native.clone(),
            stop: stop.clone(),
            returned: returned.clone(),
            permit: Some(permit),
            receipts: CloseReport::default(),
            failure_reported: false,
        }))?);
        let (session, commands) = FileTreeSession::channel(stop.clone(), returned);
        caller.spawn(TaskClass::Critical, move |tasks| {
            let bound = resource.bind_driver(&tasks);
            async move {
                bound?;
                state.lock().await.driver = tasks.caller()?.task_join_witness();
                drive(
                    state,
                    native,
                    commands,
                    stop,
                    #[cfg(test)]
                    before,
                )
                .await
            }
        })?;
        Ok(session)
    }
}
async fn drive(
    state: Arc<AsyncMutex<State>>,
    native: Arc<FileTreeNative>,
    mut commands: tokio::sync::mpsc::Receiver<FileTreeCommand>,
    stop: CancellationToken,
    #[cfg(test)] before: Option<BeforeFileOperation>,
) -> Result<(), BuildError> {
    loop {
        let command = tokio::select! {biased;_=stop.cancelled()=>return Ok(()),command=commands.recv()=>match command {Some(command)=>command,None=>return Ok(())}};
        let mut state = state.lock().await;
        let original = native.clone();
        state.spawning = true;
        #[cfg(test)]
        let before = before.clone();
        state.job = Some(tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some(before) = before {
                before(&command.operation);
            }
            original.execute(command.operation)
        }));
        state.spawning = false;
        let result = state.job.as_mut().unwrap().await;
        state.job = None;
        match result {
            Ok(result) => {
                let _ = command.reply.send(result);
            }
            Err(source) => {
                let source = Arc::new(denied().with_source(source));
                state.failure.get_or_insert_with(|| source.clone());
                return Err(crate::component::instance::retained(source));
            }
        }
    }
}
struct Owner {
    state: Arc<AsyncMutex<State>>,
    native: Arc<FileTreeNative>,
    stop: CancellationToken,
    returned: CancellationToken,
    permit: Option<OwnedSemaphorePermit>,
    receipts: CloseReport,
    failure_reported: bool,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.stop.cancel();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.stop.cancel();
        let mut state = tokio::time::timeout_at(deadline, self.state.lock())
            .await
            .map_err(|e| denied().with_source(e))?;
        if state.spawning {
            return Ok(CloseReport {
                unfinished: vec![
                    zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                        "original file job registration remains unknown".into(),
                    ),
                ],
                ..Default::default()
            });
        }
        if let Some(job) = state.job.as_mut() {
            let result = tokio::time::timeout_at(deadline, job)
                .await
                .map_err(|e| denied().with_source(e))?;
            state.job = None;
            if let Err(source) = result {
                state
                    .failure
                    .get_or_insert_with(|| Arc::new(denied().with_source(source)));
            }
        }
        if state
            .driver
            .as_ref()
            .is_some_and(|driver| !driver.is_joined())
            || !self.returned.is_cancelled()
        {
            return Ok(CloseReport {
                unfinished: vec![
                    zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                        "original file session view/driver remains owned".into(),
                    ),
                ],
                ..Default::default()
            });
        }
        let receipt = self
            .native
            .close_originals()
            .map_err(|e| denied().with_source(e))?;
        if !receipt.matches(&self.native) {
            return Err(denied());
        }
        let count = receipt.closed_handles();
        if count != 0 {
            self.receipts.closed_io.push(format!(
                "{count} original file/directory handles explicitly closed"
            ));
        }
        if let Some(source) = &state.failure {
            // This private owner is polled only by the actual accepted resource
            // collector. That collector durably remembers Err before polling
            // this owner again; do not turn a published task fault into a
            // permanent native-cleanup gap after real close/join confirmation.
            if !self.failure_reported {
                self.failure_reported = true;
                return Err(crate::component::instance::retained(source.clone()));
            }
        }
        self.permit = None;
        Ok(std::mem::take(&mut self.receipts))
    }
}

#[cfg(test)]
#[path = "../bind/tests/file_sessions.rs"]
mod tests;
