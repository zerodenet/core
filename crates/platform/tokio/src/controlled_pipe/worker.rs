use super::*;
use std::sync::Condvar;
#[derive(Default)]
pub(super) struct Worker {
    handle: Option<std::thread::JoinHandle<io::Result<()>>>,
    pub(super) joined: Option<Result<(), Arc<io::Error>>>,
    pub(super) exited: bool,
}
#[derive(Debug)]
struct WorkerPanic(Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Display for WorkerPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let payload = lock(&self.0);
        if let Some(message) = payload.downcast_ref::<String>() {
            write!(f, "original pipe worker panicked: {message}")
        } else if let Some(message) = payload.downcast_ref::<&str>() {
            write!(f, "original pipe worker panicked: {message}")
        } else {
            f.write_str("original pipe worker panicked (actual payload retained)")
        }
    }
}
impl std::error::Error for WorkerPanic {}
struct Exited(Arc<Shared>);
impl Drop for Exited {
    fn drop(&mut self) {
        lock(&self.0.worker).exited = true;
        self.0.changed.notify_waiters();
    }
}
pub(super) fn start(shared: &Arc<Shared>) -> io::Result<()> {
    let latch = Arc::new((Mutex::new(false), Condvar::new()));
    let gate = latch.clone();
    let original = shared.clone();
    let handle = match std::thread::Builder::new()
        .name("zero-ipc-pipe".into())
        .stack_size(128 * 1024)
        .spawn(move || {
            let _exit = Exited(original.clone());
            let (started, changed) = &*gate;
            let mut started = lock(started);
            while !*started {
                started = changed
                    .wait(started)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            drop(started);
            let result = lock(&original.native).run(&original).map_err(Arc::new);
            if let Err(error) = &result {
                lock(&original.state)
                    .fault
                    .get_or_insert_with(|| error.clone());
            }
            original.notify_io();
            result.map_err(|error| retained(&error))
        }) {
        Ok(handle) => handle,
        Err(error) => {
            let error = Arc::new(error);
            lock(&shared.worker).joined = Some(Err(error.clone()));
            lock(&shared.state).startup = Some(Err(error.clone()));
            return Err(retained(&error));
        }
    };
    // The thread cannot issue any native operation until its actual handle
    // and original Scope join guard have both been durably installed.
    lock(&shared.worker).handle = Some(handle);
    let joining = shared.clone();
    let registered = shared
        .tasks
        .spawn_tracked(TaskClass::Auxiliary, async move {
            loop {
                let notified = joining.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                join_if_finished(&joining);
                if let Some(result) = &lock(&joining.worker).joined {
                    return result.as_ref().map(|_| ()).map_err(|error| {
                        if error
                            .get_ref()
                            .is_some_and(|source| source.downcast_ref::<WorkerPanic>().is_some())
                        {
                            ChannelError::new(Stage::Receive, Reason::Internal, Impact::Component)
                                .with_source(retained(error))
                        } else {
                            failure(retained(error))
                        }
                    });
                }
                let exited = lock(&joining.worker).exited;
                if exited {
                    tokio::task::yield_now().await;
                } else {
                    tokio::select! { biased;
                        _ = joining.scope_stop.cancelled(), if !joining.stop.load(Ordering::Acquire) => joining.request_stop(),
                        _ = notified => {},
                    }
                }
            }
        });
    match registered {
        Ok((_, witness)) => *lock(&shared.witness) = Some(witness),
        Err(error) => {
            shared.request_stop();
            let fault = Arc::new(io::Error::other(error));
            lock(&shared.state).startup = Some(Err(fault.clone()));
            *lock(&latch.0) = true;
            latch.1.notify_one();
            return Err(retained(&fault));
        }
    }
    *lock(&latch.0) = true;
    latch.1.notify_one();
    Ok(())
}
pub(super) fn join_if_finished(shared: &Shared) {
    let mut worker = lock(&shared.worker);
    if worker.joined.is_some()
        || worker
            .handle
            .as_ref()
            .is_none_or(|handle| !handle.is_finished())
    {
        return;
    }
    let actual = worker
        .handle
        .take()
        .expect("same installed original pipe thread");
    let result = match actual.join() {
        Ok(result) => result.map_err(Arc::new),
        Err(payload) => Err(Arc::new(io::Error::other(WorkerPanic(Mutex::new(payload))))),
    };
    if let Err(error) = &result {
        let mut state = lock(&shared.state);
        state.fault.get_or_insert_with(|| error.clone());
        if state.startup.is_none() {
            state.startup = Some(Err(error.clone()));
        }
    }
    worker.joined = Some(result);
    drop(worker);
    shared.notify_io();
}
