//! Windows Named Pipe IPC over the actual retained Win32 original.
use super::connection;
use std::{
    io,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{sync::oneshot, task::JoinSet};
use tracing::{debug, info, warn};
use zero_platform_tokio::{
    contract::{scope::TaskScope, CancellationToken},
    ControlledPipe, ControlledPipeOwner,
};
use zero_proxy::ProxyHandle;
mod owner;
use owner::Originals;
pub const PIPE_NAME: &str = r"\\.\pipe\zero-control";
pub struct IpcServerHandle {
    shutdown: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    task: super::owner::ControlTask,
    path: PathBuf,
    started: AtomicBool,
    retired: AtomicBool,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
    originals: Arc<Originals>,
    scope: Arc<tokio::sync::Mutex<TaskScope>>,
    opening: std::sync::Mutex<Option<(ControlledPipe, Arc<owner::Entry>)>>,
    scope_fault: std::sync::Mutex<Option<Arc<io::Error>>>,
}
impl IpcServerHandle {
    pub(crate) fn retirement_complete(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }
    pub(crate) async fn wait_terminated(&self) -> io::Result<()> {
        self.task.join().await
    }
    pub async fn shutdown(&self) -> io::Result<()> {
        self.task.close_admission();
        if let Some(stop) = self
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = stop.send(());
        }
        self.originals.request_stop();
        self.opening
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let server = self.task.join().await;
        let tasks = self
            .scope
            .lock()
            .await
            .shutdown(super::owner::close_deadline())
            .await;
        let native = self.originals.reap(true).await;
        if tasks.is_complete()
            && native.is_ok()
            && self.originals.is_empty()
            && self.scope.lock().await.is_drained()
        {
            // Server join consumed above; every unique view, std worker and
            // Scope witness returned, and every actual native receipt matched.
            self.retired.store(true, Ordering::Release);
        }
        let mut failures = Vec::new();
        if let Err(error) = server {
            failures.push(error);
        }
        if !tasks.is_complete() || !tasks.failed_tasks.is_empty() {
            self.scope_fault
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_or_insert_with(|| Arc::new(io::Error::other(owner::ScopeFault(tasks))));
        }
        if let Some(error) = self
            .scope_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            failures.push(io::Error::new(error.kind(), error.clone()));
        }
        if let Err(error) = native {
            failures.push(error);
        }
        owner::finish(failures)
    }
    pub(crate) async fn start(&self, handle: ProxyHandle) -> io::Result<()> {
        if self.started.swap(true, Ordering::AcqRel) {
            return Err(io::Error::other("IPC original owner already started"));
        }
        let registration = self.task.reserve()?;
        let registrar = self.scope.lock().await.registrar();
        let entry = self
            .originals
            .prepare(&self.path, registrar, &self.budget)?;
        let pipe = match entry.original.open().await {
            Ok(pipe) => pipe,
            Err(error) => {
                self.originals.failed_open(&entry);
                return Err(error);
            }
        };
        *self
            .opening
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((pipe, entry));
        let (stop, stopped) = oneshot::channel();
        *self
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(stop);
        let pending = self
            .opening
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("actual opening pipe");
        let originals = self.originals.clone();
        let scope = self.scope.clone();
        let budget = self.budget.clone();
        let path = self.path.clone();
        let (begin, started) = oneshot::channel();
        registration.install(tokio::spawn(async move {
            started
                .await
                .map_err(|_| io::Error::other("IPC startup gate closed"))?;
            run_server(path, handle, stopped, budget, scope, originals, pending).await
        }));
        begin
            .send(())
            .map_err(|_| io::Error::other("original IPC task stopped during startup"))?;
        info!(pipe=%self.path.display(),"ipc server ready");
        Ok(())
    }
}
pub fn default_socket_path() -> Option<PathBuf> {
    Some(PathBuf::from(PIPE_NAME))
}
pub fn resolve_socket_path(explicit: Option<&str>) -> io::Result<PathBuf> {
    Ok(PathBuf::from(explicit.unwrap_or(PIPE_NAME)))
}
pub fn prepare_ipc_server_with_budget(
    path: &std::path::Path,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
) -> Arc<IpcServerHandle> {
    // Config permits at most4096 live connections. This cold Scope is an on-
    // demand registration ceiling, never admission. Every original/worker
    // independently retains its real shared root budget charge through close.
    let scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        std::num::NonZeroUsize::new(4096).unwrap(),
    );
    Arc::new(IpcServerHandle {
        shutdown: std::sync::Mutex::new(None),
        task: super::owner::ControlTask::empty(),
        path: path.to_owned(),
        started: AtomicBool::new(false),
        retired: AtomicBool::new(false),
        budget,
        originals: Arc::new(Originals::default()),
        scope: Arc::new(tokio::sync::Mutex::new(scope)),
        opening: std::sync::Mutex::new(None),
        scope_fault: std::sync::Mutex::new(None),
    })
}
async fn run_server(
    path: PathBuf,
    handle: ProxyHandle,
    mut shutdown: oneshot::Receiver<()>,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
    scope: Arc<tokio::sync::Mutex<TaskScope>>,
    originals: Arc<Originals>,
    first: (ControlledPipe, Arc<owner::Entry>),
) -> io::Result<()> {
    let mut pending = Some(first);
    let mut connections = JoinSet::<io::Result<()>>::new();
    let active = Arc::new(AtomicU64::new(0));
    let stop = CancellationToken::new();
    let mut failures = Vec::new();
    let mut scope = scope.lock().await;
    let work=super::owner::catch_work(async {
        loop {
            originals.reap(false).await?;
            if pending.is_none() {
                match originals.prepare(&path,scope.registrar(),&budget) {
                    Ok(entry)=>{
                        let opened=tokio::select! {biased;_=&mut shutdown=>break,opened=entry.original.open()=>opened};
                        match opened {Ok(pipe)=>pending=Some((pipe,entry)),Err(error)=>{originals.failed_open(&entry);failures.push(error);break;}}
                    }
                    Err(error) if error.kind()==io::ErrorKind::WouldBlock=>{},
                    Err(error)=>{failures.push(error);break;}
                }
            }
            tokio::select! {biased;
                _=&mut shutdown=>break,
                connected=async {if let Some((pipe,_))=&pending {pipe.connect().await} else {std::future::pending::<io::Result<()>>().await}}=>{
                    connected?;
                    let (pipe,entry)=pending.take().unwrap();let handle=handle.clone();let active=active.clone();let path=path.clone();let stopped=stop.child_token();let budget=budget.clone();
                    let n=active.fetch_add(1,Ordering::AcqRel)+1;super::events::emit_connected(&handle,n,&path.to_string_lossy());
                    let local=entry.connection_budget().expect("actual pipe connection admission");
                    let registered=local.clone();
                    let (start,started)=tokio::sync::oneshot::channel();
                    let task=connections.spawn(async move {
                        if started.await.is_err(){return Ok(());}
                        let result=connection::handle_ipc_connection_with_connection_until(pipe,handle.clone(),stopped,budget,local).await;
                        let n=active.fetch_sub(1,Ordering::AcqRel)-1;let error=result.as_ref().err().map(ToString::to_string);
                        super::events::emit_disconnected(&handle,n,&path.to_string_lossy(),error.as_deref());result
                    });
                    *entry.task.lock().unwrap_or_else(std::sync::PoisonError::into_inner)=Some(task.id());
                    assert!(registered.register_command_task(task.clone()),"same original pipe connection task before gate");
                    let _=start.send(());
                }
                joined=connections.join_next_with_id(),if !connections.is_empty()=>{
                    match joined.unwrap() {
                        Ok((id,result))=>{originals.observe_joined(id);if let Err(error)=result {if connection::is_transient_disconnect(&error) || connection::is_invalid_input_frame(&error) {debug!(%error,"IPC original observed peer disconnect");} else {failures.push(error);break;}}}
                        Err(error)=>{originals.observe_joined(error.id());failures.push(io::Error::other(error));break;}
                    }
                }
                exit=scope.next_exit(), if !scope.is_joined()=>{
                    if let Some(exit)=exit {if let Err(error)=exit.result {if error.impact==zero_platform_tokio::contract::Impact::Component {failures.push(io::Error::other(error));break;} else {warn!(%error,"original pipe worker exited after client IO failure");}}}
                }
                _=tokio::time::sleep(std::time::Duration::from_millis(25)),if pending.is_none()=>{},
            }
        }
        io::Result::Ok(())
    }).await;
    stop.cancel();
    drop(pending);
    originals.request_stop();
    while let Some(joined) = connections.join_next_with_id().await {
        match joined {
            Ok((id, result)) => {
                originals.observe_joined(id);
                if let Err(error) = result {
                    if !connection::is_transient_disconnect(&error)
                        && !connection::is_invalid_input_frame(&error)
                    {
                        failures.push(error);
                    }
                }
            }
            Err(error) => {
                originals.observe_joined(error.id());
                failures.push(io::Error::other(error));
            }
        }
    }
    scope.begin_shutdown();
    while let Some(exit) = scope.next_exit().await {
        if let Err(error) = exit.result {
            if error.impact == zero_platform_tokio::contract::Impact::Component {
                failures.push(io::Error::other(error));
            }
        }
    }
    drop(scope);
    let native = originals.reap(true).await;
    if let Err(error) = native {
        failures.push(error);
    }
    match work {
        Ok(Err(error)) => failures.push(error),
        Ok(Ok(())) => {}
        Err(payload) => std::panic::resume_unwind(payload),
    }
    info!(pipe=%path.display(),"ipc server stopped");
    owner::finish(failures)
}

#[cfg(test)]
mod tests;
