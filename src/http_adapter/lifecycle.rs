//! The same retained server/Scope/native owner installs and retires the listener.
use super::listener::bind_control_listener;
use super::server::{run_server, ServerResources};
use super::*;
impl HttpServerHandle {
    pub(crate) fn retirement_complete(&self) -> bool {
        self.retired.load(std::sync::atomic::Ordering::Acquire)
    }
    pub(crate) async fn wait_terminated(&self) -> io::Result<()> {
        self.task.join().await
    }
    pub async fn shutdown(&self) -> io::Result<()> {
        self.task.close_admission();
        if let Some(shutdown) = self
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = shutdown.send(());
        }
        let server = self.task.join().await;
        let tasks = self
            .scope
            .lock()
            .await
            .shutdown(crate::ipc::owner::close_deadline())
            .await;
        let mut original = self.original.lock().await;
        let native = match original.as_mut() {
            Some(original) => original
                .wait_closed(crate::ipc::owner::close_deadline())
                .await
                .map_err(io::Error::other)
                .and_then(|report| crate::ipc::owner::confirmed_report("HTTP native", report)),
            None => Ok(()),
        };
        let scope = crate::ipc::owner::confirmed_report("HTTP Scope", tasks);
        if scope.is_ok() && native.is_ok() {
            self.connections.confirm_all_closed();
            self.retired
                .store(true, std::sync::atomic::Ordering::Release);
        }
        crate::ipc::owner::finish_work([server, scope, native])
    }
}

pub fn prepare_http_server_with_budget(
    listen: &str,
    auth: Option<HttpServerAuth>,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
) -> Arc<HttpServerHandle> {
    // The finite validated ceiling allocates no idle socket/task. The shared
    // actual root owner admits occupancy under the current configured cap.
    let sockets = 4096;
    Arc::new(HttpServerHandle {
        shutdown: std::sync::Mutex::new(None),
        task: crate::ipc::owner::ControlTask::empty(),
        original: Arc::new(tokio::sync::Mutex::new(None)),
        scope: Arc::new(tokio::sync::Mutex::new(TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(sockets * 3 + 1).unwrap(),
        ))),
        listen: listen.to_owned(),
        auth: auth.map(Arc::new),
        started: std::sync::atomic::AtomicBool::new(false),
        retired: std::sync::atomic::AtomicBool::new(false),
        #[cfg(test)]
        bound: std::sync::Mutex::new(None),
        budget,
        connections: Arc::new(connections::Connections::default()),
    })
}
impl HttpServerHandle {
    pub(crate) async fn start(&self, proxy_handle: ProxyHandle) -> io::Result<()> {
        if self.started.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return Err(io::Error::other("HTTP original owner already started"));
        }
        let registration = self.task.reserve()?;
        let sockets = NonZeroUsize::new(4096).unwrap();
        let per_entry = NonZeroUsize::new(3).unwrap();
        let mut scope = self.scope.lock().await;
        let listener =
            bind_control_listener(&self.listen, &mut scope, sockets, per_entry, &self.original)
                .await?;
        drop(scope);
        let local_addr = listener.local_addr().map_err(io::Error::other)?;
        #[cfg(test)]
        {
            *self.bound.lock().unwrap() = Some(local_addr);
        }
        let receiver = self
            .original
            .lock()
            .await
            .as_ref()
            .expect("cold HTTP listener installed before bind")
            .accepted_task_receiver()
            .expect("declared HTTP accepted graph");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        *self
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(shutdown_tx);
        let task_scope = self.scope.clone();
        let task_original = self.original.clone();
        let auth = self.auth.clone();
        let budget = self.budget.clone();
        let connections = self.connections.clone();
        let (begin, started) = oneshot::channel();
        registration.install(tokio::spawn(async move {
            started
                .await
                .map_err(|_| io::Error::other("HTTP startup gate closed"))?;
            run_server(
                listener,
                proxy_handle,
                auth,
                shutdown_rx,
                ServerResources {
                    scope: task_scope,
                    original: task_original,
                    receiver,
                    budget,
                    connections,
                },
            )
            .await
        }));
        begin
            .send(())
            .map_err(|_| io::Error::other("original HTTP task stopped during startup"))?;
        info!(listen = %local_addr, "http api server ready");
        Ok(())
    }
}
