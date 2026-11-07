//! Concrete control server originals in the existing application owner. These
//! slots are populated while cold, before listener bind or resolver polling.
use super::*;
use std::io;
#[derive(Default, Clone)]
pub(super) struct ControlServers {
    pub(super) ipc: Option<Arc<crate::ipc::IpcServerHandle>>,
    #[cfg(feature = "status-api")]
    pub(super) http: Option<Arc<crate::http_adapter::HttpServerHandle>>,
    #[cfg(feature = "grpc-api")]
    pub(super) grpc: Option<Arc<zero_grpc::GrpcHandle>>,
}
impl ApplicationServices {
    pub(in crate::application) fn control_budget(
        &self,
    ) -> Arc<zero_runtime::configuration::ControlBudget> {
        self.control_budget.clone()
    }
    pub(in crate::application) fn retain_control_startup(self: &Arc<Self>) -> io::Result<()> {
        APPLICATION_STARTUP.install(self.clone())
    }
    pub(in crate::application) fn finish_control_startup(self: &Arc<Self>) {
        APPLICATION_STARTUP.clear(self);
    }
    pub(in crate::application) fn install_ipc(
        &self,
        original: Arc<crate::ipc::IpcServerHandle>,
    ) -> io::Result<()> {
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if control.ipc.is_some() {
            return Err(io::Error::other("original IPC server still installed"));
        }
        control.ipc = Some(original);
        Ok(())
    }
    #[cfg(feature = "status-api")]
    pub(in crate::application) fn install_http(
        &self,
        original: Arc<crate::http_adapter::HttpServerHandle>,
    ) -> io::Result<()> {
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if control.http.is_some() {
            return Err(io::Error::other("original HTTP server still installed"));
        }
        control.http = Some(original);
        Ok(())
    }
    #[cfg(feature = "grpc-api")]
    pub(in crate::application) fn install_grpc(
        &self,
        original: Arc<zero_grpc::GrpcHandle>,
    ) -> io::Result<()> {
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if control.grpc.is_some() {
            return Err(io::Error::other("original gRPC server still installed"));
        }
        control.grpc = Some(original);
        Ok(())
    }
    pub(in crate::application) fn control_retirement_complete(&self) -> bool {
        let originals = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if originals.ipc.is_some() {
            return false;
        }
        #[cfg(feature = "status-api")]
        if originals.http.is_some() {
            return false;
        }
        #[cfg(feature = "grpc-api")]
        if originals.grpc.is_some() {
            return false;
        }
        drop(originals);
        if !self.embedded_commands.retirement_complete() {
            return false;
        }
        self.control_budget.confirm_released().is_ok()
    }
    pub(in crate::application) async fn shutdown_control(self: &Arc<Self>) -> io::Result<()> {
        self.control_budget.close_admission();
        let originals = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut outcomes = Vec::new();
        self.embedded_commands.begin_shutdown();
        if let Some(original) = originals.ipc {
            outcomes.push(original.shutdown().await);
            if original.retirement_complete() {
                self.control
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .ipc = None;
            }
        }
        #[cfg(feature = "status-api")]
        if let Some(original) = originals.http {
            outcomes.push(original.shutdown().await);
            if original.retirement_complete() {
                self.control
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .http = None;
            }
        }
        #[cfg(feature = "grpc-api")]
        if let Some(original) = originals.grpc {
            outcomes.push(original.shutdown().await);
            if original.retirement_complete() {
                self.control
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .grpc = None;
            }
        }
        // Parent control tasks must really join/ACK before their command
        // records can return capacity; do not wait for those ACKs while the
        // original servers have not yet received stop.
        outcomes.push(
            self.embedded_commands
                .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(5))
                .await
                .map_err(std::io::Error::other),
        );
        outcomes.push(self.control_budget.retire_candidate_after_shutdown().await);
        if !self.control_retirement_complete() {
            // Unknown native/users/worker or budget debt retains the exact
            // application. A consumed, fully retired fault is still returned,
            // and does not become an unclosable permanent startup barrier.
            outcomes.push(APPLICATION_STARTUP.retain(self.clone()));
        }
        outcomes.push(self.control_budget.confirm_released());
        crate::ipc::owner::finish_work(outcomes)
    }
}

impl ApplicationServices {
    pub(in crate::application) async fn wait_control_terminated(&self) -> io::Result<()> {
        let originals = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let ipc = async {
            if let Some(original) = originals.ipc {
                return original.wait_terminated().await;
            }
            std::future::pending::<io::Result<()>>().await
        };
        let http = async {
            #[cfg(feature = "status-api")]
            if let Some(original) = originals.http {
                return original.wait_terminated().await;
            }
            std::future::pending::<io::Result<()>>().await
        };
        let grpc = async {
            #[cfg(feature = "grpc-api")]
            if let Some(original) = originals.grpc {
                return original.wait_terminated().await;
            }
            std::future::pending::<io::Result<()>>().await
        };
        let monitor = async {
            #[cfg(feature = "event-dispatcher")]
            {
                return self.wait_status_failure().await;
            }
            #[cfg(not(feature = "event-dispatcher"))]
            std::future::pending::<io::Result<()>>().await
        };
        tokio::select! { result = ipc => result, result = http => result, result = grpc => result, result = monitor => result }
    }
}
