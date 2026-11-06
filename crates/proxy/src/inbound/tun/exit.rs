//! Compatibility state follows the exact original main exit; never a close proof.
#[cfg(test)]
mod tests;
use crate::runtime::{Proxy, TunControl, TunInfo};
use std::sync::{Arc, Mutex, Weak};
use zero_core::component::ComponentInstanceId;
use zero_engine::EngineError;

pub(crate) struct SystemTunExitProjection {
    control: Weak<Mutex<Option<TunControl>>>,
    info: Weak<Mutex<Option<TunInfo>>>,
    error: Weak<Mutex<Option<String>>>,
    failures: tokio::sync::broadcast::WeakSender<String>,
    configured: bool,
}
impl Proxy {
    pub(crate) fn system_tun_exit_projection(&self, configured: bool) -> SystemTunExitProjection {
        SystemTunExitProjection {
            control: Arc::downgrade(&self.tun_control),
            info: Arc::downgrade(&self.tun_info),
            error: Arc::downgrade(&self.tun_last_error),
            failures: self.configured_tun_failures.downgrade(),
            configured,
        }
    }
}
impl SystemTunExitProjection {
    /// The registry's existing listener observer calls this after consuming the
    /// original main exit and classifying an unexpected successful termination.
    /// It retains the original completion/control for external physical cleanup.
    pub(crate) fn record(&self, original: ComponentInstanceId, failure: Option<&EngineError>) {
        let Some(control) = self.control.upgrade() else {
            return;
        };
        let control = control.lock().unwrap();
        let Some(control) = control
            .as_ref()
            .filter(|control| control.completion.matches_source(original))
        else {
            return;
        };
        if let Some(info) = self.info.upgrade() {
            let mut info = info.lock().unwrap();
            if info.as_ref().is_some_and(|info| info.id == control.id) {
                info.take();
            }
        }
        if let Some(failure) = failure {
            let failure = failure.to_string();
            if let Some(error) = self.error.upgrade() {
                *error.lock().unwrap() = Some(failure.clone());
            }
            if self.configured {
                if let Some(failures) = self.failures.upgrade() {
                    let _ = failures.send(failure);
                }
            }
        }
        control.shutdown.send_replace(true);
    }
}
