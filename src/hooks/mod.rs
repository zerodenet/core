pub mod ipc;

use std::sync::Arc;
use std::time::Duration;

use zero_config::{ApiConfig, HookConfig};
use zero_engine::FlowHookChain;

type WarningHandler = Arc<dyn Fn(&str, &str) + Send + Sync>;

pub(crate) struct PreparedHooks {
    chain: Option<FlowHookChain>,
    originals: Vec<Arc<ipc::IpcFlowHook>>,
    retired: bool,
}
impl PreparedHooks {
    #[cfg(test)]
    pub(crate) fn originals_for_test(&self) -> &[Arc<ipc::IpcFlowHook>] {
        &self.originals
    }
    pub(crate) fn confirm_original_io_closed(&self) -> std::io::Result<()> {
        for original in &self.originals {
            original.confirm_original_io_closed()?;
        }
        Ok(())
    }
    pub(crate) fn is_ready(&self) -> bool {
        !self.retired
            && self.originals.iter().all(|hook| !hook.is_retired())
            && self.confirm_original_io_closed().is_ok()
    }
    pub(crate) fn take_chain(&mut self) -> Option<FlowHookChain> {
        self.chain.take().filter(|chain| !chain.is_empty())
    }
    pub(crate) fn retire(&mut self) -> std::io::Result<()> {
        self.retired = true;
        let mut failure = None;
        for original in &self.originals {
            if let Err(error) = original.retire() {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

/// Prepare cold hook values and retain their concrete original owners.
///
/// CLI `ipc_socket` takes precedence.  Config `api.hooks` entries are
/// added after the CLI hook.
pub(crate) fn prepare_hooks(
    ipc_socket: Option<&str>,
    api_config: &ApiConfig,
    on_warning: Option<WarningHandler>,
) -> PreparedHooks {
    let mut chain = PreparedHooks {
        chain: Some(FlowHookChain::empty()),
        originals: Vec::new(),
        retired: false,
    };

    // CLI override: single IPC hook.
    if let Some(socket_path) = ipc_socket {
        push_ipc_hook(&mut chain, socket_path, 100, &on_warning);
    }

    // Config-based hooks (only if no CLI override for IPC).
    for hook_cfg in &api_config.hooks {
        match hook_cfg {
            HookConfig::Ipc { socket, timeout_ms } => {
                if ipc_socket.is_none() {
                    push_ipc_hook(&mut chain, socket, *timeout_ms, &on_warning);
                }
            }
        }
    }

    chain
}

fn push_ipc_hook(
    chain: &mut PreparedHooks,
    socket_path: &str,
    timeout_ms: u64,
    on_warning: &Option<WarningHandler>,
) {
    let mut hook = ipc::IpcFlowHook::new(socket_path, Duration::from_millis(timeout_ms));
    if let Some(handler) = on_warning {
        let h = Arc::clone(handler);
        hook = hook.with_warning_handler(move |code, msg| h(code, msg));
    }
    let original = Arc::new(hook);
    chain.chain.as_mut().unwrap().push(original.clone());
    chain.originals.push(original);
    tracing::info!(socket = %socket_path, timeout_ms, "ipc flow hook enabled");
}
