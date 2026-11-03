use std::io::{self, BufRead, BufReader, Write};
mod original;
use original::{Call, IpcOriginal};
use std::time::Duration;

use zero_engine::{BlockReason, FlowContext, FlowHook, FlowTraffic, SessionOutcome};

type WarningHandler = Box<dyn Fn(&str, &str) + Send + Sync>;

/// An `IpcFlowHook` delegates flow decisions to an external process over
/// a local IPC channel (Unix domain socket or Windows named pipe).
///
/// # Protocol (JSON-line)
///
/// **flow_start** — synchronous request/response:
/// ```text
/// → {"type":"check_flow","flow_id":1,...}
/// ← {"allow":true}
/// ← {"allow":false,"code":"quota","message":"daily quota exceeded"}
/// ```
///
/// **flow_end** — fire-and-forget:
/// ```text
/// → {"type":"flow_end","flow_id":1,...}
/// ```
///
/// Fail-open: if the external process is unreachable, flows are allowed.
pub struct IpcFlowHook {
    path: String,
    timeout: Duration,
    original: IpcOriginal,
    on_warning: Option<WarningHandler>,
}

impl IpcFlowHook {
    pub fn new(path: impl Into<String>, timeout: Duration) -> Self {
        Self {
            path: path.into(),
            timeout,
            original: IpcOriginal::default(),
            on_warning: None,
        }
    }

    pub fn with_warning_handler(
        mut self,
        handler: impl Fn(&str, &str) + Send + Sync + 'static,
    ) -> Self {
        self.on_warning = Some(Box::new(handler));
        self
    }

    fn emit_warning(&self, code: &str, message: &str) {
        if let Some(ref handler) = self.on_warning {
            handler(code, message);
        }
    }

    #[cfg(all(test, unix))]
    pub(crate) fn interrupt_original_for_test(&self) {
        let _callback = self.original.callback().expect("actual callback admitted");
        let _ = self.original.call(&self.path, |stream| -> io::Result<()> {
            stream.write_all(b"actual original pending callback\n")?;
            panic!("actual callback interrupted while holding its original stream");
        });
    }
    #[cfg(test)]
    pub(crate) fn native_observation_for_test(&self) -> (usize, usize, bool, bool) {
        self.original.observation()
    }
    pub(crate) fn retire(&self) -> io::Result<()> {
        self.original.retire()
    }
    pub(crate) fn confirm_original_io_closed(&self) -> io::Result<()> {
        self.original.confirm_original_io_closed()
    }
    pub(crate) fn is_retired(&self) -> bool {
        self.original.is_retired()
    }
    fn report_cleanup(&self, cleanup: io::Result<()>) {
        if let Err(error) = cleanup {
            let message = format!("IPC original cleanup unconfirmed: {error}");
            // Diagnostics cannot override an already confirmed allow/deny.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                tracing::warn!(path = %self.path, error = %error, "{message}");
                self.emit_warning("ipc_hook_cleanup_unconfirmed", &message);
            }));
        }
    }
    fn send_recv(&self, request: &serde_json::Value) -> Call<serde_json::Value> {
        let request_bytes = match serde_json::to_vec(request) {
            Ok(mut bytes) => {
                bytes.push(b'\n');
                bytes
            }
            Err(error) => {
                return Call {
                    operation: Err(io::Error::other(error)),
                    cleanup: Ok(()),
                }
            }
        };
        self.original.call(&self.path, |stream| {
            stream.set_timeout(Some(self.timeout))?;
            stream.write_all(&request_bytes)?;
            stream.flush()?;
            for line in BufReader::new(stream).lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                return serde_json::from_str(&line).map_err(io::Error::other);
            }
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "ipc hook closed connection without response",
            ))
        })
    }
    fn resolve_start_call(&self, call: Call<serde_json::Value>) -> Result<(), BlockReason> {
        self.report_cleanup(call.cleanup);
        match call.operation {
            Ok(response) => {
                if response
                    .get("allow")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true)
                {
                    Ok(())
                } else {
                    let code = response
                        .get("code")
                        .and_then(|v| v.as_str())
                        .unwrap_or("blocked")
                        .to_owned();
                    let message = response
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("blocked by external hook")
                        .to_owned();
                    Err(BlockReason::new(code, message))
                }
            }
            Err(error) => {
                let msg = format!("ipc hook unreachable ({error}); allowing flow (fail-open)");
                tracing::warn!(path = %self.path, error = %error, "{}", msg);
                self.emit_warning("ipc_hook_unreachable", &msg);
                Ok(())
            }
        }
    }

    fn send_fire_and_forget(&self, notification: &serde_json::Value) {
        let Ok(mut bytes) = serde_json::to_vec(notification) else {
            return;
        };
        bytes.push(b'\n');
        let call = self.original.call(&self.path, |stream| {
            let _ = stream.set_timeout(Some(Duration::from_secs(2)));
            stream.write_all(&bytes)
        });
        self.report_cleanup(call.cleanup);
    }
}

impl FlowHook for IpcFlowHook {
    fn on_flow_start(&self, ctx: &FlowContext) -> Result<(), BlockReason> {
        let Some(_callback) = self.original.callback() else {
            return Ok(());
        };
        let request = serde_json::json!({
            "type": "check_flow",
            "flow_id": ctx.flow_id,
            "inbound_tag": ctx.inbound_tag,
            "target_host": ctx.target_host,
            "target_port": ctx.target_port,
            "network": ctx.network,
            "protocol": ctx.protocol,
            "auth_scheme": ctx.auth.as_ref().map(|a| &a.scheme),
            "principal_key": ctx.auth.as_ref().and_then(|a| a.principal_key.as_deref()),
            "mode": ctx.mode,
        });

        self.resolve_start_call(self.send_recv(&request))
    }

    fn on_flow_end(&self, ctx: &FlowContext, outcome: SessionOutcome, stats: &FlowTraffic) {
        let Some(_callback) = self.original.callback() else {
            return;
        };
        let notification = serde_json::json!({
            "type": "flow_end",
            "flow_id": ctx.flow_id,
            "inbound_tag": ctx.inbound_tag,
            "target_host": ctx.target_host,
            "target_port": ctx.target_port,
            "network": ctx.network,
            "principal_key": ctx.auth.as_ref().and_then(|a| a.principal_key.as_deref()),
            "outcome": outcome.kind(),
            "bytes_up": stats.bytes_up,
            "bytes_down": stats.bytes_down,
            "duration_ms": stats.duration_ms,
        });
        self.send_fire_and_forget(&notification);
    }
}

#[cfg(all(test, unix))]
mod tests;
