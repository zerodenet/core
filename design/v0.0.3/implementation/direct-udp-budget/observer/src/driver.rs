//! Public APIs shared by both fixed sources; no activation or policy override.
use serde_json::{json, Value};
use std::time::Duration;
use zero_api::QueryService;
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle, RunningProxy};

#[derive(Default)]
pub struct Driver {
    proxy: Option<Proxy>,
    handle: Option<ProxyHandle>,
    running: Option<RunningProxy>,
}
impl Driver {
    pub async fn execute(&mut self, command: &Value) -> Result<Value, String> {
        match command["op"].as_str().ok_or("missing operation")? {
            "start" => {
                if self.proxy.is_some() {
                    return Err("already started".into());
                }
                let config = RuntimeConfig::parse(&command["config"].to_string())
                    .map_err(|e| e.to_string())?;
                let proxy = Proxy::new(config).map_err(|e| e.to_string())?;
                let handle =
                    ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
                self.running = Some(proxy.spawn());
                self.handle = Some(handle);
                self.proxy = Some(proxy);
                Ok(json!({"spawned": true, "pid": std::process::id()}))
            }
            "apply" => {
                let config = RuntimeConfig::parse(&command["config"].to_string())
                    .map_err(|e| e.to_string())?;
                self.handle
                    .as_ref()
                    .ok_or("not started")?
                    .apply_runtime_config_and_wait(config, Duration::from_secs(10))
                    .await?;
                Ok(json!({"acknowledged": true}))
            }
            "query" => {
                let query =
                    serde_json::from_value(command["query"].clone()).map_err(|e| e.to_string())?;
                let response = self
                    .handle
                    .as_ref()
                    .ok_or("not started")?
                    .query(query)
                    .map_err(|e| e.to_string())?;
                serde_json::to_value(response).map_err(|e| e.to_string())
            }
            "invalidate_network" => {
                let control = self
                    .proxy
                    .as_ref()
                    .ok_or("not started")?
                    .egress_interface_control();
                let before = control.generation();
                control.invalidate_network();
                Ok(
                    json!({"generation_before": before, "generation_after": control.generation(),
                    "action": "actual shared EgressControl invalidation; no system route change"}),
                )
            }
            _ => Err("unsupported operation".into()),
        }
    }
    pub async fn shutdown(&mut self) -> Result<Value, String> {
        let result = if let Some(running) = self.running.take() {
            // Keep the actual future through completion. An external timeout
            // is probe failure, never an individual native close receipt.
            running.shutdown().await.map_err(|e| e.to_string())
        } else {
            Ok(())
        };
        self.handle.take();
        self.proxy.take();
        result.map(|()| {
            json!({"public_running_proxy_shutdown_returned": true,
            "proof_scope": "public API result; not individual native close receipts"})
        })
    }
}
