use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::Notify,
    time::{timeout, Duration},
};
use zero_api::RawResponse;
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{ConfigApplyReconciler, ConfigReconcileResult, Proxy, ProxyHandle, RunningProxy};

pub struct Fixture {
    pub handle: ProxyHandle,
    pub running: RunningProxy,
    pub config: Value,
    pub port: u16,
}

impl Fixture {
    pub async fn new() -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = socket.local_addr().unwrap().port();
        let config = json!({"endpoints":[{
            "tag":"ipc-test", "directions":{"inbound":true,"outbound":true},
            "listen":{"address":"127.0.0.1","port":port},
            "protocol":{"type":"wireguard",
                "private_key":"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
                "addresses":["10.0.0.1/32"], "peers":[{
                    "public_key":"AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=",
                    "endpoint":"127.0.0.1:9","allowed_ips":["10.0.0.0/24"]}]}
        }],"route":{"rules":[],"final":{"type":"direct"}}});
        let proxy = Proxy::new(RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap();
        let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
        drop(socket);
        let running = proxy.clone().spawn();
        let mut ipc = Ipc::new(handle.clone());
        timeout(Duration::from_secs(5), async {
            loop {
                if ipc.endpoint().await["state"] == "running" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        ipc.close().await;
        Self {
            handle,
            running,
            config,
            port,
        }
    }
}

pub struct Ipc {
    stream: BufReader<UnixStream>,
    pub task: tokio::task::JoinHandle<std::io::Result<()>>,
    next_id: u64,
}

impl Ipc {
    pub fn with_auth(handle: ProxyHandle, auth: zero_api::AuthContext) -> Self {
        let (client, server) = UnixStream::pair().unwrap();
        let task = tokio::spawn(crate::ipc::connection::handle_ipc_connection_with_auth(
            server, handle, auth,
        ));
        Self {
            stream: BufReader::new(client),
            task,
            next_id: 0,
        }
    }

    pub fn new(handle: ProxyHandle) -> Self {
        let (client, server) = UnixStream::pair().unwrap();
        let task = tokio::spawn(crate::ipc::connection::handle_ipc_connection(
            server, handle,
        ));
        Self {
            stream: BufReader::new(client),
            task,
            next_id: 0,
        }
    }
    pub async fn send(&mut self, mut frame: Value) -> u64 {
        self.next_id += 1;
        frame["id"] = self.next_id.into();
        self.stream
            .get_mut()
            .write_all(format!("{frame}\n").as_bytes())
            .await
            .unwrap();
        self.next_id
    }
    pub async fn read(&mut self) -> RawResponse {
        let mut line = String::new();
        assert!(
            timeout(Duration::from_secs(5), self.stream.read_line(&mut line))
                .await
                .unwrap()
                .unwrap()
                > 0
        );
        let response: RawResponse = serde_json::from_str(&line).unwrap();
        assert_eq!(response.id, Some(self.next_id.into()));
        response
    }
    pub async fn command(&mut self, method: &str, params: Value) -> RawResponse {
        self.send(json!({"type":"command","method":method,"params":params}))
            .await;
        self.read().await
    }
    pub async fn query(&mut self, request: Value) -> Value {
        self.send(json!({"type":"query","request":request})).await;
        let response = self.read().await;
        assert!(response.ok, "{:?}", response.error);
        response.result.unwrap()
    }
    pub async fn endpoint(&mut self) -> Value {
        self.query(json!({"endpoint":{"endpoint_id":"endpoint:ipc-test"}}))
            .await["endpoint"]
            .clone()
    }
    pub async fn traffic(&mut self) -> Value {
        self.query(json!({"traffic_stat":{"scope":{"kind":"global"}}}))
            .await["traffic_stat"]
            .clone()
    }
    pub async fn assert_pending(&mut self) {
        let mut line = String::new();
        assert!(
            timeout(Duration::from_millis(100), self.stream.read_line(&mut line))
                .await
                .is_err(),
            "command bypassed reconciliation: {line}"
        );
    }
    pub async fn close(mut self) {
        self.stream.get_mut().shutdown().await.unwrap();
        timeout(Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

pub fn state(enabled: bool) -> Value {
    json!({"endpoint_id":"endpoint:ipc-test", "enabled":enabled})
}

pub fn conditional(endpoint: &Value, mut params: Value) -> Value {
    params["expected_core_instance_id"] = endpoint["core_instance_id"].clone();
    params["expected_intent_revision"] = endpoint["intent_revision"].clone();
    params
}

pub fn applied(response: RawResponse) -> Value {
    assert!(response.ok, "{:?}", response.error);
    let result = response.result.unwrap();
    assert_eq!(result["accepted"], true);
    assert_eq!(result["result"]["applied"], true);
    assert_eq!(result["result"]["reconciled"], true);
    result["result"]["endpoint"].clone()
}

#[derive(Default)]
pub struct PausedReconciler {
    armed: AtomicBool,
    pub entered: Notify,
    pub resume: Notify,
}

impl PausedReconciler {
    pub fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
    pub async fn wait(&self) {
        timeout(Duration::from_secs(5), self.entered.notified())
            .await
            .unwrap();
    }
}

#[async_trait::async_trait]
impl ConfigApplyReconciler for PausedReconciler {
    fn validate(&self, _: &RuntimeConfig, _: &RuntimeConfig) -> Result<(), String> {
        Ok(())
    }
    async fn reconcile(&self, _: Arc<RuntimeConfig>) -> Result<ConfigReconcileResult, String> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        Ok(ConfigReconcileResult::default())
    }
}
