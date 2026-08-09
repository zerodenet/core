use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tracing_subscriber::fmt::MakeWriter;
use zero_api::{
    event_type, CommandRequest, CommandService, DiagnosticsProbeOutboundCommand, EventFilter,
    EventSource,
};
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle};

#[tokio::test(flavor = "multi_thread")]
async fn diagnostic_outbound_probe_does_not_mutate_urltest_policy() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind probe server");
    let port = listener.local_addr().expect("probe address").port();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept probe");
        let mut request = [0_u8; 512];
        let _ = socket.read(&mut request).await;
        tokio::io::AsyncWriteExt::write_all(
            &mut socket,
            b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n",
        )
        .await
        .expect("write probe response");
    });
    let url = format!("http://127.0.0.1:{port}/generate_204");
    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [],
            "outbounds": [
                {{ "tag": "node-a", "protocol": {{ "type": "direct" }} }},
                {{ "tag": "node-b", "protocol": {{ "type": "direct" }} }}
            ],
            "outbound_groups": [{{
                "tag": "auto",
                "type": "url_test",
                "outbounds": ["node-a", "node-b"],
                "url": "{url}",
                "interval_seconds": 3600,
                "tolerance_ms": 50
            }}],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("parse config");
    let proxy = Proxy::new(config).expect("build proxy");
    let engine = proxy.engine().clone();
    let before = engine.export_status().config.outbound_groups;
    let subscriber = engine
        .subscribe(EventFilter {
            event_types: vec![event_type::POLICY_PROBE_COMPLETED.to_owned()],
            ..EventFilter::default()
        })
        .expect("subscribe to policy probes");
    let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy);

    let response = tokio::task::block_in_place(|| {
        handle.execute(CommandRequest::DiagnosticsProbeOutbound(
            DiagnosticsProbeOutboundCommand {
                target_tag: "node-a".to_owned(),
                url: Some(url),
                operation_id: Some("isolated-diagnostic".to_owned()),
            },
        ))
    })
    .expect("execute outbound diagnostic");
    let result = response.result.expect("diagnostic result");

    assert_eq!(result["operation_kind"], "diagnostic_outbound");
    assert_eq!(result["terminal_status"], "succeeded");
    assert_eq!(result["reachable"], true);
    assert_eq!(result["timeout_ms"], 5_000);
    assert_eq!(engine.export_status().config.outbound_groups, before);
    assert!(
        subscriber.try_recv().is_none(),
        "single-outbound diagnostics must not emit policy.probe.completed"
    );
    assert!(!result.to_string().to_ascii_lowercase().contains("urltest"));
    server.await.expect("probe server task");
}

#[tokio::test(flavor = "multi_thread")]
async fn diagnostic_outbound_failure_has_stable_code_and_correlated_core_logs() {
    let (handle, engine) = diagnostic_handle();
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(BufferMakeWriter {
            buffer: buffer.clone(),
        })
        .with_ansi(false)
        .with_target(false)
        .without_time()
        .compact()
        .finish();

    let response = tracing::subscriber::with_default(subscriber, || {
        tokio::task::block_in_place(|| {
            handle.execute(CommandRequest::DiagnosticsProbeOutbound(
                DiagnosticsProbeOutboundCommand {
                    target_tag: "direct".to_owned(),
                    url: Some("https://example.com/generate_204".to_owned()),
                    operation_id: Some("diagnostic-log-1".to_owned()),
                },
            ))
        })
    })
    .expect("execute invalid outbound diagnostic");
    let result = response.result.expect("diagnostic result");

    assert_eq!(result["operation_id"], "diagnostic-log-1");
    assert_eq!(result["core_instance_id"], engine.core_instance_id());
    assert_eq!(result["operation_kind"], "diagnostic_outbound");
    assert_eq!(result["terminal_status"], "failed");
    assert_eq!(result["reachable"], false);
    assert_eq!(result["error_code"], "invalid_probe_url");
    assert_eq!(result["timeout_ms"], 5_000);
    assert!(!result.to_string().to_ascii_lowercase().contains("urltest"));

    let logs = String::from_utf8(buffer.lock().expect("log buffer lock").clone())
        .expect("utf-8 diagnostic logs");
    assert!(
        logs.contains("method=\"diagnostics.probe_outbound\""),
        "{logs}"
    );
    assert!(logs.contains("operation_id=\"diagnostic-log-1\""), "{logs}");
    assert!(logs.contains("phase=\"started\""), "{logs}");
    assert!(logs.contains("phase=\"completed\""), "{logs}");
    assert!(logs.contains("error_code=\"invalid_probe_url\""), "{logs}");
    assert!(logs.contains("timeout_ms=5000"), "{logs}");
    assert!(!logs.to_ascii_lowercase().contains("urltest"), "{logs}");
}

#[tokio::test(flavor = "multi_thread")]
async fn diagnostic_outbound_timeout_is_neutral_and_reports_limit() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stalled probe server");
    let port = listener.local_addr().expect("probe address").port();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept probe");
        let mut request = [0_u8; 512];
        let _ = socket.read(&mut request).await;
        tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    });
    let (handle, _) = diagnostic_handle();

    let response = tokio::task::block_in_place(|| {
        handle.execute(CommandRequest::DiagnosticsProbeOutbound(
            DiagnosticsProbeOutboundCommand {
                target_tag: "direct".to_owned(),
                url: Some(format!("http://127.0.0.1:{port}/stalled")),
                operation_id: Some("diagnostic-timeout-1".to_owned()),
            },
        ))
    })
    .expect("execute stalled outbound diagnostic");
    let result = response.result.expect("diagnostic result");

    assert_eq!(result["terminal_status"], "failed");
    assert_eq!(result["reachable"], false);
    assert_eq!(result["error_code"], "probe_timeout");
    assert_eq!(result["timeout_ms"], 5_000);
    assert!(result["error"]
        .as_str()
        .expect("error message")
        .contains("outbound latency probe timed out after 5000 ms"));
    assert!(!result.to_string().to_ascii_lowercase().contains("urltest"));
    server.abort();
}

fn diagnostic_handle() -> (ProxyHandle, zero_engine::Engine) {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [{ "tag": "direct", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let proxy = Proxy::new(config).expect("build proxy");
    let engine = proxy.engine().clone();
    (
        ProxyHandle::new(EngineHandle::new(engine.clone()), proxy),
        engine,
    )
}

#[derive(Clone)]
struct BufferMakeWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl<'a> MakeWriter<'a> for BufferMakeWriter {
    type Writer = BufferWriter;

    fn make_writer(&'a self) -> Self::Writer {
        BufferWriter {
            buffer: self.buffer.clone(),
        }
    }
}

struct BufferWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl Write for BufferWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer
            .lock()
            .expect("log buffer lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
