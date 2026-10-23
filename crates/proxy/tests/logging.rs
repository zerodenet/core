mod support;
#[cfg(feature = "shadowsocks")]
#[path = "logging/upstream_failure.rs"]
mod upstream_failure;

use std::io::{self, Write};
use std::sync::{Arc, Mutex, Once, OnceLock};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing_subscriber::fmt::MakeWriter;
use zero_config::RuntimeConfig;
use zero_proxy::Proxy as Engine;

use support::{free_port, spawn_engine, wait_for, wait_for_listener};

static LOG_INIT: Once = Once::new();
static LOG_BUFFER: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();

#[tokio::test]
async fn emits_session_logs_for_successful_proxy_traffic() {
    let buffer = init_test_tracing();

    let echo_port = free_port();
    let proxy_port = free_port();

    let echo_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", echo_port))
            .await
            .expect("bind echo");
        let (mut stream, _) = listener.accept().await.expect("accept echo");
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).await.expect("read echo");
        stream.write_all(&buf).await.expect("write echo");
    });

    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [],
            "route": {{
                "rules": [],
                "final": {{ "type": "direct" }}
            }}
        }}"#
    ))
    .expect("parse config");
    let engine = Engine::new(config).expect("build engine");
    let handle = spawn_engine(engine);

    wait_for_listener(proxy_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect socks5");
    client
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("write auth");

    let mut auth = [0_u8; 2];
    client.read_exact(&mut auth).await.expect("read auth");
    assert_eq!(auth, [0x05, 0x00]);

    let request = [
        0x05,
        0x01,
        0x00,
        0x01,
        127,
        0,
        0,
        1,
        ((echo_port >> 8) & 0xff) as u8,
        (echo_port & 0xff) as u8,
    ];
    client.write_all(&request).await.expect("write request");

    let mut response = [0_u8; 10];
    client
        .read_exact(&mut response)
        .await
        .expect("read response");
    assert_eq!(response[1], 0x00);

    client.write_all(b"ping").await.expect("write payload");
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).await.expect("read payload");
    assert_eq!(&echoed, b"ping");
    drop(client);

    wait_for("session to finish", || {
        let stats = handle.stats_snapshot();
        stats.active_sessions == 0 && stats.completed_sessions == 1
    })
    .await;

    handle.shutdown().await.expect("shutdown engine");
    let _ = echo_task.await;

    let logs =
        String::from_utf8(buffer.lock().expect("log buffer lock").clone()).expect("utf-8 logs");
    assert!(logs.contains("zero-proxy started"), "{logs}");
    assert!(logs.contains("session accepted"), "{logs}");
    assert!(logs.contains("session finished"), "{logs}");
    assert!(logs.contains("duration_ms="), "{logs}");
    assert!(logs.contains("bytes_up="), "{logs}");
    assert!(logs.contains("bytes_down="), "{logs}");
}

fn init_test_tracing() -> Arc<Mutex<Vec<u8>>> {
    let buffer = LOG_BUFFER
        .get_or_init(|| Arc::new(Mutex::new(Vec::new())))
        .clone();

    LOG_INIT.call_once(|| {
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

        tracing::subscriber::set_global_default(subscriber)
            .expect("install test tracing subscriber");
    });

    buffer
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
