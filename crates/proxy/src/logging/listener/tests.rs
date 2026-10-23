use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use tracing_subscriber::fmt::MakeWriter;
use zero_engine::EngineError;

use super::{log_listener_connection_error, INBOUND_ACCEPT_ROUTE_STAGE};

#[test]
fn resolution_failure_bursts_share_warning_budget_with_session_logging() {
    // Isolate the global limiter and tracing callsite cache from parallel tests.
    const CHILD: &str = "ZERO_TEST_RESOLUTION_WARNING_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "logging::listener::tests::resolution_failure_bursts_share_warning_budget_with_session_logging",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(BufferMakeWriter(buffer.clone()))
        .with_ansi(false)
        .without_time()
        .finish();
    let error = EngineError::Core(zero_core::Error::Io("failed to resolve direct target"));
    tracing::subscriber::with_default(subscriber, || {
        for _ in 0..100 {
            log_listener_connection_error(
                INBOUND_ACCEPT_ROUTE_STAGE,
                "mixed",
                "mixed-in",
                &"127.0.0.1:1234",
                &error,
            );
        }
        let session = zero_core::Session::new(
            1,
            zero_core::Address::Domain("phone.local.".to_owned()),
            49152,
            zero_core::Network::Tcp,
            zero_core::ProtocolType::new("socks5"),
        );
        crate::logging::session::log_session_failed(
            &session,
            None,
            "route_or_establish",
            std::time::Duration::ZERO,
            &error,
            None,
        );
        // Non-environmental errors must remain visible during a DNS storm.
        log_listener_connection_error(
            INBOUND_ACCEPT_ROUTE_STAGE,
            "mixed",
            "mixed-in",
            &"127.0.0.1:1234",
            &EngineError::InvalidPlan {
                message: "independent route construction failure".to_owned(),
            },
        );
    });
    let logs = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
    assert_eq!(
        logs.matches("failed to resolve direct target").count(),
        1,
        "{logs}"
    );
    assert_eq!(logs.matches("connection failed").count(), 2, "{logs}");
    assert!(logs.contains("suppressed_failures=0"), "{logs}");
    assert!(
        logs.contains("independent route construction failure"),
        "{logs}"
    );
}

#[test]
fn accepted_route_failures_keep_a_stable_stage_across_log_levels() {
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(BufferMakeWriter(buffer.clone()))
        .with_ansi(false)
        .with_target(false)
        .without_time()
        .compact()
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        log_listener_connection_error(
            INBOUND_ACCEPT_ROUTE_STAGE,
            "vless",
            "inbound-a",
            &"127.0.0.1:1234",
            &EngineError::InvalidPlan {
                message: "route construction failed".to_owned(),
            },
        );
        log_listener_connection_error(
            INBOUND_ACCEPT_ROUTE_STAGE,
            "vless",
            "inbound-a",
            &"127.0.0.1:1234",
            &EngineError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client disconnected",
            )),
        );
    });

    let logs = String::from_utf8(buffer.lock().expect("log buffer lock").clone())
        .expect("logs should be UTF-8");
    assert!(logs.contains(" WARN "), "{logs}");
    assert!(logs.contains("DEBUG"), "{logs}");
    assert_eq!(
        logs.matches("stage=\"inbound_accept_route\"").count(),
        2,
        "{logs}"
    );
    assert_eq!(logs.matches("protocol=\"vless\"").count(), 2, "{logs}");
    assert_eq!(
        logs.matches("inbound_tag=\"inbound-a\"").count(),
        2,
        "{logs}"
    );
}

#[derive(Clone)]
struct BufferMakeWriter(Arc<Mutex<Vec<u8>>>);

impl<'a> MakeWriter<'a> for BufferMakeWriter {
    type Writer = BufferWriter;

    fn make_writer(&'a self) -> Self::Writer {
        BufferWriter(self.0.clone())
    }
}

struct BufferWriter(Arc<Mutex<Vec<u8>>>);

impl Write for BufferWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
