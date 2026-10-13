#![cfg(all(unix, feature = "sink-jsonl"))]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

struct Kernel(Child, std::path::PathBuf);
impl Drop for Kernel {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Ok(log) = std::fs::read_to_string(&self.1) {
                eprintln!(
                    "kernel log: {}",
                    log.lines()
                        .rev()
                        .take(12)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect::<Vec<_>>()
                        .join("\n")
                );
            }
        }
        // This is a disposable test kernel, never an installed service.
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn request(socket: &Path, value: Value) -> Value {
    let mut stream = UnixStream::connect(socket).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    writeln!(stream, "{value}").unwrap();
    let mut line = String::new();
    BufReader::new(stream)
        .read_line(&mut line)
        .expect("control acknowledgement did not arrive within five seconds");
    serde_json::from_str(&line).unwrap()
}

fn runtime(socket: &Path) -> Value {
    request(socket, json!({"type":"query","request":{"runtime":{}}}))["result"]["runtime"].clone()
}

fn write_blocked(value: &Value) -> bool {
    match value {
        Value::Object(fields) => {
            fields.get("write_blocked") == Some(&Value::Bool(true))
                || fields.values().any(write_blocked)
        }
        Value::Array(items) => items.iter().any(write_blocked),
        _ => false,
    }
}

fn probe(stream: &mut TcpStream, phase: &str) {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream.write_all(b"existing connection").unwrap();
    let mut echo = [0; 19];
    stream
        .read_exact(&mut echo)
        .unwrap_or_else(|err| panic!("{phase}: {err}"));
    assert_eq!(&echo, b"existing connection");
}

fn apply_under_outbox_pressure(change_dispatcher: bool) {
    let _ports = super::support::acquire_port_lock();
    let directory = tempfile::tempdir().unwrap();
    let target = TcpListener::bind("127.0.0.1:0").unwrap();
    let target_port = target.local_addr().unwrap().port();
    target.set_nonblocking(true).unwrap();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stopped = stop.clone();
    let echo = std::thread::spawn(move || {
        while !stopped.load(std::sync::atomic::Ordering::Acquire) {
            match target.accept() {
                Ok((mut stream, _)) => {
                    // BSD/macOS accepts inherit the listener's nonblocking
                    // flag; the disposable echo workers perform blocking I/O.
                    stream.set_nonblocking(false).unwrap();
                    std::thread::spawn(move || {
                        let mut buffer = [0; 64];
                        while let Ok(n) = stream.read(&mut buffer) {
                            if n == 0 || stream.write_all(&buffer[..n]).is_err() {
                                break;
                            }
                        }
                    });
                }
                Err(_) => std::thread::sleep(Duration::from_millis(5)),
            }
        }
    });
    struct StopEcho(std::sync::Arc<std::sync::atomic::AtomicBool>);
    impl Drop for StopEcho {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Release);
        }
    }
    let _stop_echo = StopEcho(stop.clone());
    let port = super::support::free_port();
    let config = json!({
        "inbounds":[{"tag":"existing","listen":{"address":"127.0.0.1","port":port},
            "protocol":{"type":"direct","target":"127.0.0.1","port":target_port}}],
        "route":{"final":{"type":"direct"}},
        "api":{
            "event_sinks":[{"type":"jsonl","tag":"audit","path":"events.jsonl","events":["flow.completed"]}],
            "outbox_path":"outbox.jsonl",
            "dispatcher":{"replay_batch_size":2,"max_in_memory_deliveries":2,
                "outbox_min_free_bytes":u64::MAX,"outbox_min_free_percent":1}
        }
    });
    let path = directory.path().join("runtime.json");
    let socket = directory.path().join("control.sock");
    std::fs::write(&path, config.to_string()).unwrap();
    let log_path = directory.path().join("kernel.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut kernel = Kernel(
        Command::new(env!("CARGO_BIN_EXE_zero"))
            .args(["run", "--control-socket"])
            .arg(&socket)
            .arg(&path)
            .stdout(Stdio::null())
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
        log_path,
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(kernel.0.try_wait().unwrap().is_none(), "test kernel exited");
        if socket.exists() && TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        assert!(Instant::now() < deadline, "test kernel did not start");
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut established = TcpStream::connect(("127.0.0.1", port)).unwrap();
    probe(&mut established, "initial established flow");
    // More lifecycle facts than the subscription bridge can buffer. A full,
    // protected outbox must not make configuration acknowledgements hang.
    for _ in 0..24 {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        probe(&mut stream, "generate lifecycle facts");
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    let before = loop {
        let state = runtime(&socket);
        let sinks = request(&socket, json!({"type":"query","request":{"sinks":{}}}));
        if write_blocked(&sinks) {
            break state;
        }
        assert!(Instant::now() < deadline, "outbox did not enter protection");
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut candidate = config;
    let inspected = Command::new(env!("CARGO_BIN_EXE_zero"))
        .args(["connector", "state", "--json"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        inspected.status.success(),
        "{}",
        String::from_utf8_lossy(&inspected.stderr)
    );
    let after_inspection = runtime(&socket);
    for field in ["core_instance_id", "config_revision", "started_at_unix_ms"] {
        assert_eq!(
            after_inspection[field], before[field],
            "preflight changed {field}"
        );
    }
    assert!(kernel.0.try_wait().unwrap().is_none());
    probe(&mut established, "existing flow after read-only preflight");
    if change_dispatcher {
        candidate["api"]["event_sinks"][0]["path"] = json!("replacement.jsonl");
    } else {
        let mut extra = candidate["inbounds"][0].clone();
        extra["tag"] = json!("added");
        extra["listen"]["port"] = json!(super::support::free_port());
        #[cfg(feature = "vless")]
        {
            extra["protocol"] = json!({
                "type":"vless",
                "users":[{"id":"6a978c2e-8cdb-4295-96d9-cb781463ddcb","flow":"xtls-rprx-vision"}],
                "reality":{"private_key":"OKMOFBeltHBXaTQ8cIcsgabVQcqXeTB9Ih3lPtWMY04",
                    "short_ids":["0123456789abcdef"],"server_name":"private.test"}
            });
        }
        candidate["inbounds"].as_array_mut().unwrap().push(extra);
    }
    let response = request(
        &socket,
        json!({"type":"command","method":"config.apply","params":{"config":candidate}}),
    );
    assert_eq!(response["ok"], true, "{response}");
    assert_eq!(response["result"]["result"]["reconciled"], true);
    let after = runtime(&socket);
    assert_eq!(after["core_instance_id"], before["core_instance_id"]);
    assert_eq!(
        after["config_revision"].as_u64(),
        before["config_revision"].as_u64().map(|v| v + 1)
    );
    assert!(kernel.0.try_wait().unwrap().is_none());
    probe(&mut established, "existing flow after acknowledgement");
    stop.store(true, std::sync::atomic::Ordering::Release);
    echo.join().unwrap();
}

#[test]
fn inbound_only_apply_preserves_dispatcher_and_existing_connection_under_backpressure() {
    apply_under_outbox_pressure(false);
}

#[test]
fn dispatcher_replacement_acknowledges_under_backpressure_without_closing_existing_connection() {
    apply_under_outbox_pressure(true);
}
