use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[test]
fn managed_kernel_exits_when_parent_lifetime_pipe_closes() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should follow Unix epoch")
        .as_nanos();
    let workspace = std::env::temp_dir().join(format!(
        "zero-parent-lifetime-{}-{unique}",
        std::process::id()
    ));
    std::fs::create_dir_all(&workspace).expect("temporary workspace should be created");
    let config_path = workspace.join("config.json");
    let inbound_port = free_loopback_port();
    std::fs::write(
        &config_path,
        format!(
            r#"{{
  "inbounds": [{{
    "tag": "managed-test",
    "listen": {{ "address": "127.0.0.1", "port": {inbound_port} }},
    "protocol": {{ "type": "mixed" }}
  }}],
  "outbounds": [],
  "outbound_groups": [],
  "runtime": {{}},
  "api": {{ "control": {{ "enabled": false }} }},
  "mode": {{ "type": "rule" }},
  "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
}}"#
        ),
    )
    .expect("temporary config should be written");

    #[cfg(windows)]
    let control_socket = format!(r"\\.\pipe\zero-parent-lifetime-{unique}");
    #[cfg(unix)]
    let control_socket = std::path::PathBuf::from("/tmp")
        .join(format!("zero-pl-{unique}.sock"))
        .to_string_lossy()
        .into_owned();

    let mut child = Command::new(env!("CARGO_BIN_EXE_zero"))
        .arg("run")
        .arg("--parent-lifetime-stdin")
        .arg("--control-socket")
        .arg(&control_socket)
        .arg(&config_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("managed kernel should start");

    let startup_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match child
            .try_wait()
            .expect("managed kernel should be inspectable")
        {
            Some(status) => fail_with_stderr(
                child,
                format!("managed kernel exited during startup with {status}"),
            ),
            None if managed_endpoint_ready(&control_socket) => break,
            None if Instant::now() >= startup_deadline => {
                let _ = child.kill();
                fail_with_stderr(
                    child,
                    "managed kernel endpoint did not become ready".to_string(),
                );
            }
            None => std::thread::sleep(Duration::from_millis(25)),
        }
    }

    // The child remains alive while this process owns the pipe writer.
    assert!(child
        .try_wait()
        .expect("managed kernel should be inspectable")
        .is_none());

    child.stdin.take();
    let shutdown_deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        match child
            .try_wait()
            .expect("managed kernel should be inspectable")
        {
            Some(status) => break status,
            None if Instant::now() >= shutdown_deadline => {
                let _ = child.kill();
                fail_with_stderr(
                    child,
                    "managed kernel did not exit after parent pipe EOF".to_string(),
                );
            }
            None => std::thread::sleep(Duration::from_millis(25)),
        }
    };

    if !status.success() {
        fail_with_stderr(
            child,
            format!("managed kernel exited unsuccessfully with {status}"),
        );
    }
    #[cfg(unix)]
    let _ = std::fs::remove_file(control_socket);
    let _ = std::fs::remove_dir_all(workspace);
}

#[cfg(unix)]
fn managed_endpoint_ready(control_socket: &str) -> bool {
    std::path::Path::new(control_socket).exists()
}

#[cfg(windows)]
fn managed_endpoint_ready(_control_socket: &str) -> bool {
    // Named pipes do not have a filesystem entry. Surviving the startup
    // grace period proves that configuration and listener startup succeeded.
    std::thread::sleep(Duration::from_millis(300));
    true
}

fn fail_with_stderr(mut child: std::process::Child, message: String) -> ! {
    let mut stderr = String::new();
    if let Some(mut stream) = child.stderr.take() {
        let _ = stream.read_to_string(&mut stderr);
    }
    panic!("{message}; stderr: {stderr}");
}

fn free_loopback_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .expect("ephemeral loopback port should be available")
        .local_addr()
        .expect("ephemeral listener should have a local address")
        .port()
}
