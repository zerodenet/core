#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::net::UnixStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

struct Managed(Child);
impl Drop for Managed {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn request(path: &std::path::Path, frame: Value) -> Value {
    let mut stream = UnixStream::connect(path).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    writeln!(stream, "{frame}").unwrap();
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).unwrap();
    let reply: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(reply["ok"], true, "{reply}");
    reply
}

#[test]
fn management_only_start_applies_first_listener_returns_to_idle_and_shuts_down() {
    let dir = tempfile::Builder::new()
        .prefix("zero-mgmt-")
        .tempdir_in("/tmp")
        .unwrap();
    let path = dir.path().join("config.json");
    let socket = dir.path().join("ipc.sock");
    let empty = json!({"inbounds":[],"outbounds":[],"api":{"control":{"enabled":false}},
        "route":{"rules":[],"final":{"type":"direct"}}});
    std::fs::write(&path, empty.to_string()).unwrap();
    let mut child = Managed(
        Command::new(env!("CARGO_BIN_EXE_zero"))
            .args(["run", "--parent-lifetime-stdin", "--control-socket"])
            .arg(&socket)
            .arg(&path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while !socket.exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "management kernel exited"
        );
        assert!(Instant::now() < deadline, "IPC startup timeout");
        std::thread::sleep(Duration::from_millis(25));
    }
    // IPC can start before orchestration; surviving this delay catches the
    // previous NoInbounds error instead of mistaking the transient socket for readiness.
    std::thread::sleep(Duration::from_millis(350));
    let health = request(&socket, json!({"type":"query","request":{"health":{}}}));
    assert_eq!(health["result"]["health"]["healthy"], true);
    let probe = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let mut active = empty.clone();
    active["inbounds"] = json!([{"tag":"first","listen":{"address":"127.0.0.1","port":port},"protocol":{"type":"socks5"}}]);
    request(
        &socket,
        json!({"type":"command","method":"config.apply","params":{"config":active}}),
    );
    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    client.write_all(&[5, 1, 0]).unwrap();
    let mut greeting = [0; 2];
    client.read_exact(&mut greeting).unwrap();
    assert_eq!(greeting, [5, 0]);
    drop(client);
    request(
        &socket,
        json!({"type":"command","method":"config.apply","params":{"config":empty}}),
    );
    assert!(TcpListener::bind(("127.0.0.1", port)).is_ok());
    request(&socket, json!({"type":"query","request":{"health":{}}}));
    drop(child.0.stdin.take());
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "idle kernel failed to stop at lifetime EOF"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}
