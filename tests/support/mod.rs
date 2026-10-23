#![allow(dead_code)]

pub mod listener;

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

/// Global lock to serialize port allocation and zero process startup.
/// Cooperating tests share this lock; it does not reserve OS endpoints.
static PORT_LOCK: Mutex<()> = Mutex::new(());

/// Acquire the global port lock. Tests that spawn zero must hold this lock
/// from port allocation through process startup confirmation.
pub fn acquire_port_lock() -> std::sync::MutexGuard<'static, ()> {
    PORT_LOCK.lock().expect("port lock poisoned")
}

pub fn config_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/v0.0.1/basic.json")
}

pub fn free_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind free port probe");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

pub fn wait_for_port(port: u16) {
    for _ in 0..120 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }

        thread::sleep(Duration::from_millis(50));
    }

    panic!("port {port} did not open in time");
}

pub fn http_get(port: u16, path: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect status port");
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .expect("write http request");

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read http response");
    response
}

pub fn http_post(port: u16, path: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect status port");
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .expect("write http request");

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read http response");
    response
}

pub fn http_post_json(port: u16, path: &str, body: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect status port");
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .expect("write http request");

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read http response");
    response
}

pub fn write_temp_config(contents: &str, suffix: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "zero-test-{}-{}-{suffix}.json",
        std::process::id(),
        free_port()
    ));
    fs::write(&path, contents).expect("write temp config");
    path
}

pub fn create_temp_dir(suffix: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "zero-test-{}-{}-{suffix}",
        std::process::id(),
        free_port()
    ));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

pub fn remove_temp_file(path: &Path) {
    let _ = fs::remove_file(path);
}

pub fn remove_temp_dir(path: &Path) {
    let _ = fs::remove_dir_all(path);
}

pub fn spawn_zero(args: &[&str]) -> Child {
    Command::new(env!("CARGO_BIN_EXE_zero"))
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn zero process")
}

pub fn stop_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

pub fn socks5_connect(port: u16, host: &str, target_port: u16) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect socks5 proxy");
    stream
        .write_all(&[0x05, 0x01, 0x00])
        .expect("write socks auth");

    let mut auth = [0_u8; 2];
    stream.read_exact(&mut auth).expect("read socks auth");
    assert_eq!(auth, [0x05, 0x00]);

    let host_bytes = host.as_bytes();
    let mut request = vec![0x05, 0x01, 0x00, 0x03, host_bytes.len() as u8];
    request.extend_from_slice(host_bytes);
    request.extend_from_slice(&target_port.to_be_bytes());
    stream.write_all(&request).expect("write socks request");

    let mut response = vec![0_u8; 10];
    stream
        .read_exact(&mut response)
        .expect("read socks response");
    assert_eq!(response[1], 0x00, "unexpected socks5 reply: {:?}", response);

    stream
}

pub fn socks5_connect_ipv4(port: u16, addr: [u8; 4], target_port: u16) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect socks5 proxy");
    stream
        .write_all(&[0x05, 0x01, 0x00])
        .expect("write socks auth");

    let mut auth = [0_u8; 2];
    stream.read_exact(&mut auth).expect("read socks auth");
    assert_eq!(auth, [0x05, 0x00]);

    let mut request = vec![0x05, 0x01, 0x00, 0x01];
    request.extend_from_slice(&addr);
    request.extend_from_slice(&target_port.to_be_bytes());
    stream.write_all(&request).expect("write socks request");

    let mut response = vec![0_u8; 10];
    stream
        .read_exact(&mut response)
        .expect("read socks response");
    assert_eq!(response[1], 0x00, "unexpected socks5 reply: {:?}", response);

    stream
}

pub fn http_tunnel(port: u16, authority: &str) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect http proxy");
    let request =
        format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .expect("write http connect request");

    let mut response = vec![0_u8; 39];
    stream
        .read_exact(&mut response)
        .expect("read http connect response");
    assert_eq!(&response, b"HTTP/1.1 200 Connection Established\r\n\r\n");

    stream
}
