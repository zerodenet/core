//! Exercise the ordinary child runtime without the suite's larger test stack.
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct Kernel {
    child: Child,
    log: PathBuf,
}
impl Drop for Kernel {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "kernel log: {}",
                std::fs::read_to_string(&self.log).unwrap()
            );
        }
        // Failure cleanup only. The successful path requires a natural exit.
        if self.child.try_wait().unwrap().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

#[test]
fn ordinary_forwarding_uses_default_worker_stack_and_exits_after_parent_eof() {
    let _ports = crate::support::acquire_port_lock();
    let directory = tempfile::Builder::new()
        .prefix("zero-stack-")
        .tempdir_in("/tmp")
        .unwrap();
    let target = TcpListener::bind("127.0.0.1:0").unwrap();
    target.set_nonblocking(true).unwrap();
    let port = crate::support::free_port();
    let config = serde_json::json!({
        "inbounds":[{"tag":"forward","listen":{"address":"127.0.0.1","port":port},
            "protocol":{"type":"direct","target":"127.0.0.1","port":target.local_addr().unwrap().port()}}],
        "route":{"final":{"type":"direct"}}
    });
    let path = directory.path().join("config.json");
    let socket = directory.path().join("control.sock");
    std::fs::write(&path, config.to_string()).unwrap();
    let log = directory.path().join("kernel.log");
    let mut kernel = Kernel {
        child: Command::new(env!("CARGO_BIN_EXE_zero"))
            .env_remove("RUST_MIN_STACK")
            .args(["run", "--parent-lifetime-stdin", "--control-socket"])
            .arg(&socket)
            .arg(&path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::from(std::fs::File::create(&log).unwrap()))
            .spawn()
            .unwrap(),
        log,
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut client = loop {
        assert!(kernel.child.try_wait().unwrap().is_none());
        if let Ok(client) = TcpStream::connect(("127.0.0.1", port)) {
            break client;
        }
        assert!(Instant::now() < deadline, "inbound did not start");
        std::thread::sleep(Duration::from_millis(10));
    };
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    client
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    client.write_all(b"echo").unwrap();
    let (mut accepted, _) = loop {
        match target.accept() {
            Ok(accepted) => break accepted,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(kernel.child.try_wait().unwrap().is_none());
                assert!(Instant::now() < deadline, "actual forwarding did not start");
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("accept: {error}"),
        }
    };
    // BSD accepts inherit the listener's nonblocking flag.
    accepted.set_nonblocking(false).unwrap();
    accepted
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    accepted
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut bytes = [0; 4];
    accepted.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"echo");
    accepted.write_all(&bytes).unwrap();
    client.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"echo");
    drop((client, accepted, target));
    kernel.child.stdin.take();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = kernel.child.try_wait().unwrap() {
            assert!(status.success(), "managed runtime exited with {status}");
            break;
        }
        assert!(Instant::now() < deadline, "managed runtime did not exit");
        std::thread::sleep(Duration::from_millis(10));
    }
}
