use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub(super) const HOST: &str = "http-control.zero.test";

pub(super) struct Observation {
    pub case: String,
    pub result: io::Result<()>,
}

pub(super) struct Peer {
    pub address: SocketAddr,
    stop: Arc<AtomicBool>,
    observations: Arc<Mutex<Vec<Observation>>>,
    worker: Option<JoinHandle<()>>,
}

impl Peer {
    pub fn start(address: SocketAddr) -> Self {
        let listener = TcpListener::bind(address).expect("bind controlled HTTP peer");
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let observations = Arc::new(Mutex::new(Vec::new()));
        let worker_stop = Arc::clone(&stop);
        let worker_observations = Arc::clone(&observations);
        let worker = thread::spawn(move || {
            while !worker_stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, source)) => {
                        let mut case = String::new();
                        let result = serve(&mut stream, &mut case);
                        eprintln!("HTTP peer case={case} local={address} source={source} result={result:?}");
                        worker_observations
                            .lock()
                            .unwrap()
                            .push(Observation { case, result });
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("accept controlled HTTP peer: {error}"),
                }
            }
        });
        Self {
            address,
            stop,
            observations,
            worker: Some(worker),
        }
    }

    pub fn assert_observations(&self, suites: usize) {
        // The final complete exchange ends at EOF, just before the worker
        // publishes its observation; join it before checking exact counts.
        let observations = self.observations.lock().unwrap();
        assert_eq!(
            observations.len(),
            suites * 64,
            "every client request must reach the peer"
        );
        let mut cases = std::collections::HashSet::new();
        for observation in observations.iter() {
            assert!(
                cases.insert(&observation.case),
                "duplicated request {}",
                observation.case
            );
            if let Err(error) = &observation.result {
                assert!(
                    observation.case.ends_with("/early")
                        && matches!(
                            error.kind(),
                            io::ErrorKind::ConnectionReset
                                | io::ErrorKind::ConnectionAborted
                                | io::ErrorKind::BrokenPipe
                                | io::ErrorKind::NotConnected
                        ),
                    "unexpected peer failure case={} error={error}",
                    observation.case
                );
            }
        }
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            worker.join().expect("join controlled HTTP peer");
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.stop();
    }
}

pub(super) fn response(case: &str) -> Vec<u8> {
    let size = if case.ends_with("/single") {
        869
    } else {
        65_536
    };
    let mut result = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nConnection: close\r\nX-Case: {case}\r\n\r\n"
    )
    .into_bytes();
    result.extend((0..size).map(|index| (index % 251) as u8));
    result
}

fn serve(stream: &mut TcpStream, case: &mut String) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        if request.len() >= 4096 {
            return Err(io::Error::other("oversized HTTP test request"));
        }
        let mut byte = [0];
        stream.read_exact(&mut byte)?;
        request.push(byte[0]);
    }
    let text = std::str::from_utf8(&request).map_err(io::Error::other)?;
    *case = text
        .lines()
        .next()
        .and_then(|line| line.strip_prefix("GET /"))
        .and_then(|line| line.strip_suffix(" HTTP/1.1"))
        .ok_or_else(|| io::Error::other("invalid request line"))?
        .to_owned();
    if !text.lines().any(|line| line == format!("Host: {HOST}")) {
        return Err(io::Error::other("unexpected HTTP Host"));
    }
    let response = response(case);
    if case.ends_with("/single") {
        stream.write_all(&response)?;
    } else {
        // A short first segment exercises fragmented reads; in early-close
        // cases the client drops immediately after these 32 bytes.
        stream.write_all(&response[..32])?;
        thread::sleep(Duration::from_millis(2));
        for chunk in response[32..].chunks(4096) {
            stream.write_all(chunk)?;
            thread::sleep(Duration::from_millis(1));
        }
    }
    stream.shutdown(Shutdown::Write)
}
