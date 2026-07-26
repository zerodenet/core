use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(any(feature = "trojan", feature = "vmess"))]
use ring::digest::{digest, SHA256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{broadcast, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
#[cfg(feature = "socks5")]
use zero_core::Address;

/// Look up an environment variable.  Returns `None` when the variable is
/// unset and prints a skip message.  Callers should `return;` early to
/// gracefully skip the test.
pub fn require_env(var: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(v) => Some(v),
        Err(_) => {
            eprintln!("SKIP: env var {var} is not set");
            None
        }
    }
}

static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);
static LOG_INIT: Once = Once::new();

pub struct TcpResetProxy {
    reset_tx: broadcast::Sender<()>,
    accepted_connections: Arc<AtomicU64>,
    active_connections: Arc<AtomicU64>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl TcpResetProxy {
    pub async fn start(listen_port: u16, upstream_port: u16) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", listen_port))
            .await
            .expect("bind TCP reset proxy");
        let (reset_tx, _) = broadcast::channel(16);
        let accept_reset_tx = reset_tx.clone();
        let accepted_connections = Arc::new(AtomicU64::new(0));
        let accept_counter = accepted_connections.clone();
        let active_connections = Arc::new(AtomicU64::new(0));
        let active_counter = active_connections.clone();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((mut downstream, _)) = accepted else {
                            break;
                        };
                        accept_counter.fetch_add(1, Ordering::Relaxed);
                        active_counter.fetch_add(1, Ordering::Relaxed);
                        let mut reset_rx = accept_reset_tx.subscribe();
                        let connection_counter = active_counter.clone();
                        tokio::spawn(async move {
                            if let Ok(mut upstream) =
                                TcpStream::connect(("127.0.0.1", upstream_port)).await
                            {
                                tokio::select! {
                                    _ = tokio::io::copy_bidirectional(
                                        &mut downstream,
                                        &mut upstream,
                                    ) => {}
                                    _ = reset_rx.recv() => {}
                                }
                            }
                            connection_counter.fetch_sub(1, Ordering::Relaxed);
                        });
                    }
                    _ = &mut shutdown_rx => break,
                }
            }
        });
        Self {
            reset_tx,
            accepted_connections,
            active_connections,
            shutdown_tx: Some(shutdown_tx),
            task,
        }
    }

    pub fn accepted_connections(&self) -> u64 {
        self.accepted_connections.load(Ordering::Relaxed)
    }

    pub fn active_connections(&self) -> u64 {
        self.active_connections.load(Ordering::Relaxed)
    }

    pub fn reset_connections(&self) {
        let _ = self.reset_tx.send(());
    }

    pub async fn shutdown(mut self) {
        self.reset_connections();
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        let _ = tokio::time::timeout(Duration::from_secs(2), self.task).await;
    }
}

// ── External process management ──────────────────────────────────────────

pub struct ExternalProcess {
    child: Child,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

impl ExternalProcess {
    pub fn start(program: String, args: &[&str], material: &TempMaterial, name: &str) -> Self {
        Self::start_with_env(program, args, &[], material, name)
    }

    pub fn start_with_env(
        program: String,
        args: &[&str],
        env: &[(&str, &str)],
        material: &TempMaterial,
        name: &str,
    ) -> Self {
        let stdout_path = material.path(&format!("{name}.stdout"));
        let stderr_path = material.path(&format!("{name}.stderr"));
        let stdout = std::fs::File::create(&stdout_path).expect("process stdout");
        let stderr = std::fs::File::create(&stderr_path).expect("process stderr");
        let child = Command::new(program)
            .args(args)
            .envs(env.iter().copied())
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .unwrap_or_else(|error| panic!("start {name}: {error}"));
        Self {
            child,
            stdout_path,
            stderr_path,
        }
    }

    pub fn kill(&mut self) {
        if self.child.try_wait().ok().flatten().is_some() {
            return;
        }
        let _ = self.child.kill();
        for _ in 0..100 {
            match self.child.try_wait() {
                Ok(Some(_)) | Err(_) => return,
                Ok(None) => std::thread::sleep(std::time::Duration::from_millis(10)),
            }
        }
    }

    pub fn logs(&self) -> String {
        let stdout = std::fs::read_to_string(&self.stdout_path).unwrap_or_default();
        let stderr = std::fs::read_to_string(&self.stderr_path).unwrap_or_default();
        format!("stdout:\n{stdout}\nstderr:\n{stderr}")
    }
}

impl Drop for ExternalProcess {
    fn drop(&mut self) {
        self.kill();
    }
}

// ── Temp material ────────────────────────────────────────────────────────

pub struct TempMaterial {
    pub dir: PathBuf,
}

impl TempMaterial {
    pub fn new(prefix: &str) -> Self {
        let unique = NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "{prefix}-{}-{}-{unique}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos(),
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        Self { dir }
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    #[cfg(any(feature = "trojan", feature = "vmess"))]
    pub fn tls(&self) -> TestTlsMaterial {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate self-signed cert");
        let cert_path = self.path("server.crt");
        let key_path = self.path("server.key");
        let cert_sha256_hex = hex_lower(digest(&SHA256, certified.cert.der().as_ref()).as_ref());
        std::fs::write(&cert_path, certified.cert.pem()).expect("write cert");
        std::fs::write(&key_path, certified.signing_key.serialize_pem()).expect("write key");
        TestTlsMaterial {
            cert_path,
            key_path,
            cert_sha256_hex,
        }
    }
}

impl Drop for TempMaterial {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// ── TLS material ─────────────────────────────────────────────────────────

pub struct TestTlsMaterial {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub cert_sha256_hex: String,
}

// ── Xray process wrapper ─────────────────────────────────────────────────

pub struct XrayProcess {
    inner: ExternalProcess,
}

impl XrayProcess {
    pub fn start(xray_bin: String, config: &Path, material: &TempMaterial) -> Self {
        Self::start_with_env(xray_bin, config, &[], material)
    }

    pub fn start_with_env(
        xray_bin: String,
        config: &Path,
        env: &[(&str, &str)],
        material: &TempMaterial,
    ) -> Self {
        Self {
            inner: ExternalProcess::start_with_env(
                xray_bin,
                &["run", "-config", config.to_str().expect("xray config path")],
                env,
                material,
                "xray",
            ),
        }
    }

    pub fn kill(&mut self) {
        self.inner.kill();
    }

    pub fn logs(&self) -> String {
        self.inner.logs()
    }
}

impl Drop for XrayProcess {
    fn drop(&mut self) {
        self.kill();
    }
}

// ── Logging ──────────────────────────────────────────────────────────────

/// Initialize tracing for interop tests.  `filter_suffix` is appended to
/// `"zero_proxy=debug,"` to form the default RUST_LOG (e.g. `"vmess=debug"`).
pub fn init_logs(filter_suffix: &str) {
    LOG_INIT.call_once(|| {
        let default_filter = format!("zero_proxy=debug,{filter_suffix}");
        let _ = tracing_subscriber::fmt()
            .with_env_filter(std::env::var("RUST_LOG").unwrap_or(default_filter))
            .with_test_writer()
            .try_init();
    });
}

// ── Binary resolvers ─────────────────────────────────────────────────────

/// Resolve the sing-box binary path.  Falls back to
/// `$TMP/zero-{protocol}-interop/sing-box/sing-box.exe`.
pub fn sing_box_bin(protocol: &str) -> String {
    std::env::var("SING_BOX_BIN").unwrap_or_else(|_| {
        std::env::temp_dir()
            .join(format!("zero-{protocol}-interop"))
            .join("sing-box")
            .join("sing-box.exe")
            .display()
            .to_string()
    })
}

/// Resolve the mihomo binary path.  Falls back to
/// `$TMP/zero-{protocol}-interop/mihomo/mihomo.exe`.
pub fn mihomo_bin(protocol: &str) -> String {
    std::env::var("MIHOMO_BIN").unwrap_or_else(|_| {
        std::env::temp_dir()
            .join(format!("zero-{protocol}-interop"))
            .join("mihomo")
            .join("mihomo.exe")
            .display()
            .to_string()
    })
}

// ── Utilities ────────────────────────────────────────────────────────────

/// Escape backslashes in a path so it is safe inside a JSON string value.
pub fn escape_json_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "\\\\")
}

/// Hex-encode bytes as lowercase hex.
#[cfg(any(feature = "trojan", feature = "vmess"))]
pub fn hex_lower(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(TABLE[(byte >> 4) as usize] as char);
        out.push(TABLE[(byte & 0x0f) as usize] as char);
    }
    out
}

// ── Echo servers ─────────────────────────────────────────────────────────

/// Spawn a single-shot TCP echo server: accepts one connection, reads
/// `payload_len` bytes, writes the same bytes back.
pub async fn spawn_tcp_echo(port: u16, payload_len: usize) -> tokio::task::JoinHandle<()> {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("bind echo");
        let _ = ready_tx.send(());
        let (mut stream, _) = listener.accept().await.expect("accept echo");
        let mut buf = vec![0_u8; payload_len];
        stream.read_exact(&mut buf).await.expect("read echo");
        stream.write_all(&buf).await.expect("write echo");
    });
    ready_rx.await.expect("echo ready");
    task
}

/// Spawn a single-shot UDP echo server: receives one datagram, sends the
/// same bytes back to the sender.
pub async fn spawn_udp_echo(port: u16, _payload_len: usize) -> tokio::task::JoinHandle<()> {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let socket = UdpSocket::bind(("127.0.0.1", port))
            .await
            .expect("bind udp echo");
        let _ = ready_tx.send(());
        let mut buf = [0_u8; 2048];
        let (read, peer) = socket.recv_from(&mut buf).await.expect("recv udp echo");
        socket
            .send_to(&buf[..read], peer)
            .await
            .expect("send udp echo");
    });
    ready_rx.await.expect("udp echo ready");
    task
}

pub async fn spawn_udp_echo_count(port: u16, count: usize) -> tokio::task::JoinHandle<()> {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let socket = UdpSocket::bind(("127.0.0.1", port))
            .await
            .expect("bind udp echo");
        let _ = ready_tx.send(());
        let mut buf = [0_u8; 2048];
        for _ in 0..count {
            let (read, peer) = socket.recv_from(&mut buf).await.expect("recv udp echo");
            socket
                .send_to(&buf[..read], peer)
                .await
                .expect("send udp echo");
        }
    });
    ready_rx.await.expect("udp echo ready");
    task
}

// ── SOCKS5 test helpers ──────────────────────────────────────────────────

/// Via a SOCKS5 proxy at `proxy_port`, TCP-connect to `127.0.0.1:target_port`,
/// send `payload`, read back the same amount of bytes.  Retries up to 50
/// times with 50 ms delays (the proxy may need a moment to be ready).
pub async fn socks5_tcp_echo(proxy_port: u16, target_port: u16, payload: &[u8]) -> Vec<u8> {
    let mut last_error = None;
    for _ in 0..50 {
        match socks5_tcp_echo_once(proxy_port, target_port, payload).await {
            Ok(echoed) => return echoed,
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
    panic!("socks5 tcp echo failed: {:?}", last_error);
}

/// Single attempt at `socks5_tcp_echo`.
pub async fn socks5_tcp_echo_once(
    proxy_port: u16,
    target_port: u16,
    payload: &[u8],
) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(("127.0.0.1", proxy_port)).await?;
    stream.write_all(&[0x05, 0x01, 0x00]).await?;
    let mut auth = [0_u8; 2];
    stream.read_exact(&mut auth).await?;
    assert_eq!(auth, [0x05, 0x00]);

    let mut request = vec![0x05, 0x01, 0x00, 0x01, 127, 0, 0, 1];
    request.extend_from_slice(&target_port.to_be_bytes());
    stream.write_all(&request).await?;
    let mut response = [0_u8; 10];
    stream.read_exact(&mut response).await?;
    assert_eq!(response[1], 0x00, "socks connect failed: {response:?}");

    stream.write_all(payload).await?;
    let mut echoed = vec![0_u8; payload.len()];
    stream.read_exact(&mut echoed).await?;
    Ok(echoed)
}

/// Via a SOCKS5 proxy at `proxy_port`, issue UDP ASSOCIATE, then send a
/// single UDP datagram to `127.0.0.1:target_port` and read the response.
#[cfg(feature = "socks5")]
pub async fn socks5_udp_echo(proxy_port: u16, target_port: u16, payload: &[u8]) -> Vec<u8> {
    let mut control = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect socks5 control");
    control
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("write socks5 auth");
    let mut auth = [0_u8; 2];
    control
        .read_exact(&mut auth)
        .await
        .expect("read socks5 auth");
    assert_eq!(auth, [0x05, 0x00]);

    control
        .write_all(&[
            0x05, 0x03, 0x00, 0x01, // UDP ASSOCIATE + IPv4
            0, 0, 0, 0, 0, 0,
        ])
        .await
        .expect("write udp associate");
    let mut response = [0_u8; 10];
    control
        .read_exact(&mut response)
        .await
        .expect("read udp associate response");
    assert_eq!(response[1], 0x00, "udp associate failed: {response:?}");
    let relay_port = u16::from_be_bytes([response[8], response[9]]);

    let client = UdpSocket::bind(("127.0.0.1", 0))
        .await
        .expect("bind udp client");
    let packet = super::build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), target_port, payload)
        .expect("build socks5 udp packet");
    client
        .send_to(&packet, ("127.0.0.1", relay_port))
        .await
        .expect("send udp packet");

    let mut buf = [0_u8; 2048];
    let (read, _) = client.recv_from(&mut buf).await.expect("recv udp response");
    let response = super::parse_udp_packet(&buf[..read]).expect("parse socks5 udp response");
    assert_eq!(response.target, Address::Ipv4([127, 0, 0, 1]));
    assert_eq!(response.port, target_port);
    response.payload.to_vec()
}

#[cfg(feature = "socks5")]
pub async fn socks5_udp_echo_sequence(
    proxy_port: u16,
    target_port: u16,
    payloads: &[&[u8]],
) -> Vec<Vec<u8>> {
    let packets = payloads
        .iter()
        .map(|payload| (target_port, *payload))
        .collect::<Vec<_>>();
    socks5_udp_echo_targets(proxy_port, &packets).await
}

/// Keep one SOCKS5 UDP association alive while sending packets to different
/// destination ports. This exercises full-cone/XUDP target changes without
/// creating a new client-side association for each destination.
#[cfg(feature = "socks5")]
pub async fn socks5_udp_echo_targets(proxy_port: u16, packets: &[(u16, &[u8])]) -> Vec<Vec<u8>> {
    socks5_udp_echo_targets_after_first(proxy_port, packets, || {}).await
}

#[cfg(feature = "socks5")]
pub async fn socks5_udp_echo_targets_after_first<F>(
    proxy_port: u16,
    packets: &[(u16, &[u8])],
    after_first: F,
) -> Vec<Vec<u8>>
where
    F: FnOnce(),
{
    let mut control = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect socks5 control");
    control
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("write socks5 auth");
    let mut auth = [0_u8; 2];
    control
        .read_exact(&mut auth)
        .await
        .expect("read socks5 auth");
    assert_eq!(auth, [0x05, 0x00]);

    control
        .write_all(&[
            0x05, 0x03, 0x00, 0x01, // UDP ASSOCIATE + IPv4
            0, 0, 0, 0, 0, 0,
        ])
        .await
        .expect("write udp associate");
    let mut response = [0_u8; 10];
    control
        .read_exact(&mut response)
        .await
        .expect("read udp associate response");
    assert_eq!(response[1], 0x00, "udp associate failed: {response:?}");
    let relay_port = u16::from_be_bytes([response[8], response[9]]);

    let client = UdpSocket::bind(("127.0.0.1", 0))
        .await
        .expect("bind udp client");
    let mut echoed = Vec::with_capacity(packets.len());
    let mut after_first = Some(after_first);
    for (index, (target_port, payload)) in packets.iter().enumerate() {
        let packet = super::build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), *target_port, payload)
            .expect("build socks5 udp packet");
        client
            .send_to(&packet, ("127.0.0.1", relay_port))
            .await
            .expect("send udp packet");

        let mut buf = [0_u8; 2048];
        let (read, _) = client.recv_from(&mut buf).await.expect("recv udp response");
        let response = super::parse_udp_packet(&buf[..read]).expect("parse socks5 udp response");
        assert_eq!(response.target, Address::Ipv4([127, 0, 0, 1]));
        assert_eq!(response.port, *target_port);
        echoed.push(response.payload);
        if index == 0 {
            if let Some(after_first) = after_first.take() {
                after_first();
            }
            sleep(Duration::from_millis(250)).await;
        }
    }
    echoed
}
