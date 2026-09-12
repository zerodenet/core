use shadowsocks::CipherKind;
use std::{
    collections::BTreeSet,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
pub struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
pub fn launch(binary: &str, args: &[String]) -> Process {
    let folder = std::env::var_os("SS_RUST_BIN_DIR")
        .expect("SS_RUST_BIN_DIR must point to the fixed official 1.21.2 binaries");
    let path = PathBuf::from(folder).join(format!("{binary}{}", std::env::consts::EXE_SUFFIX));
    let version = Command::new(&path).arg("--version").output().unwrap();
    assert!(
        String::from_utf8_lossy(&version.stdout)
            .split_whitespace()
            .any(|word| word == "1.21.2"),
        "reference must be exactly 1.21.2"
    );
    Process(
        Command::new(path)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    )
}
pub fn methods() -> BTreeSet<&'static str> {
    shadowsocks_crypto::available_ciphers()
        .iter()
        .copied()
        .filter(|name| {
            !CipherKind::from_str(name).unwrap().is_extra_aead()
                && *name != "2022-blake3-chacha8-poly1305"
        })
        .chain([
            "2022-blake3-aes-128-gcm",
            "2022-blake3-aes-256-gcm",
            "2022-blake3-chacha20-poly1305",
        ])
        .collect()
}
pub fn password(method: &str) -> &'static str {
    match method {
        "2022-blake3-aes-128-gcm" => "MDEyMzQ1Njc4OWFiY2RlZg==",
        method if method.starts_with("2022-") => "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
        _ => "reference-password",
    }
}
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
pub async fn ready(port: u16) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
pub async fn socks(port: u16, command: u8, target: u16) -> (TcpStream, u16) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    stream.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    let mut request = vec![5, command, 0, 1, 127, 0, 0, 1];
    request.extend_from_slice(&target.to_be_bytes());
    stream.write_all(&request).await.unwrap();
    let mut header = [0; 4];
    stream.read_exact(&mut header).await.unwrap();
    assert_eq!(header[1], 0);
    let size = match header[3] {
        1 => 4,
        4 => 16,
        3 => stream.read_u8().await.unwrap() as usize,
        _ => panic!("bad SOCKS address"),
    };
    let mut address = vec![0; size];
    stream.read_exact(&mut address).await.unwrap();
    let port = stream.read_u16().await.unwrap();
    (stream, port)
}
