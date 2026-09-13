//! Opt-in wire capture for local plaintext reference diagnostics.
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub struct Tap(tokio::task::JoinHandle<()>);
impl Drop for Tap {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct Capture {
    path: PathBuf,
    bytes: Arc<Mutex<Vec<u8>>>,
}
impl Drop for Capture {
    fn drop(&mut self) {
        let _ = std::fs::write(&self.path, &*self.bytes.lock().unwrap());
    }
}
pub async fn start(port: u16) -> Option<(u16, Tap)> {
    let directory = std::env::var_os("XHTTP_TAP_DIR")?;
    let root = PathBuf::from(directory).join(port.to_string());
    std::fs::create_dir_all(&root).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound = listener.local_addr().unwrap().port();
    let sequence = Arc::new(AtomicUsize::new(0));
    let task = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                incoming = listener.accept() => {
                    let (incoming, _) = incoming.unwrap(); incoming.set_nodelay(true).unwrap();
                    let root = root.clone(); let index = sequence.fetch_add(1, Ordering::SeqCst);
                    tasks.spawn(async move {
                        let outgoing = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap(); outgoing.set_nodelay(true).unwrap();
                        let (ir,iw) = incoming.into_split(); let (or,ow) = outgoing.into_split();
                        async fn pump(mut read: tokio::net::tcp::OwnedReadHalf, mut write: tokio::net::tcp::OwnedWriteHalf, path: PathBuf) {
                            let capture = Capture { path, bytes: Arc::new(Mutex::new(Vec::new())) }; let mut data = [0;16384];
                            while let Ok(n) = read.read(&mut data).await {
                                if n == 0 { break; }
                                { let mut bytes = capture.bytes.lock().unwrap(); if bytes.len() < 1024*1024 { bytes.extend_from_slice(&data[..n]); } }
                                if write.write_all(&data[..n]).await.is_err() { break; }
                            }
                            let _ = write.shutdown().await;
                        }
                        tokio::join!(pump(ir, ow, root.join(format!("{index}-up.bin"))), pump(or, iw, root.join(format!("{index}-down.bin"))));
                    });
                },
                _ = tasks.join_next(), if !tasks.is_empty() => {},
            }
        }
    });
    Some((bound, Tap(task)))
}
