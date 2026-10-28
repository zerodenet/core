use serde_json::{json, Value};
use std::{
    net::SocketAddr,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};
use zero_platform_tokio::contract::CancellationToken;

pub(crate) fn credentials(protocol: &str) -> Value {
    if protocol == "trojan" {
        json!({"type":protocol,"password":"mux-password"})
    } else {
        json!({"type":protocol,"id":"11111111-2222-3333-4444-555555555555"})
    }
}
pub(crate) fn inbound_credentials(protocol: &str) -> Value {
    let mut user = credentials(protocol);
    user.as_object_mut().unwrap().remove("type");
    json!({"type":protocol,"users":[user]})
}
pub(crate) fn config_at(value: Value, path: &Path) -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse_with_source_dir(&value.to_string(), Some(path.to_owned()))
        .unwrap()
}
pub(crate) fn material() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    std::fs::write(dir.path().join("ca.pem"), cert.cert.pem()).unwrap();
    std::fs::write(dir.path().join("key.pem"), cert.signing_key.serialize_pem()).unwrap();
    let other = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    std::fs::write(dir.path().join("untrusted.pem"), other.cert.pem()).unwrap();
    dir
}
pub(crate) struct Fixture {
    pub(crate) front: SocketAddr,
    pub(crate) echo: SocketAddr,
    connections: Arc<AtomicUsize>,
    cancel: CancellationToken,
    workers: Vec<JoinHandle<()>>,
}
impl Fixture {
    pub(crate) async fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = listener.local_addr().unwrap();
        let echo_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let echo = echo_listener.local_addr().unwrap();
        let cancel = CancellationToken::new();
        let connections = Arc::new(AtomicUsize::new(0));
        let stop = cancel.clone();
        let count = connections.clone();
        let front_worker = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {biased;
                    _=stop.cancelled()=>break,
                    accepted=listener.accept()=>{
                        let (mut client,_)=accepted.unwrap();
                        count.fetch_add(1,Ordering::SeqCst);
                        let stop=stop.clone();
                        tasks.spawn(async move {
                            let mut server=TcpStream::connect(upstream).await.unwrap();
                            tokio::select! {biased;
                                _=stop.cancelled()=>{},
                                // Failed TLS/WS authentication can close either
                                // side. This fixture only counts actual sockets.
                                _=tokio::io::copy_bidirectional(&mut client,&mut server)=>{},
                            }
                        });
                    }
                    result=tasks.join_next(),if !tasks.is_empty()=>{result.unwrap().unwrap();}
                }
            }
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        });
        let stop = cancel.clone();
        let echo_worker = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {biased;
                    _=stop.cancelled()=>break,
                    accepted=echo_listener.accept()=>{
                        let (mut stream,_)=accepted.unwrap(); let stop=stop.clone();
                        tasks.spawn(async move {
                            tokio::select! {biased;
                                _=stop.cancelled()=>{},
                                _=async {
                                    let mut data=[0;64];
                                    loop {let size=stream.read(&mut data).await.unwrap();
                                        if size==0 {break;} stream.write_all(&data[..size]).await.unwrap();}
                                }=>{},
                            }
                        });
                    }
                    result=tasks.join_next(),if !tasks.is_empty()=>{result.unwrap().unwrap();}
                }
            }
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        });
        Self {
            front,
            echo,
            connections,
            cancel,
            workers: vec![front_worker, echo_worker],
        }
    }
    pub(crate) fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
    pub(crate) async fn finish(self) {
        self.cancel.cancel();
        for worker in self.workers {
            worker.await.unwrap();
        }
    }
}
