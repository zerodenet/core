use crate::validation::{PluginConfig, PluginMode};
pub(super) fn config() -> PluginConfig {
    PluginConfig {
        command: "python3".into(), options: None, mode: PluginMode::TcpOnly,
        args: vec!["-c".into(), "import os,socket,time; s=socket.socket(); s.bind((os.environ['SS_LOCAL_HOST'],int(os.environ['SS_LOCAL_PORT']))); s.listen(); time.sleep(60)".into()],
    }
}
pub(super) struct StartupMarker(pub(super) std::path::PathBuf);
impl StartupMarker {
    pub(super) fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zero-ss-start-{}-{id}", std::process::id()));
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        Self(path)
    }
    pub(super) fn config(&self, tail: &str) -> PluginConfig {
        let mut config = config();
        config.args = vec![
            "-c".into(),
            format!("import os,sys,time,socket; s=socket.socket(); s.bind((os.environ['SS_LOCAL_HOST'],int(os.environ['SS_LOCAL_PORT']))); f=open(sys.argv[1],'a'); f.write(str(os.getpid())+'\\n'); f.close(); {tail}"),
            self.0.to_str().unwrap().into(),
        ];
        config
    }
    pub(super) async fn pid(&self) -> u32 {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                if let Ok(text) = std::fs::read_to_string(&self.0) {
                    if let Ok(pid) = text.trim().parse() {
                        return pid;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
}

/// Hold the real TCP probe at a known suspension point. No successful probe is
/// fabricated: releasing the gate would execute the normal TCP connect.
#[derive(Default)]
pub(super) struct PendingProbe {
    entered: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}
type ProbeFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = std::io::Result<tokio::net::TcpStream>> + Send>,
>;
impl PendingProbe {
    pub(super) fn probe(&self) -> impl FnMut(std::net::SocketAddr) -> ProbeFuture + Send + 'static {
        let entered = self.entered.clone();
        let release = self.release.clone();
        move |endpoint| {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                tokio::net::TcpStream::connect(endpoint).await
            })
        }
    }
    pub(super) async fn entered(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(3), self.entered.notified())
            .await
            .unwrap();
    }
}
impl Drop for StartupMarker {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
