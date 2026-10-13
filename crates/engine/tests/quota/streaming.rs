use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use zero_config::RuntimeConfig;
use zero_engine::{inspect_principal_quota_state, Engine, PrincipalQuotaStateStatus};

fn verify(path: &Path) {
    let mut config = RuntimeConfig::parse(
        r#"{"inbounds":[],"outbounds":[],"route":{"rules":[],"final":{"type":"direct"}}}"#,
    )
    .unwrap();
    config.runtime.principal_quota_state_path = Some(path.display().to_string());
    let bytes = std::fs::metadata(path).unwrap().len();
    let report = inspect_principal_quota_state(&config).unwrap();
    assert_eq!(report.status, PrincipalQuotaStateStatus::Ready);
    assert_eq!(report.balances, Some(1));
    assert_eq!(report.bytes, bytes);
    assert!(
        !path.with_extension("json.zero.lock").exists(),
        "inspection must not acquire a writer lease"
    );
    let engine = Engine::new(config).expect("streaming quota startup");
    assert_eq!(std::fs::metadata(path).unwrap().len(), bytes);
    drop(engine);
}

#[test]
fn large_quota_preflight_and_startup_fit_in_limited_memory() {
    #[cfg(target_os = "linux")]
    if let Some(path) = std::env::var_os("ZERO_QUOTA_STREAMING_CHILD") {
        verify(Path::new(&path));
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("quota.json");
    let mut file = BufWriter::new(File::create(&path).unwrap());
    let spaces = vec![b' '; 1024 * 1024];
    for _ in 0..256 {
        file.write_all(&spaces).unwrap();
    }
    file.write_all(br#"{"version":1,"balances":[{"key":{"principal_key":"retained","policy_revision":2,"initial_bytes":100},"remaining_bytes":37}]}"#).unwrap();
    file.flush().unwrap();
    #[cfg(not(target_os = "linux"))]
    verify(&path);
    #[cfg(target_os = "linux")]
    {
        use std::process::Command;
        use std::time::{Duration, Instant};
        let mut child = Command::new("sh")
            .args(["-c", "ulimit -v 196608 || exit 99; exec \"$1\" --exact \"$2\" --nocapture --test-threads=1", "quota-regression"])
            .arg(std::env::current_exe().unwrap())
            .arg(std::thread::current().name().unwrap())
            .env("ZERO_QUOTA_STREAMING_CHILD", &path).spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "quota preflight under 192 MiB failed: {status}"
                );
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("quota regression exceeded three minutes");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}
