//! A regressed blocking dispatcher must not strand the workspace test runner.
use super::*;

pub(super) fn run(name: &str, case: impl std::future::Future<Output = ()>) {
    const CHILD: &str = "ZERO_TEST_RELOAD_CASE";
    if std::env::var(CHILD).ok().as_deref() == Some(name) {
        std::fs::write(
            std::env::var_os("ZERO_TEST_RELOAD_READY").unwrap(),
            b"ready",
        )
        .unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(case);
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let ready = dir.path().join("ready");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([name, "--nocapture"])
        .env(CHILD, name)
        .env("ZERO_TEST_RELOAD_READY", &ready)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let startup = std::time::Instant::now() + Duration::from_secs(180);
    let mut execution = None;
    loop {
        if child.try_wait().unwrap().is_some() {
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "{name}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        if execution.is_none() && ready.exists() {
            execution = Some(std::time::Instant::now() + Duration::from_secs(20));
        }
        if std::time::Instant::now() >= execution.unwrap_or(startup) {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!(
                "{name} hung: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
}
