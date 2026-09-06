#![cfg(feature = "socks5")]

use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

fn config(port: u16) -> serde_json::Value {
    serde_json::json!({
        "inbounds": [{"tag":"in", "listen":{"address":"127.0.0.1","port":port},
                      "protocol":{"type":"socks5"}}],
        "outbounds": [],
        "route": {"rules":[],"final":{"type":"direct"}},
        "runtime": {
            "principal_quota_state_path":"quota/state.json",
            "dns": {
                "servers":{"system":{"type":"system"}}, "default_server":"system",
                "answer":{"type":"fake_ip","cidr":"198.18.0.0/24","ttl_seconds":3600}
            }
        }
    })
}

fn write_config(directory: &Path, value: &serde_json::Value) -> std::path::PathBuf {
    let path = directory.join("config.json");
    std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
    path
}

fn validate(path: &Path, state: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_zero"))
        .arg("validate")
        .arg(path)
        .env("ZERO_DNS_STATE_DIR", state)
        // Relative resource resolution must use the config's directory.
        .current_dir(std::env::temp_dir())
        .output()
        .unwrap()
}

fn assert_valid(output: Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("config valid: 1 inbounds"), "{stdout}");
    assert!(!stdout.contains("engine started"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("engine started"));
}

#[test]
fn validate_does_not_open_runtime_state_or_bind_listeners() {
    let directory = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let path = write_config(
        directory.path(),
        &config(listener.local_addr().unwrap().port()),
    );
    let state = directory.path().join("dns-state");
    assert_valid(validate(&path, &state));
    assert!(!state.exists());
    assert!(!directory.path().join("quota").exists());

    // Configuration validation must not read/repair an existing runtime journal.
    std::fs::create_dir(directory.path().join("quota")).unwrap();
    let quota = directory.path().join("quota/state.json");
    std::fs::write(&quota, "runtime-owned sentinel").unwrap();
    assert_valid(validate(&path, &state));
    assert_eq!(
        std::fs::read_to_string(quota).unwrap(),
        "runtime-owned sentinel"
    );
}

struct Running(Child);
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn validate_succeeds_while_running_kernel_owns_fake_ip_state() {
    let directory = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = write_config(directory.path(), &config(address.port()));
    let state = directory.path().join("dns-state");
    drop(listener);
    #[cfg(unix)]
    let socket = directory.path().join("control.sock");
    #[cfg(windows)]
    let socket = std::path::PathBuf::from(format!(
        r"\\.\pipe\zero-validate-{}-{}",
        std::process::id(),
        address.port()
    ));
    let mut runtime = Running(
        Command::new(env!("CARGO_BIN_EXE_zero"))
            .args(["run", "--parent-lifetime-stdin", "--control-socket"])
            .arg(socket)
            .arg(&path)
            .env("ZERO_DNS_STATE_DIR", &state)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            runtime.0.try_wait().unwrap().is_none(),
            "runtime exited before ready"
        );
        if TcpStream::connect_timeout(&address, Duration::from_millis(50)).is_ok() {
            break;
        }
        assert!(Instant::now() < deadline, "runtime not ready");
        std::thread::sleep(Duration::from_millis(25));
    }
    let snapshot = || {
        let mut files: Vec<_> = std::fs::read_dir(&state)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                // Windows enforces byte-range locks even for reads. The lease
                // file is coordination, not journal data; inspect its metadata
                // and verify contention separately instead of reading it.
                let contents = if path.extension().is_some_and(|ext| ext == "lock") {
                    None
                } else {
                    Some(std::fs::read(&path).unwrap())
                };
                (
                    path.file_name().unwrap().to_owned(),
                    std::fs::metadata(&path).unwrap().len(),
                    contents,
                )
            })
            .collect();
        files.sort();
        files
    };
    let before = snapshot();
    let lock_name = &before
        .iter()
        .find(|(name, _, _)| name.to_string_lossy().ends_with(".lock"))
        .expect("running kernel created its Fake-IP lease")
        .0;
    let assert_lease_held = || {
        let lease = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(state.join(lock_name))
            .unwrap();
        assert!(matches!(
            lease.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
    };
    assert_lease_held();
    assert_valid(validate(&path, &state));
    assert_lease_held();
    assert_eq!(snapshot(), before);
    assert!(runtime.0.try_wait().unwrap().is_none());
    assert!(TcpStream::connect_timeout(&address, Duration::from_secs(1)).is_ok());
}

#[test]
fn validate_still_rejects_invalid_dns_and_route_targets() {
    let directory = tempfile::tempdir().unwrap();
    let mut value = config(12345);
    value["runtime"]["dns"]["answer"]["cidr"] = "invalid-cidr".into();
    let path = write_config(directory.path(), &value);
    assert!(!validate(&path, directory.path()).status.success());
    let mut value = config(12345);
    value["route"]["final"] = serde_json::json!({"type":"route","outbound":"missing"});
    let path = write_config(directory.path(), &value);
    assert!(!validate(&path, directory.path()).status.success());
}

#[test]
fn validate_resolves_and_checks_rule_files_relative_to_original_config() {
    let directory = tempfile::tempdir().unwrap();
    let mut value = config(12345);
    value["route"]["rule_sets"] = serde_json::json!([{
        "tag":"ads", "type":"file", "path":"rules/ads.json", "format":"zero_rule_ir"
    }]);
    value["route"]["rules"] = serde_json::json!([{
        "condition":{"type":"rule_set","tag":"ads"}, "action":{"type":"reject"}
    }]);
    let path = write_config(directory.path(), &value);
    std::fs::create_dir(directory.path().join("rules")).unwrap();
    let rules = directory.path().join("rules/ads.json");
    std::fs::write(&rules, r#"{"version":1,"name":"ads","rules":[{"type":"domain_suffix","value":"blocked.example"}]}"#).unwrap();
    assert_valid(validate(&path, &directory.path().join("dns-state")));
    std::fs::write(&rules, "malformed rule file").unwrap();
    let output = validate(&path, &directory.path().join("dns-state"));
    assert!(!output.status.success());
    assert!(!directory.path().join("dns-state").exists());
}
