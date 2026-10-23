//! Library observation without observer Tokio tasks or allocator overrides.
mod driver;
mod metrics;
use serde_json::{json, Value};
use std::error::Error;
use std::io::{BufRead, Read, Write};
use std::time::Duration;

fn input_thread(
    sender: tokio::sync::mpsc::Sender<Result<Value, String>>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let input = std::io::stdin();
        let mut reader = input.lock();
        loop {
            let mut bytes = Vec::new();
            let result = reader
                .by_ref()
                .take(1_048_577)
                .read_until(b'\n', &mut bytes);
            let command = match result {
                Ok(0) => break,
                Ok(_) if bytes.len() <= 1_048_576 => {
                    serde_json::from_slice(&bytes).map_err(|e| e.to_string())
                }
                Ok(_) => Err("probe input frame exceeds 1 MiB".into()),
                Err(error) => Err(error.to_string()),
            };
            let failed = command.is_err();
            if sender.blocking_send(command).is_err() || failed {
                break;
            }
        }
    })
}
fn reply(value: Value) -> Result<(), Box<dyn Error>> {
    let output = std::io::stdout();
    let mut output = output.lock();
    serde_json::to_writer(&mut output, &value)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}
#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        std::fs::read_to_string("/sys/fs/cgroup/cpu.max")?.trim(),
        "100000 100000"
    );
    assert_eq!(
        std::fs::read_to_string("/sys/fs/cgroup/memory.max")?.trim(),
        "1073741824"
    );
    assert_eq!(
        std::fs::read_to_string("/sys/fs/cgroup/memory.swap.max")?.trim(),
        "0"
    );
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| "crypto provider already installed")?;
    let sampler = metrics::Metrics::start(tokio::runtime::Handle::current());
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let input = input_thread(sender);
    let mut driver = driver::Driver::default();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut shutdown_id = Value::Null;
    let mut failed = false;
    while let Some(command) = receiver.recv().await {
        let command = match command {
            Ok(command) => command,
            Err(error) => {
                reply(json!({"ok": false, "error": error}))?;
                failed = true;
                break;
            }
        };
        let id = command["id"].clone();
        if command["op"] == "shutdown" {
            shutdown_id = id;
            break;
        }
        let result = if command["op"] == "phase" {
            sampler.phase(command["phase"].as_str().unwrap_or("unspecified"));
            Ok(json!({"phase_installed": true}))
        } else {
            driver.execute(&command).await
        };
        reply(match result {
            Ok(result) => json!({"id": id, "ok": true, "result": result}),
            Err(error) => json!({"id": id, "ok": false, "error": error}),
        })?;
    }
    sampler.phase("shutdown_with_sources_live");
    let result = driver.shutdown().await;
    sampler.phase("after_shutdown_and_owner_drop");
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Python closes stdin after shutdown. Receiver drop also releases a
    // bounded blocking_send; join the actual reader, never detach it.
    drop(receiver);
    input.join().map_err(|_| "actual input thread panicked")?;
    let observation = sampler.finish()?;
    let ok = result.is_ok() && !failed;
    reply(json!({"id": shutdown_id, "ok": ok, "shutdown": result,
        "task_observation": observation, "runtime_workers": 1,
        "scope": "public proxy library; CLI/IPC/Connector application tasks excluded",
        "harness_tokio_tasks_spawned": 0, "harness_native_threads": 2,
        "observer_source": include_str!("../source.json")}))?;
    if !ok {
        return Err("actual library run/shutdown failed; raw response retained".into());
    }
    Ok(())
}
