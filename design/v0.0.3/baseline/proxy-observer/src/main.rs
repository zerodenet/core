//! Observe real library Tokio tasks without modifying the old runtime.
//! Echo peers use native threads; the observer does not spawn Tokio tasks.

mod allocation;

use std::error::Error;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

async fn samples(phase: &str) -> Value {
    let metrics = tokio::runtime::Handle::current().metrics();
    let mut values = Vec::new();
    for _ in 0..20 {
        values.push(json!({
            "alive_tasks": metrics.num_alive_tasks(),
            "global_queue_depth": metrics.global_queue_depth(),
        }));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let allocations = allocation::snapshot();
    json!({"phase": phase, "samples": values, "allocation_observation": allocations})
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    let cpu = std::fs::read_to_string("/sys/fs/cgroup/cpu.max")?;
    let memory = std::fs::read_to_string("/sys/fs/cgroup/memory.max")?;
    let cpu_values: Vec<_> = cpu.split_whitespace().collect();
    assert_eq!(cpu_values[0], cpu_values[1], "requires one CPU quota");
    assert_eq!(memory.trim(), "1073741824", "requires 1 GiB cgroup");
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| "crypto provider already installed")?;

    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let destination = listener.local_addr()?;
    let stopping = Arc::new(AtomicBool::new(false));
    let stop = stopping.clone();
    let echo = thread::spawn(move || {
        let mut handlers = Vec::new();
        while !stop.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((mut stream, _)) => handlers.push(thread::spawn(move || {
                    let mut buffer = [0; 16384];
                    while let Ok(count) = stream.read(&mut buffer) {
                        if count == 0 || stream.write_all(&buffer[..count]).is_err() {
                            break;
                        }
                    }
                })),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(_) => break,
            }
        }
        for handler in handlers {
            handler.join().expect("echo handler exits");
        }
    });
    let reservation = TcpListener::bind("127.0.0.1:0")?;
    let port = reservation.local_addr()?.port();
    drop(reservation);
    let config = RuntimeConfig::parse(
        &json!({
            "inbounds": [{"tag":"probe","listen":{"address":"127.0.0.1","port":port},
                          "protocol":{"type":"socks5"}}],
            "outbounds": [], "route":{"rules":[],"final":{"type":"direct"}}
        })
        .to_string(),
    )?;
    let mut phases = vec![samples("before_proxy").await];
    let proxy = Proxy::new(config)?;
    let running = proxy.spawn();
    for attempt in 0..200 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            break;
        }
        assert_ne!(attempt, 199, "proxy readiness");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    phases.push(samples("ready_idle").await);
    let mut streams = Vec::new();
    for _ in 0..32 {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
        stream.write_all(&[5, 1, 0]).await?;
        let mut auth = [0; 2];
        stream.read_exact(&mut auth).await?;
        assert_eq!(auth, [5, 0]);
        let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
        request.extend_from_slice(&destination.port().to_be_bytes());
        stream.write_all(&request).await?;
        let mut reply = [0; 10];
        stream.read_exact(&mut reply).await?;
        assert_eq!(reply[1], 0);
        stream.write_all(b"task-observation").await?;
        let mut payload = [0; 16];
        stream.read_exact(&mut payload).await?;
        assert_eq!(&payload, b"task-observation");
        streams.push(stream);
    }
    phases.push(samples("32_live_streams").await);
    drop(streams);
    tokio::time::sleep(Duration::from_millis(300)).await;
    phases.push(samples("after_streams_closed").await);
    running.shutdown().await?;
    drop(proxy);
    tokio::time::sleep(Duration::from_millis(300)).await;
    phases.push(samples("after_shutdown_and_owner_drop").await);
    stopping.store(true, Ordering::Release);
    echo.join().expect("echo owner exits");
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "scope":"public proxy library; CLI/IPC/Connector application tasks excluded",
            "metrics":"Tokio num_alive_tasks/global_queue_depth; samples are not linearizable",
            "runtime_workers":1,"streams":32,"phases":phases
        }))?
    );
    Ok(())
}
