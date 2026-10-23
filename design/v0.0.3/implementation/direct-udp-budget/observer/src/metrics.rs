//! Native sampler reads the actual application's Handle; no observer Tokio task.
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const MAX_SAMPLES: usize = 30_000;
struct State {
    phase: String,
    samples: Vec<Value>,
    truncated: bool,
}
pub struct Metrics {
    state: Arc<Mutex<State>>,
    stopping: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}
impl Metrics {
    pub fn start(handle: tokio::runtime::Handle) -> Self {
        let state = Arc::new(Mutex::new(State {
            phase: "before_proxy".into(),
            samples: Vec::new(),
            truncated: false,
        }));
        let stopping = Arc::new(AtomicBool::new(false));
        let shared = state.clone();
        let stop = stopping.clone();
        let thread = thread::spawn(move || {
            let started = Instant::now();
            let metrics = handle.metrics();
            while !stop.load(Ordering::Acquire) {
                let mut state = shared.lock().expect("metrics state");
                if state.samples.len() < MAX_SAMPLES {
                    let phase = state.phase.clone();
                    state.samples.push(json!({"phase": phase,
                        "elapsed_ns": started.elapsed().as_nanos(),
                        "alive_tasks": metrics.num_alive_tasks(),
                        "global_queue_depth": metrics.global_queue_depth(),
                        "application_pending_payload_bytes": null}));
                } else {
                    state.truncated = true;
                }
                drop(state);
                thread::sleep(Duration::from_millis(10));
            }
        });
        Self {
            state,
            stopping,
            thread: Some(thread),
        }
    }
    pub fn phase(&self, phase: &str) {
        self.state.lock().expect("metrics state").phase = phase.into();
    }
    pub fn finish(mut self) -> Result<Value, String> {
        self.stopping.store(true, Ordering::Release);
        self.thread
            .take()
            .expect("sampler owner")
            .join()
            .map_err(|_| "actual native metrics thread panicked")?;
        let mut state = self.state.lock().expect("metrics state");
        Ok(
            json!({"metrics": "Tokio Handle.metrics; non-linearizable 10ms samples",
            "sampler": "one actual joined native thread; zero observer Tokio tasks",
            "application_pending_payload_bytes": null,
            "application_queue_reason": "no public owner accounting API; no RSS estimate",
            "sample_cap": MAX_SAMPLES, "truncated": state.truncated,
            "samples": std::mem::take(&mut state.samples)}),
        )
    }
}
