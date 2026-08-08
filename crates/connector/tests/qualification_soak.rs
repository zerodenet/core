#![cfg(feature = "sink-jsonl")]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::json;
use zero_api::{event_type, ApiEvent, EventFilter, EventSource, EventStream, RawApiEvent};
use zero_config::{ApiConfig, EventDispatcherConfig, EventSinkConfig};
use zero_connector::{spawn_event_dispatcher, EventDispatcherOptions, EventDispatcherStatusHandle};

#[derive(Clone)]
struct QualificationEventSource {
    first_sequence: usize,
    count: usize,
    started: Instant,
    interval: Duration,
}

struct QualificationEventStream {
    first_sequence: usize,
    end_sequence: usize,
    next_sequence: Mutex<usize>,
    started: Instant,
    interval: Duration,
}

impl QualificationEventSource {
    fn new(first_sequence: usize, count: usize, duration: Duration) -> Self {
        Self {
            first_sequence,
            count,
            started: Instant::now(),
            interval: if count == 0 {
                Duration::ZERO
            } else {
                duration.div_f64(count as f64)
            },
        }
    }

    fn visible_end_sequence(&self) -> usize {
        if self.interval.is_zero() {
            return self.first_sequence.saturating_add(self.count);
        }
        let visible =
            (self.started.elapsed().as_secs_f64() / self.interval.as_secs_f64()).floor() as usize;
        self.first_sequence.saturating_add(visible.min(self.count))
    }
}

impl EventStream for QualificationEventStream {
    fn recv(&self) -> Option<RawApiEvent> {
        self.try_recv()
    }

    fn try_recv(&self) -> Option<RawApiEvent> {
        let mut next = self.next_sequence.lock().expect("sequence lock");
        if *next >= self.end_sequence {
            return None;
        }
        let offset = next.saturating_sub(self.first_sequence);
        let due = self.started + self.interval.mul_f64(offset.saturating_add(1) as f64);
        if Instant::now() < due {
            return None;
        }
        let sequence = *next;
        *next += 1;
        drop(next);
        Some(build_event(sequence))
    }
}

impl EventSource for QualificationEventSource {
    type Stream = QualificationEventStream;

    fn subscribe(&self, _filter: EventFilter) -> zero_api::ApiResult<Self::Stream> {
        Ok(QualificationEventStream {
            first_sequence: self.first_sequence,
            end_sequence: self.first_sequence.saturating_add(self.count),
            next_sequence: Mutex::new(self.first_sequence),
            started: self.started,
            interval: self.interval,
        })
    }

    fn latest(&self, limit: usize, _filter: EventFilter) -> zero_api::ApiResult<Vec<RawApiEvent>> {
        Ok((self.first_sequence..self.visible_end_sequence())
            .take(limit)
            .map(build_event)
            .collect())
    }

    fn since(
        &self,
        sequence: u64,
        limit: usize,
        _filter: EventFilter,
    ) -> zero_api::ApiResult<zero_api::EventReplay> {
        let first = self
            .first_sequence
            .max(usize::try_from(sequence.saturating_add(1)).unwrap_or(usize::MAX));
        let events = (first..self.visible_end_sequence())
            .take(limit)
            .map(build_event)
            .collect::<Vec<_>>();
        let actual_from = events
            .first()
            .and_then(|event| event.sequence)
            .unwrap_or_else(|| sequence.saturating_add(1));
        Ok(zero_api::EventReplay {
            core_instance_id: "test-core".to_owned(),
            requested_after: sequence,
            actual_from,
            has_gap: actual_from > sequence.saturating_add(1),
            events,
        })
    }
}

#[tokio::test]
#[ignore = "long-running Zero connector qualification; configure with ZERO_CONNECTOR_SOAK_*"]
async fn connector_outbox_survives_sustained_delivery_and_restarts_without_loss() {
    let event_count = env_usize("ZERO_CONNECTOR_SOAK_EVENTS", 100_000);
    let restart_cycles = env_usize("ZERO_CONNECTOR_SOAK_RESTARTS", 10).max(1);
    let timeout_seconds = env_u64("ZERO_CONNECTOR_SOAK_TIMEOUT_SECONDS", 900);
    let minimum_seconds = env_u64("ZERO_CONNECTOR_SOAK_MIN_SECONDS", 0);
    let cycle_duration = Duration::from_secs_f64(minimum_seconds as f64 / restart_cycles as f64);
    let directory = tempfile::tempdir().expect("qualification directory");
    let sink_path = directory.path().join("delivered.jsonl");
    let outbox_path = directory.path().join("outbox.jsonl");
    let api = ApiConfig {
        event_sinks: vec![EventSinkConfig::JsonLines {
            tag: "qualification".to_owned(),
            path: sink_path.display().to_string(),
            events: vec![event_type::FLOW_COMPLETED.to_owned()],
            source_id: Some("zero-qualification".to_owned()),
        }],
        outbox_path: Some(outbox_path.display().to_string()),
        dispatcher: EventDispatcherConfig {
            max_in_memory_deliveries: 256,
            replay_batch_size: 512,
            ..Default::default()
        },
        ..Default::default()
    };
    let options = EventDispatcherOptions {
        poll_interval: Duration::from_millis(1),
        max_retry_attempts: 10,
    };

    let started = Instant::now();
    let rss_sampler = RssSampler::start();
    let mut delivered = 0usize;
    let mut peak_outbox_bytes = 0u64;
    for cycle in 0..restart_cycles {
        let remaining = event_count.saturating_sub(delivered);
        let cycles_left = restart_cycles - cycle;
        let cycle_events = remaining.div_ceil(cycles_left);
        let first_sequence = delivered + 1;
        let dispatcher = spawn_event_dispatcher(
            QualificationEventSource::new(first_sequence, cycle_events, cycle_duration),
            api.clone(),
            None,
            options,
        )
        .expect("spawn qualification dispatcher")
        .expect("qualification dispatcher handle");
        if !wait_for_delivered(
            &dispatcher.status_handle(),
            cycle_events as u64,
            Duration::from_secs(timeout_seconds),
        )
        .await
        {
            let status = dispatcher.status_handle().sink_status();
            dispatcher.shutdown().await;
            panic!("qualification delivery timed out: {status:?}");
        }
        dispatcher.shutdown().await;
        delivered += cycle_events;
        peak_outbox_bytes = peak_outbox_bytes.max(
            std::fs::metadata(&outbox_path)
                .expect("outbox metadata")
                .len(),
        );
    }

    let lines_before_restart = count_lines(&sink_path);
    assert_eq!(lines_before_restart, event_count);
    let final_restart = spawn_event_dispatcher(
        QualificationEventSource::new(event_count.saturating_add(1), 0, Duration::ZERO),
        api,
        None,
        options,
    )
    .expect("spawn empty final restart")
    .expect("empty final restart handle");
    tokio::time::sleep(Duration::from_millis(250)).await;
    let final_status = final_restart.status_handle().sink_status();
    final_restart.shutdown().await;

    assert_eq!(
        count_lines(&sink_path),
        event_count,
        "fully acknowledged events were redelivered after restart"
    );
    let qualification = final_status
        .iter()
        .find(|status| status.name == "qualification")
        .expect("qualification sink status");
    assert_eq!(qualification.pending, 0);
    assert_eq!(qualification.total_delivered, 0);

    let elapsed = started.elapsed();
    assert!(
        elapsed + Duration::from_millis(50) >= Duration::from_secs(minimum_seconds),
        "qualification completed before the configured minimum duration"
    );
    let peak_rss_bytes = rss_sampler.finish();
    let throughput = event_count as f64 / elapsed.as_secs_f64().max(0.001);
    eprintln!(
        "zero connector qualification: events={event_count} restarts={restart_cycles} minimum_seconds={minimum_seconds} elapsed_seconds={:.3} events_per_second={throughput:.1} peak_rss_bytes={peak_rss_bytes} rss_supported={} peak_outbox_bytes={peak_outbox_bytes} sink_bytes={}",
        elapsed.as_secs_f64(),
        peak_rss_bytes > 0,
        std::fs::metadata(&sink_path)
            .expect("sink metadata")
            .len()
    );
}

fn build_event(sequence: usize) -> RawApiEvent {
    let mut event = ApiEvent::new(
        format!("qualification-flow-{sequence}"),
        event_type::FLOW_COMPLETED,
        1_760_000_000_000 + sequence as u64,
        json!({
            "flow_id": sequence.to_string(),
            "traffic": {
                "bytes_up": sequence % 8192,
                "bytes_down": sequence % 16384
            }
        }),
    );
    event.sequence = Some(sequence as u64);
    event.principal_key = Some(format!("principal:{}", sequence % 10_000));
    event
}

async fn wait_for_delivered(
    status: &EventDispatcherStatusHandle,
    expected: u64,
    timeout: Duration,
) -> bool {
    tokio::time::timeout(timeout, async {
        loop {
            let sink = status
                .sink_status()
                .into_iter()
                .find(|item| item.name == "qualification")
                .expect("qualification sink status");
            if sink.total_delivered >= expected && sink.pending == 0 {
                assert_eq!(sink.total_failed, 0);
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

fn count_lines(path: &std::path::Path) -> usize {
    std::fs::read_to_string(path)
        .expect("read qualification sink")
        .lines()
        .count()
}

struct RssSampler {
    stop: Arc<AtomicBool>,
    peak_rss_bytes: Arc<AtomicU64>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl RssSampler {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak_rss_bytes = Arc::new(AtomicU64::new(0));
        let worker_stop = stop.clone();
        let worker_peak = peak_rss_bytes.clone();
        let worker = std::thread::spawn(move || {
            while !worker_stop.load(Ordering::Relaxed) {
                sample_peak_rss(&worker_peak);
                std::thread::sleep(Duration::from_millis(50));
            }
            sample_peak_rss(&worker_peak);
        });
        Self {
            stop,
            peak_rss_bytes,
            worker: Some(worker),
        }
    }

    fn finish(mut self) -> u64 {
        self.stop_and_join();
        self.peak_rss_bytes.load(Ordering::Relaxed)
    }

    fn stop_and_join(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            worker.join().expect("RSS sampler thread");
        }
    }
}

impl Drop for RssSampler {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

fn sample_peak_rss(peak_rss_bytes: &AtomicU64) {
    if let Some(rss) = resident_set_bytes() {
        peak_rss_bytes.fetch_max(rss, Ordering::Relaxed);
    }
}

#[cfg(target_os = "linux")]
fn resident_set_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kibibytes = status.lines().find_map(|line| {
        line.strip_prefix("VmRSS:")
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.parse::<u64>().ok())
    })?;
    kibibytes.checked_mul(1_024)
}

#[cfg(windows)]
fn resident_set_bytes() -> Option<u64> {
    use std::ffi::c_void;
    use std::mem::{size_of, MaybeUninit};

    #[repr(C)]
    struct ProcessMemoryCounters {
        cb: u32,
        page_fault_count: u32,
        peak_working_set_size: usize,
        working_set_size: usize,
        quota_peak_paged_pool_usage: usize,
        quota_paged_pool_usage: usize,
        quota_peak_non_paged_pool_usage: usize,
        quota_non_paged_pool_usage: usize,
        pagefile_usage: usize,
        peak_pagefile_usage: usize,
    }

    #[link(name = "kernel32")]
    extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
    }
    #[link(name = "psapi")]
    extern "system" {
        fn GetProcessMemoryInfo(
            process: *mut c_void,
            counters: *mut ProcessMemoryCounters,
            size: u32,
        ) -> i32;
    }

    let size = u32::try_from(size_of::<ProcessMemoryCounters>()).ok()?;
    let mut counters = MaybeUninit::<ProcessMemoryCounters>::zeroed();
    // SAFETY: the Windows API receives a correctly sized writable structure,
    // and GetCurrentProcess returns a valid pseudo-handle for this process.
    unsafe {
        (*counters.as_mut_ptr()).cb = size;
        if GetProcessMemoryInfo(GetCurrentProcess(), counters.as_mut_ptr(), size) == 0 {
            return None;
        }
        Some(counters.assume_init().working_set_size as u64)
    }
}

#[cfg(not(any(target_os = "linux", windows)))]
fn resident_set_bytes() -> Option<u64> {
    None
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
