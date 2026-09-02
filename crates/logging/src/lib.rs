//! Structured logging for Zero — non-blocking, split output, event bridge,
//! rate-limited.  Works in any binary, not tied to the CLI layer, usable
//! from any crate that depends on `zero-logging`.

use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};
use zero_config::LogConfig;

// ── public API ────────────────────────────────────────────────────────

/// Keeps non-blocking file writers alive and flushes them when dropped.
///
/// Callers must retain this guard for the complete process lifetime. In
/// particular, drop it explicitly before terminating through
/// [`std::process::exit`], which does not run destructors itself.
#[must_use = "dropping the tracing guard immediately stops file log writers"]
pub struct TracingGuard {
    _file_guards: Vec<tracing_appender::non_blocking::WorkerGuard>,
}

/// Initialise tracing.  Must be called exactly once, before any work.
pub fn init_tracing(config: &LogConfig) -> TracingGuard {
    let default_level = config.level.as_str();

    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(default_level))
        .expect("valid tracing log level");

    let rate_limiter: Option<Arc<RateLimiter>> = config
        .rate_limit
        .map(|r| Arc::new(RateLimiter::new(r.max_per_second)));
    let rate_filter = {
        let rl = rate_limiter.clone();
        tracing_subscriber::filter::filter_fn(move |_meta: &tracing::Metadata<'_>| {
            rl.as_ref().map(|l| l.allow()).unwrap_or(true)
        })
    };

    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(false)
        .compact()
        .with_filter(rate_filter)
        .with_filter(filter);

    let mut layers: Vec<Box<dyn Layer<_> + Send + Sync>> =
        vec![Box::new(stderr_layer), Box::new(WarningBridgeLayer)];

    let mut guards = Vec::new();

    for file_cfg in &config.files {
        let level = file_cfg.level.as_deref().unwrap_or(default_level);
        let file_filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new(level));
        let writer = RotatingWriter::new(&file_cfg.path, file_cfg.max_bytes, file_cfg.max_files)
            .expect("failed to open log file");
        let (non_blocking, guard) = tracing_appender::non_blocking(writer);
        guards.push(guard);

        let file_rate_filter = {
            let rl = rate_limiter.clone();
            tracing_subscriber::filter::filter_fn(move |_meta: &tracing::Metadata<'_>| {
                rl.as_ref().map(|l| l.allow()).unwrap_or(true)
            })
        };

        let file_layer = tracing_subscriber::fmt::layer()
            .with_target(false)
            .with_ansi(false)
            .with_writer(non_blocking)
            .json()
            .with_filter(file_rate_filter)
            .with_filter(file_filter);

        layers.push(Box::new(file_layer));
    }

    let _ = tracing_subscriber::registry().with(layers).try_init();
    TracingGuard {
        _file_guards: guards,
    }
}

type WarningSink = Box<dyn Fn(&str, &str) + Send + Sync>;

/// Callback that receives every `warn` / `error` log line so it can be
/// forwarded to the engine event log.
static WARNING_SINK: OnceLock<WarningSink> = OnceLock::new();

/// Register the engine warning callback.  Safe to call after `init_tracing`.
pub fn set_warning_sink(f: impl Fn(&str, &str) + Send + Sync + 'static) {
    let _ = WARNING_SINK.set(Box::new(f));
}

// ── Warning bridge ────────────────────────────────────────────────────

struct WarningBridgeLayer;

impl<S: tracing::Subscriber> Layer<S> for WarningBridgeLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let meta = event.metadata();
        if !matches!(*meta.level(), tracing::Level::WARN | tracing::Level::ERROR) {
            return;
        }
        let code = meta.target().split("::").last().unwrap_or(meta.target());
        let mut message = String::new();
        let mut visitor = MessageVisitor(&mut message);
        event.record(&mut visitor);

        if let Some(sink) = WARNING_SINK.get() {
            sink(code, &message);
        }
    }
}

struct MessageVisitor<'a>(&'a mut String);

impl tracing::field::Visit for MessageVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            use std::fmt::Write;
            let _ = write!(self.0, "{value:?}");
        }
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.0.push_str(value);
        }
    }
}

// ── Rate limiter ──────────────────────────────────────────────────────

struct RateLimiter {
    window_start: AtomicU64,
    count: AtomicU64,
    max: u64,
}

impl RateLimiter {
    fn new(max: u64) -> Self {
        Self {
            window_start: AtomicU64::new(now_secs()),
            count: AtomicU64::new(0),
            max,
        }
    }

    fn allow(&self) -> bool {
        let now = now_secs();
        let window = self.window_start.load(Ordering::Relaxed);
        if now != window
            && self
                .window_start
                .compare_exchange(window, now, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
        {
            self.count.store(1, Ordering::Release);
            return true;
        }
        if self.count.fetch_add(1, Ordering::Acquire) < self.max {
            return true;
        }
        self.count.fetch_sub(1, Ordering::Release);
        false
    }
}

fn now_secs() -> u64 {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_secs()
}

// ── Rotating file writer ──────────────────────────────────────────────

struct RotatingWriter {
    path: PathBuf,
    max_bytes: u64,
    max_files: usize,
    file: fs::File,
    written: u64,
}

impl RotatingWriter {
    fn new(path: &str, max_bytes: u64, max_files: usize) -> io::Result<Self> {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let written = file.metadata()?.len();
        Ok(Self {
            path,
            max_bytes,
            max_files,
            file,
            written,
        })
    }

    fn rotate(&mut self) -> io::Result<()> {
        for i in (1..self.max_files).rev() {
            let src = if i == 1 {
                self.path.clone()
            } else {
                self.path.with_extension(format!("{}", i - 1))
            };
            let dst = self.path.with_extension(format!("{i}"));
            if src.exists() {
                let _ = fs::rename(&src, &dst);
            }
        }
        self.file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.written = 0;
        Ok(())
    }
}

impl Write for RotatingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.written + buf.len() as u64 > self.max_bytes {
            self.rotate()?;
        }

        let n = self.file.write(buf)?;
        self.written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
