use std::env;
use std::error::Error;
use std::process;

mod application;
mod artifact;
mod cli;
mod error_report;
mod hooks;
#[cfg(feature = "status-api")]
mod http_adapter;
mod ipc;
mod output;
mod rule_set_fetch;

#[tokio::main]
async fn main() {
    // Parse CLI args to find the config path, then initialise tracing
    // from `runtime.log` before any other work so all logs are captured.
    let args: Vec<String> = env::args().collect();
    let config_path = cli::config_path_from_args(&args);
    let tracing_guard = init_tracing_from_config(config_path.unwrap_or(""));
    install_panic_hook();

    if let Err(error) = try_main().await {
        tracing::error!(error = %error, "zero process terminating with fatal error");
        error_report::print_error(error.as_ref());
        drop(tracing_guard);
        process::exit(1);
    }
}

fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic| {
        tracing::error!(panic = %panic, "zero process panicked");
        eprintln!("fatal panic: {panic}");
        previous(panic);
    }));
}

async fn try_main() -> Result<(), Box<dyn Error>> {
    // Install rustls crypto provider before any TLS operation.
    // Must be called once at process start (rustls 0.23 requirement).
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("rustls ring crypto provider");

    // Register compiled feature flags so they're visible in capabilities queries.
    zero_engine::register_build_features(collect_build_features());

    application::execute(cli::parse_args(env::args())?).await
}

/// Early-parse the configuration file to extract `runtime.log` and
/// initialise the tracing subscriber before any meaningful work.
fn init_tracing_from_config(config_path: &str) -> zero_logging::TracingGuard {
    let log_config = std::fs::read_to_string(config_path)
        .ok()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
        .and_then(|v| v.get("runtime")?.get("log").cloned())
        .and_then(|v| serde_json::from_value::<zero_config::LogConfig>(v).ok())
        .unwrap_or_else(|| {
            // Fallback: stderr only, respect RUST_LOG or default to info.
            zero_config::LogConfig::default()
        });

    zero_logging::init_tracing(&log_config)
}

/// Collect compiled feature flags for the capabilities endpoint.
fn collect_build_features() -> Vec<String> {
    let mut features = Vec::new();
    if cfg!(feature = "status-api") {
        features.push("status-api".to_owned());
    }
    if cfg!(feature = "event-dispatcher") {
        features.push("event-dispatcher".to_owned());
    }
    if cfg!(feature = "sink-jsonl") {
        features.push("sink-jsonl".to_owned());
    }
    if cfg!(feature = "connector") {
        features.push("connector".to_owned());
    }
    if cfg!(feature = "grpc-api") {
        features.push("grpc-api".to_owned());
    }
    features.extend(zero_proxy::compiled_protocol_features());
    if cfg!(feature = "dns") {
        features.push("dns".to_owned());
    }
    features
}
