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
mod logging_owner;
mod output;
mod rule_set_fetch;

#[tokio::main]
async fn main() {
    // Parse CLI args to find the config path, then initialise tracing
    // from `runtime.log` before any other work so all logs are captured.
    let args: Vec<String> = env::args().collect();
    let config_path = cli::config_path_from_args(&args);
    let mut logging = init_tracing_from_config(config_path.unwrap_or(""))
        .expect("failed to prepare original logging owner");
    install_panic_hook();
    let initialization = logging.initialize().await;
    let result = match initialization {
        Ok(()) => {
            let handle = logging.handle();
            use futures_util::FutureExt;
            logging
                .run(std::panic::AssertUnwindSafe(try_main(handle)).catch_unwind())
                .await
        }
        Err(source) => {
            eprintln!("failed to initialize tracing: {source}");
            // The actual logger source remains in its original paid receiver.
            let _ = logging.shutdown().await;
            process::exit(101);
        }
    };
    let code = match &result {
        Ok(Ok(())) => 0,
        Ok(Err(error)) => {
            tracing::error!(error = %error, "zero process terminating with fatal error");
            error_report::print_error(error.as_ref());
            1
        }
        Err(_) => 101, // the original panic hook already emitted its message
    };
    let complete = logging.shutdown().await;
    if complete {
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
        if code == 0 {
            return;
        }
    }
    // Keep the original application outcome/panic AND unknown logging owner
    // alive through exit; Runtime Drop must not wait forever for a stuck write.
    // This is process termination, never a native/Scope closure assertion.
    process::exit(code);
}

fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic| {
        tracing::error!(panic = %panic, "zero process panicked");
        eprintln!("fatal panic: {panic}");
        previous(panic);
    }));
}

async fn try_main(logging: zero_logging::LoggingHandle) -> Result<(), Box<dyn Error>> {
    // Install rustls crypto provider before any TLS operation.
    // Must be called once at process start (rustls 0.23 requirement).
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("rustls ring crypto provider");

    // Register compiled feature flags so they're visible in capabilities queries.
    zero_engine::register_build_features(collect_build_features());

    application::execute_with_logging(cli::parse_args(env::args())?, Some(logging)).await
}

/// Early-parse the configuration file to extract `runtime.log` and
/// initialise the tracing subscriber before any meaningful work.
fn init_tracing_from_config(config_path: &str) -> std::io::Result<logging_owner::ProcessLogging> {
    let log_config =
        zero_runtime::configuration::ConfigurationSource::read_initial_log_config(config_path)
            .ok()
            .flatten()
            .unwrap_or_default();
    logging_owner::ProcessLogging::prepare(&log_config)
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
    if cfg!(feature = "host-network-stats") {
        features.push("host-network-stats".to_owned());
    }
    features.extend(zero_proxy::compiled_protocol_features());
    if cfg!(feature = "dns") {
        features.push("dns".to_owned());
    }
    features
}
