use std::error::Error;
use std::future::Future;

use crate::cli::Command;
#[cfg(any(feature = "status-api", feature = "grpc-api"))]
use std::env;
use zero_engine::{Engine, EngineHandle};
use zero_proxy::{Proxy, ProxyHandle};

use super::services::ApplicationServices;
#[cfg(feature = "status-api")]
use crate::http_adapter;
use crate::{ipc, rule_set_fetch};

#[cfg(feature="grpc-api")]
mod control_material;
mod control_failure;
use control_failure::RunFailures;

#[cfg(test)]
mod tests;

pub async fn execute(command: Command) -> Result<(), Box<dyn Error>> {
    let Command::Run {
        config_path,
        status_listen,
        control_socket,
        ipc_hook_socket,
        parent_lifetime_stdin,
    } = command
    else {
        unreachable!("application routes only run commands here")
    };
    Box::pin(run(
        &config_path,
        status_listen.as_deref(),
        control_socket.as_deref(),
        ipc_hook_socket.as_deref(),
        parent_lifetime_stdin,
    ))
    .await
}

async fn run(
    config_path: &str,
    status_listen: Option<&str>,
    control_socket: Option<&str>,
    ipc_hook_socket: Option<&str>,
    parent_lifetime_stdin: bool,
) -> Result<(), Box<dyn Error>> {
    let source = std::sync::Arc::new(zero_runtime::configuration::ConfigurationSource::new(
        config_path,
    ));
    let config = rule_set_fetch::load_initial_configuration(source.clone(), config_path).await?;
    let prepared = zero_runtime::configuration::PreparedConfiguration::prepare_startup(
        config,
        source.clone(),
    )?;
    let engine = zero_engine::Engine::from_prepared(prepared)?;
    source.complete_recovered_boot()?;
    let proxy = Proxy::from_engine(engine)?;
    let engine = proxy.engine().clone();

    let engine_handle = EngineHandle::new(engine.clone());
    let base_handle = ProxyHandle::new(engine_handle.clone(), proxy.clone());
    let services = ApplicationServices::start_with_proxy(&proxy, ipc_hook_socket).await?;
    let ipc_handle = base_handle.with_config_apply_reconciler(services.clone());

    // Bridge tracing warn/error ->?engine.warning events.
    {
        let e = engine.clone();
        zero_logging::set_warning_sink(move |code: &str, msg: &str| {
            e.emit_warning(code, msg);
        });
    }

    tracing::info!(config = %config_path, "loaded proxy configuration");

    // Install the same application before cold control preparation. A cancelled
    // initializer leaves the exact listener/scope owner in its existing slot.
    services.retain_control_startup()?;
    let parent_input = if parent_lifetime_stdin {Some(services.install_parent_input()?)} else {None};


    let control_start = async {
        #[cfg(not(any(feature = "status-api", feature = "grpc-api")))]
        ensure_status_api_not_configured(&engine, status_listen)?;
        #[cfg(any(feature = "status-api", feature = "grpc-api"))]
        let status_spec = status_server_spec(&engine, status_listen)?;
        let ipc_socket_path = ipc::resolve_ipc_path(control_socket)?;
        let ipc_server =
            ipc::prepare_ipc_server_with_budget(&ipc_socket_path, services.control_budget());
        services.install_ipc(ipc_server.clone())?;
        ipc_server.start(ipc_handle.clone()).await?;
        #[cfg(feature = "status-api")]
        if let Some(ref status) = status_spec {
            let http = http_adapter::prepare_http_server_with_budget(
                &status.listen,
                status.auth.clone(),
                services.control_budget(),
            );
            services.install_http(http.clone())?;
            http.start(ipc_handle.clone()).await?;
        }
        #[cfg(feature = "grpc-api")]
        if let Some(ref status) = status_spec {
            let addr: std::net::SocketAddr = status
                .grpc_listen
                .parse()
                .map_err(|error| std::io::Error::other(format!("gRPC listen address: {error}")))?;
            let grpc = zero_grpc::prepare_with_budget(
                addr,
                status.grpc_security.clone(),
                services.control_budget(),
            )?;
            services.install_grpc(grpc.clone())?;
            grpc.start(ipc_handle.clone()).await?;
        }
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    if let Err(error) = control_start {
        let controls = services.shutdown_control().await;
        let monitor = services.shutdown_status_monitor().await;
        let dispatcher = services.shutdown_dispatcher().await;
        let quota = engine.shutdown_principal_quota_state();
        let parent = services.shutdown_parent_input().await;
        if services.control_retirement_complete()
            && monitor.is_ok()
            && dispatcher.is_ok()
            && quota.is_ok()
            && services.parent_input_retirement_complete()
        {
            services.finish_control_startup();
        } else {
            tracing::error!(
                ?controls,
                ?monitor,
                ?dispatcher,
                ?quota,
                "original control startup cleanup remains retained"
            );
        }
        let mut failures = RunFailures::default();
        failures.error("control startup", error);
        failures.result("control cleanup", controls);
        failures.result("status monitor cleanup", monitor);
        failures.result("event dispatcher cleanup", dispatcher);
        failures.result("quota cleanup", quota);
        failures.result("parent input cleanup", parent);
        return failures.finish();
    }
    if parent_input.is_none() {services.finish_control_startup();}

    // The proxy data plane is a critical application service. Run it under the
    // root application lifecycle instead of detaching it into an unsupervised
    // task. If listener orchestration exits unexpectedly, surface the error
    // immediately so the process cannot remain control-plane alive while its
    // configured inbound ports have already disappeared.
    let control_exit = std::sync::Mutex::new(None::<std::io::Result<()>>);
    let proxy_result = Box::pin(run_supervised_proxy(
        &proxy,
        async {
            tokio::select! {
                _ = wait_for_shutdown_signal(parent_input.as_deref()) => {},
                result = services.wait_control_terminated() => {
                    *control_exit.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result.and_then(|_| Err(std::io::Error::other("original control server terminated unexpectedly"))));
                }
            }
        },
    ))
    .await;
    if let Err(error) = &proxy_result {
        tracing::error!(
            core_instance_id = engine.core_instance_id(),
            config_revision = engine.config_revision(),
            reason = "runtime_error",
            error = %error,
            "proxy runtime terminated unexpectedly"
        );
    }

    // Proxy shutdown emits terminal flow.completed facts. Stop only status
    // polling here; the dispatcher remains alive for the terminal events.
    let parent_shutdown = services.shutdown_parent_input().await;
    let monitor_shutdown = services.shutdown_status_monitor().await;

    engine.push_engine_stopped(proxy_stop_reason(&proxy_result));
    // Allow the event dispatcher to observe the terminal engine event before
    // its final drain persists any remaining deliveries to the outbox.
    tokio::task::yield_now().await;

    let dispatcher_shutdown = services.shutdown_dispatcher().await;
    let control_shutdown = services.shutdown_control().await;
    let quota_shutdown = engine.shutdown_principal_quota_state();

    if services.parent_input_retirement_complete() && services.control_retirement_complete()
        && monitor_shutdown.is_ok() && dispatcher_shutdown.is_ok() && quota_shutdown.is_ok() {
        services.finish_control_startup();
    }

    // Every original cleanup above ran, even when another service failed.
    // Keep their original typed sources instead of returning only the first.
    let mut failures = RunFailures::default();
    failures.result("proxy", proxy_result);
    failures.result("parent input cleanup", parent_shutdown);
    failures.result("control cleanup", control_shutdown);
    failures.result("quota cleanup", quota_shutdown);
    if let Some(result) = control_exit
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
    {
        failures.result("control server", result);
    }
    failures.result("status monitor cleanup", monitor_shutdown);
    failures.result("event dispatcher cleanup", dispatcher_shutdown);
    failures.finish()
}

async fn run_supervised_proxy<F>(proxy: &Proxy, shutdown: F) -> Result<(), zero_engine::EngineError>
where
    F: Future<Output = ()> + Send,
{
    Box::pin(proxy.run_until(shutdown)).await
}

fn proxy_stop_reason(result: &Result<(), zero_engine::EngineError>) -> &'static str {
    if result.is_ok() {
        "signal"
    } else {
        "runtime_error"
    }
}

#[cfg(any(feature = "status-api", feature = "grpc-api"))]
struct StatusServerSpec {
    #[cfg(feature = "status-api")]
    listen: String,
    #[cfg(feature = "grpc-api")]
    grpc_listen: String,
    #[cfg(feature = "status-api")]
    auth: Option<http_adapter::HttpServerAuth>,
    #[cfg(feature = "grpc-api")]
    grpc_security: zero_grpc::GrpcServerSecurity,
}

#[cfg(any(feature = "status-api", feature = "grpc-api"))]
fn status_server_spec(
    engine: &Engine,
    cli_listen: Option<&str>,
) -> Result<Option<StatusServerSpec>, Box<dyn Error>> {
    let control = &engine.config().api.control;

    if cli_listen.is_some() && control.enabled {
        return Err(std::io::Error::other(
            "use either `--status-listen` or `api.control`, not both",
        )
        .into());
    }

    if let Some(listen) = cli_listen {
        return Ok(Some(StatusServerSpec {
            #[cfg(feature = "status-api")]
            listen: listen.to_owned(),
            #[cfg(feature = "grpc-api")]
            grpc_listen: next_port(listen)?,
            #[cfg(feature = "status-api")]
            auth: None,
            #[cfg(feature = "grpc-api")]
            grpc_security: zero_grpc::GrpcServerSecurity::default(),
        }));
    }

    if !control.enabled {
        return Ok(None);
    }

    let listen = control
        .listen
        .as_ref()
        .expect("config validation requires api.control.listen");

    #[cfg(feature = "status-api")]
    let auth = {
        let key = config_api_key(control.api_key.as_ref(), control.api_key_env.as_ref())?;
        Some(http_adapter::HttpServerAuth::single_admin(key))
    };
    #[cfg(feature = "grpc-api")]
    let grpc_security = grpc_server_security(
        control,
        engine.config().source_dir(),
        &engine.configuration_source_owner(),
        engine.config().runtime.effective_file_read_max_bytes(),
    )?;

    let control_listen = zero_core::address::format_socket_addr(&listen.address, listen.port);
    Ok(Some(StatusServerSpec {
        #[cfg(feature = "status-api")]
        listen: control_listen.clone(),
        #[cfg(feature = "grpc-api")]
        grpc_listen: next_port(&control_listen)?,
        #[cfg(feature = "status-api")]
        auth,
        #[cfg(feature = "grpc-api")]
        grpc_security,
    }))
}

#[cfg(feature = "grpc-api")]
fn grpc_server_security(
    control: &zero_config::ControlApiConfig,
    source_dir: Option<&std::path::Path>,
    source: &std::sync::Arc<zero_runtime::configuration::ConfigurationSource>,
    max_bytes: usize,
) -> Result<zero_grpc::GrpcServerSecurity, Box<dyn Error>> {
    let grpc = control.grpc.as_ref().cloned().unwrap_or_default();
    let auth = if grpc.bearer_auth {
        let key = config_api_key(control.api_key.as_ref(), control.api_key_env.as_ref())?;
        Some(zero_grpc::GrpcServerAuth::single_admin(key))
    } else {
        None
    };
    let tls = grpc
        .tls
        .as_ref()
        .map(|tls| load_grpc_tls(tls, source_dir, source, max_bytes))
        .transpose()?;
    Ok(zero_grpc::GrpcServerSecurity {
        auth,
        tls,
        allow_insecure_remote: grpc.allow_insecure_remote,
    })
}

#[cfg(feature = "grpc-api")]
fn load_grpc_tls(
    config: &zero_config::ControlGrpcTlsConfig,
    source_dir: Option<&std::path::Path>,
    source: &std::sync::Arc<zero_runtime::configuration::ConfigurationSource>,
    max_bytes: usize,
) -> Result<zero_grpc::GrpcServerTls, Box<dyn Error>> {
    let cert_path = resolve_config_path(&config.cert_path, source_dir);
    let key_path = resolve_config_path(&config.key_path, source_dir);
    let cert_pem = control_material::read(source,&cert_path,"TLS certificate",max_bytes)?;
    let key_pem = control_material::read(source,&key_path,"TLS private key",max_bytes)?;
    let mut tls = zero_grpc::GrpcServerTls::new(cert_pem, key_pem);
    if let Some(path) = &config.client_ca_cert_path {
        let path = resolve_config_path(path, source_dir);
        let client_ca = control_material::read(source,&path,"mTLS client CA",max_bytes)?;
        tls = tls.with_client_ca(client_ca);
    }
    Ok(tls)
}

#[cfg(feature = "grpc-api")]
fn resolve_config_path(path: &str, source_dir: Option<&std::path::Path>) -> std::path::PathBuf {
    let path = std::path::PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        source_dir.map_or(path.clone(), |source_dir| source_dir.join(path))
    }
}

#[cfg(feature = "grpc-api")]
fn next_port(listen: &str) -> Result<String, std::io::Error> {
    let mut address = listen.parse::<std::net::SocketAddr>().map_err(|error| {
        std::io::Error::other(format!(
            "control listen address `{listen}` is invalid: {error}"
        ))
    })?;
    let port = address.port().checked_add(1).ok_or_else(|| {
        std::io::Error::other("gRPC companion port is unavailable after control port 65535")
    })?;
    address.set_port(port);
    Ok(address.to_string())
}

#[cfg(not(any(feature = "status-api", feature = "grpc-api")))]
fn ensure_status_api_not_configured(
    engine: &Engine,
    cli_listen: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    if let Some(status_listen) = cli_listen {
        return Err(std::io::Error::other(format!(
            "`--status-listen {status_listen}` requires Cargo feature `status-api`"
        ))
        .into());
    }

    if engine.config().api.control.enabled {
        return Err(std::io::Error::other(
            "`api.control.enabled` requires Cargo feature `status-api`",
        )
        .into());
    }

    Ok(())
}

#[cfg(any(feature = "status-api", feature = "grpc-api"))]
fn config_api_key(
    api_key: Option<&String>,
    api_key_env: Option<&String>,
) -> Result<String, Box<dyn Error>> {
    if let Some(key) = api_key {
        return Ok(key.clone());
    }

    let name = api_key_env.expect("config validation requires api_key or api_key_env");
    let value = env::var(name)?;
    if value.trim().is_empty() {
        return Err(std::io::Error::other(format!(
            "api key environment variable `{name}` must not be empty"
        ))
        .into());
    }
    Ok(value)
}

async fn wait_for_shutdown_signal(parent_input: Option<&zero_platform_tokio::parent_input::ParentInputOwner>) {
    let Some(parent_input) = parent_input else {
        log_ctrl_c_result(tokio::signal::ctrl_c().await);
        return;
    };

    tokio::select! {
        result = tokio::signal::ctrl_c() => log_ctrl_c_result(result),
        result = async { parent_input.start()?; parent_input.wait().await } => match result {
            Ok(()) => tracing::info!(reason = "parent_lifetime_closed", "managed parent exited; stopping proxy"),
            Err(error) => tracing::warn!(error = %error, reason = "parent_lifetime_error", "managed parent lifetime channel failed; stopping proxy"),
        },
    }
}

fn log_ctrl_c_result(result: std::io::Result<()>) {
    match result {
        Ok(()) => tracing::info!("shutdown signal received"),
        Err(error) => tracing::warn!(error = %error, "failed to listen for ctrl-c; stopping proxy"),
    }
}

// ── IPC client commands ───────────────────────────────────────────────
