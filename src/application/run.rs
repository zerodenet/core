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

#[cfg(test)]
mod tests;

pub async fn execute(command: Command) -> Result<(), Box<dyn Error>> {
    let Command::Run {
        config_path,
        status_listen,
        control_socket,
        ipc_hook_socket,
    } = command
    else {
        unreachable!("application routes only run commands here")
    };
    run(
        &config_path,
        status_listen.as_deref(),
        control_socket.as_deref(),
        ipc_hook_socket.as_deref(),
    )
    .await
}

async fn run(
    config_path: &str,
    status_listen: Option<&str>,
    control_socket: Option<&str>,
    ipc_hook_socket: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    let mut config = zero_config::RuntimeConfig::load_from_path(config_path)?;
    rule_set_fetch::pre_fetch_rule_sets(&mut config.route.rule_sets, config.source_dir.as_deref());
    let engine = zero_engine::Engine::new_with_config_path(config, config_path)?;
    let proxy = Proxy::from_engine(engine)?;
    let engine = proxy.engine().clone();

    let engine_handle = EngineHandle::new(engine.clone());
    let base_handle = ProxyHandle::new(engine_handle.clone(), proxy.clone());
    let services = ApplicationServices::start(engine.clone(), ipc_hook_socket).await?;
    let ipc_handle = base_handle.with_config_apply_reconciler(services.clone());

    // Bridge tracing warn/error ->?engine.warning events.
    {
        let e = engine.clone();
        zero_logging::set_warning_sink(move |code: &str, msg: &str| {
            e.emit_warning(code, msg);
        });
    }

    #[cfg(not(any(feature = "status-api", feature = "grpc-api")))]
    ensure_status_api_not_configured(&engine, status_listen)?;

    tracing::info!(config = %config_path, "loaded proxy configuration");

    // IPC server always starts (not feature-gated).
    let ipc_socket_path = ipc::resolve_ipc_path(control_socket)?;
    let ipc_server = ipc::spawn_ipc_server(ipc_handle.clone(), &ipc_socket_path).await?;

    #[cfg(any(feature = "status-api", feature = "grpc-api"))]
    let status_spec = status_server_spec(&engine, status_listen)?;

    #[cfg(feature = "status-api")]
    let http_server = {
        if let Some(ref status) = status_spec {
            Some(
                http_adapter::spawn_http_server(
                    ipc_handle.clone(),
                    &status.listen,
                    status.auth.clone(),
                )
                .await?,
            )
        } else {
            None
        }
    };

    #[cfg(feature = "grpc-api")]
    let grpc_server = {
        if let Some(ref status) = status_spec {
            let addr: std::net::SocketAddr = status
                .grpc_listen
                .parse()
                .map_err(|e| std::io::Error::other(format!("gRPC listen address: {e}")))?;
            Some(zero_grpc::spawn(ipc_handle.clone(), addr, status.grpc_security.clone()).await?)
        } else {
            None
        }
    };

    let stats_sampler = spawn_stats_sampler(engine.clone());

    // The proxy data plane is a critical application service. Run it under the
    // root application lifecycle instead of detaching it into an unsupervised
    // task. If listener orchestration exits unexpectedly, surface the error
    // immediately so the process cannot remain control-plane alive while its
    // configured inbound ports have already disappeared.
    let proxy_result = run_supervised_proxy(&proxy, wait_for_shutdown_signal()).await;
    if let Err(error) = &proxy_result {
        tracing::error!(
            core_instance_id = engine.core_instance_id(),
            config_revision = engine.config_revision(),
            reason = "runtime_error",
            error = %error,
            "proxy runtime terminated unexpectedly"
        );
    }

    stats_sampler.abort();
    // Proxy shutdown emits terminal flow.completed facts. Stop only status
    // polling here; the dispatcher remains alive for the terminal events.
    services.shutdown_status_monitor().await;

    engine.push_engine_stopped(proxy_stop_reason(&proxy_result));
    // Allow the event dispatcher to observe the terminal engine event before
    // its final drain persists any remaining deliveries to the outbox.
    tokio::task::yield_now().await;

    services.shutdown_dispatcher().await;
    ipc_server.shutdown().await?;
    #[cfg(feature = "status-api")]
    if let Some(s) = http_server {
        s.shutdown().await?;
    }
    #[cfg(feature = "grpc-api")]
    if let Some(s) = grpc_server {
        s.shutdown().await;
    }

    proxy_result?;
    Ok(())
}

async fn run_supervised_proxy<F>(proxy: &Proxy, shutdown: F) -> Result<(), zero_engine::EngineError>
where
    F: Future<Output = ()> + Send,
{
    proxy.run_until(shutdown).await
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
    let grpc_security = grpc_server_security(control, engine.config().source_dir())?;

    let control_listen = format!("{}:{}", listen.address, listen.port);
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
        .map(|tls| load_grpc_tls(tls, source_dir))
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
) -> Result<zero_grpc::GrpcServerTls, Box<dyn Error>> {
    let cert_path = resolve_config_path(&config.cert_path, source_dir);
    let key_path = resolve_config_path(&config.key_path, source_dir);
    let cert_pem = std::fs::read(&cert_path).map_err(|error| {
        std::io::Error::other(format!(
            "read gRPC TLS certificate `{}`: {error}",
            cert_path.display()
        ))
    })?;
    let key_pem = std::fs::read(&key_path).map_err(|error| {
        std::io::Error::other(format!(
            "read gRPC TLS private key `{}`: {error}",
            key_path.display()
        ))
    })?;
    let mut tls = zero_grpc::GrpcServerTls::new(cert_pem, key_pem);
    if let Some(path) = &config.client_ca_cert_path {
        let path = resolve_config_path(path, source_dir);
        let client_ca = std::fs::read(&path).map_err(|error| {
            std::io::Error::other(format!(
                "read gRPC mTLS client CA `{}`: {error}",
                path.display()
            ))
        })?;
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

async fn wait_for_shutdown_signal() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => tracing::info!("shutdown signal received"),
        Err(error) => {
            tracing::warn!(error = %error, "failed to listen for ctrl-c; stopping proxy")
        }
    }
}

fn spawn_stats_sampler(engine: Engine) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut stats_tick = tokio::time::interval(std::time::Duration::from_secs(1));
        let mut flow_tick = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = stats_tick.tick() => engine.push_stats_sampled(),
                _ = flow_tick.tick() => engine.push_flow_updates(),
            }
        }
    })
}

// ── IPC client commands ───────────────────────────────────────────────
