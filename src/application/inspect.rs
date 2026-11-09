use std::error::Error;

use zero_api::QueryRequest;

use super::resolve_socket;
use crate::cli::Command;

pub async fn execute(
    command: Command,
    client: Option<&crate::ipc::client::Caller>,
) -> Result<(), Box<dyn Error>> {
    match command {
        Command::Status {
            config_path,
            json,
            socket_path,
        } => status(config_path.as_deref(), json, socket_path.as_deref(), client).await,
        Command::Validate { config_path } => {
            let source =
                std::sync::Arc::new(zero_runtime::configuration::ConfigurationSource::default());
            let config = source.load_initial_configuration(&config_path)?;
            let prepared = zero_runtime::configuration::PreparedConfiguration::prepare_startup(
                config, source,
            )?;
            zero_proxy::validate_prepared_configuration(&prepared)?;
            let config = &prepared.config;
            println!(
                "config valid: {} inbounds, {} outbounds, {} groups, {} rules",
                config.inbounds.len(),
                config.outbounds.len(),
                config.outbound_groups.len(),
                config.route.rules.len(),
            );
            Ok(())
        }
        Command::ConnectorState { config_path, json } => connector_state(&config_path, json),
        Command::BuildInfo => {
            println!("build_id: {}", env!("CARGO_PKG_VERSION"));
            println!("build_time: {}", env!("ZERO_BUILD_TIME"));
            println!("build_profile: {}", env!("ZERO_BUILD_PROFILE"));
            println!("features: {}", crate::collect_build_features().join(","));
            println!(
                "protocol_capabilities: {}",
                serde_json::to_string(
                    &zero_proxy::ProtocolInventory::default().protocol_capabilities()
                )?
            );
            println!(
                "binary_sha256: {}",
                crate::artifact::current_executable_sha256()?
            );
            if let Some(hash) = option_env!("ZERO_GIT_DESCRIBE").or(option_env!("ZERO_GIT_HASH")) {
                println!("git: {hash}");
            }
            if let Some(hash) = option_env!("ZERO_GIT_HASH") {
                println!("git_hash: {hash}");
            }
            Ok(())
        }
        Command::Help => {
            println!("{}", crate::cli::usage());
            Ok(())
        }
        _ => unreachable!("application routes only inspect commands here"),
    }
}

#[cfg(feature = "event-dispatcher")]
mod state;

#[cfg(feature = "event-dispatcher")]
fn connector_state(config_path: &str, json: bool) -> Result<(), Box<dyn Error>> {
    #[derive(serde::Serialize)]
    struct UpgradeStateReport<'a> {
        schema_id: &'static str,
        compatible: bool,
        connector_files: &'a [zero_connector::ConnectorStateFile],
        #[serde(skip_serializing_if = "Option::is_none")]
        principal_quota: Option<&'a zero_engine::PrincipalQuotaStateReport>,
    }

    let source = std::sync::Arc::new(zero_runtime::configuration::ConfigurationSource::default());
    let config = source.load_initial_configuration(config_path)?;
    let (connector, mut inspection_failures) =
        zero_connector::inspect_persistent_state_with_source(&config, &source).into_parts();
    let principal_quota = zero_engine::inspect_principal_quota_state_with_source(&config, &source);
    let source_closed = match source.confirm_original_io_closed() {
        Ok(()) => true,
        Err(original) => {
            inspection_failures.push(std::sync::Arc::new(original));
            false
        }
    };
    let compatible = source_closed
        && connector.compatible
        && principal_quota
            .as_ref()
            .is_none_or(zero_engine::PrincipalQuotaStateReport::is_compatible);
    let report = UpgradeStateReport {
        schema_id: "zero.connector.upgrade-state-report.v1",
        compatible,
        connector_files: &connector.files,
        principal_quota: principal_quota.as_ref(),
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!(
            "Connector state: {}",
            if compatible {
                "compatible"
            } else {
                "incompatible"
            }
        );
        for file in &connector.files {
            let status = match file.status {
                zero_connector::ConnectorStateStatus::Missing => "missing",
                zero_connector::ConnectorStateStatus::Ready => "ready",
                zero_connector::ConnectorStateStatus::RecoverablePartialTail => {
                    "recoverable_partial_tail"
                }
                zero_connector::ConnectorStateStatus::Incompatible => "incompatible",
            };
            println!(
                "  {}: {} format={} bytes={} path={}",
                file.kind, status, file.format, file.bytes, file.path
            );
            if let Some(pending) = file.pending {
                println!("    pending: {pending}");
            }
            if let Some(records) = file.records {
                println!("    records: {records}");
            }
            if let Some(error) = file.error.as_deref() {
                println!("    error: {error}");
            }
        }
        if let Some(quota) = principal_quota.as_ref() {
            let status = match quota.status {
                zero_engine::PrincipalQuotaStateStatus::Missing => "missing",
                zero_engine::PrincipalQuotaStateStatus::Ready => "ready",
                zero_engine::PrincipalQuotaStateStatus::Incompatible => "incompatible",
            };
            println!(
                "  principal_quota: {} format={} bytes={} path={}",
                status, quota.format, quota.bytes, quota.path
            );
            if let Some(balances) = quota.balances {
                println!("    balances: {balances}");
            }
            if let Some(error) = quota.error.as_deref() {
                println!("    error: {error}");
            }
        }
    }
    if !compatible {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            state::InspectionFailure(inspection_failures),
        )
        .into());
    }
    Ok(())
}

#[cfg(not(feature = "event-dispatcher"))]
fn connector_state(_config_path: &str, _json: bool) -> Result<(), Box<dyn Error>> {
    Err(std::io::Error::other(
        "connector state inspection requires a build with the `event-dispatcher` feature",
    )
    .into())
}

async fn status(
    config_path: Option<&str>,
    json: bool,
    socket_path: Option<&str>,
    client: Option<&crate::ipc::client::Caller>,
) -> Result<(), Box<dyn Error>> {
    if config_path.is_none() {
        if let Ok(socket) = resolve_socket(socket_path) {
            let response = client
                .expect("actual status client installed")
                .request(
                    &socket,
                    &crate::ipc::protocol::IpcRequest::Query {
                        id: None,
                        request: QueryRequest::Runtime(Default::default()),
                    },
                )
                .await?;
            response.print_status(&socket, json)?;
            return Ok(());
        }
    }

    let path = config_path
        .ok_or_else(|| std::io::Error::other("no config path provided and no socket available"))?;
    let status = zero_proxy::Proxy::from_path(path)?.export_status();
    if json {
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        print!("{}", crate::output::render_status(&status));
    }
    Ok(())
}
