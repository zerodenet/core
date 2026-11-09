use super::resolve_socket;
use crate::{
    cli::Command,
    ipc::{self, client::Caller},
};
use std::error::Error;
use zero_api::{FlowListQuery, PoliciesQuery, QueryRequest};

pub async fn execute(command: Command, client: &Caller) -> Result<(), Box<dyn Error>> {
    match command {
        Command::Select {
            policy_tag,
            target_tag,
            socket_path,
        } => {
            #[derive(serde::Serialize)]
            struct Params<'a> {
                policy_tag: &'a str,
                target_tag: &'a str,
            }
            send_command(
                client,
                socket_path.as_deref(),
                "policies.select",
                Params {
                    policy_tag: &policy_tag,
                    target_tag: &target_tag,
                },
            )
            .await?
            .ensure_ok("unknown IPC error")?;
            println!("selector `{policy_tag}` -> `{target_tag}`");
            Ok(())
        }
        Command::Flows { socket_path } => {
            print_query(
                client,
                socket_path.as_deref(),
                QueryRequest::ActiveFlows(FlowListQuery {
                    limit: Some(100),
                    filter: Default::default(),
                }),
            )
            .await
        }
        Command::Policies { socket_path } => {
            print_query(
                client,
                socket_path.as_deref(),
                QueryRequest::Policies(PoliciesQuery),
            )
            .await
        }
        Command::Events { socket_path } => {
            let socket = resolve_socket(socket_path.as_deref())?;
            client
                .events(
                    &socket,
                    &ipc::protocol::IpcRequest::Subscribe {
                        id: None,
                        events: None,
                    },
                )
                .await?;
            Ok(())
        }
        Command::Reload {
            config_path,
            socket_path,
        } => {
            let raw = zero_runtime::configuration::ConfigurationSource::read_initial_document(
                &config_path,
            )?;
            let config = client.raw_json(&raw)?;
            #[derive(serde::Serialize)]
            struct Params<'a> {
                config: &'a crate::ipc::client::RawInput<'a>,
            }
            send_command(
                client,
                socket_path.as_deref(),
                "config.apply",
                Params { config: &config },
            )
            .await?
            .ensure_ok("unknown IPC error")?;
            println!("config applied");
            Ok(())
        }
        Command::Mode {
            mode,
            outbound,
            socket_path,
        } => {
            #[derive(serde::Serialize)]
            struct Params<'a> {
                mode: &'a str,
                outbound: Option<&'a str>,
            }
            send_command(
                client,
                socket_path.as_deref(),
                "mode.set",
                Params {
                    mode: &mode,
                    outbound: outbound.as_deref(),
                },
            )
            .await?
            .ensure_ok("unknown IPC error")?;
            let target = outbound.map_or(mode, |tag| format!("global -> {tag}"));
            println!("mode switched to {target}");
            Ok(())
        }
        _ => unreachable!("application routes only control commands here"),
    }
}
async fn print_query(
    client: &Caller,
    socket_path: Option<&str>,
    request: QueryRequest,
) -> Result<(), Box<dyn Error>> {
    let socket = resolve_socket(socket_path)?;
    client
        .request(
            &socket,
            &ipc::protocol::IpcRequest::Query { id: None, request },
        )
        .await?
        .ensure_ok("unknown IPC error")?
        .print(true, true)?;
    Ok(())
}
async fn send_command(
    client: &Caller,
    socket_path: Option<&str>,
    method: &str,
    params: impl serde::Serialize,
) -> Result<ipc::client::Reply, Box<dyn Error>> {
    let socket = resolve_socket(socket_path)?;
    Ok(client
        .request(&socket, &ipc::client::Command::new(method, params))
        .await?)
}
