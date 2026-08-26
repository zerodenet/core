use std::error::Error;
use std::fmt;

/// Extract the config file path from raw CLI arguments for early
/// initialisation of the tracing subscriber. Returns `None` for
/// commands that don't need a config file.
pub fn config_path_from_args(args: &[String]) -> Option<&str> {
    let mut iter = args.iter().skip(1);
    let first = iter.next()?;
    match first.as_str() {
        "run" | "validate" | "reload" => {
            for arg in iter {
                if arg == "--status-listen"
                    || arg == "--control-socket"
                    || arg == "--ipc-hook-socket"
                    || arg == "--socket"
                    || arg == "--json"
                {
                    continue;
                }
                if arg.starts_with('-') {
                    continue;
                }
                return Some(arg);
            }
            None
        }
        "status" => {
            while let Some(arg) = iter.next() {
                if arg == "--socket" {
                    iter.next();
                    continue;
                }
                if arg.starts_with('-') {
                    continue;
                }
                return Some(arg);
            }
            None
        }
        "connector" => match iter.next().map(String::as_str) {
            Some("state") => iter.find(|arg| !arg.starts_with('-')).map(String::as_str),
            _ => None,
        },
        // Commands that talk to a running daemon via IPC — no config needed.
        "select" | "flows" | "policies" | "events" | "build_info" | "build-info" | "version"
        | "mode" | "help" | "--help" | "-h" | "--version" | "-V" => None,
        _ if first.starts_with('-') => None,
        _ => Some(first),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Run {
        config_path: String,
        status_listen: Option<String>,
        control_socket: Option<String>,
        ipc_hook_socket: Option<String>,
    },
    Status {
        config_path: Option<String>,
        json: bool,
        socket_path: Option<String>,
    },
    Select {
        policy_tag: String,
        target_tag: String,
        socket_path: Option<String>,
    },
    Flows {
        socket_path: Option<String>,
    },
    Policies {
        socket_path: Option<String>,
    },
    Events {
        socket_path: Option<String>,
    },
    Reload {
        config_path: String,
        socket_path: Option<String>,
    },
    Validate {
        config_path: String,
    },
    ConnectorState {
        config_path: String,
        json: bool,
    },
    Mode {
        mode: String,
        outbound: Option<String>,
        socket_path: Option<String>,
    },
    TunStart {
        name: Option<String>,
        addr: String,
        mask: Option<String>,
        secondary_addr: Option<String>,
        mtu: Option<u16>,
        tag: String,
        auto_route: bool,
        include_cidrs: Vec<String>,
        exclude_cidrs: Vec<String>,
        dual_stack: bool,
        strict_route: bool,
        dns_hijack: bool,
        socket_path: Option<String>,
    },
    TunStop {
        socket_path: Option<String>,
    },
    TunStatus {
        socket_path: Option<String>,
    },
    BuildInfo,
    Help,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliError {
    message: String,
}

impl CliError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for CliError {}

pub fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Command, CliError> {
    let mut args = args.into_iter();
    let _ = args.next(); // skip program name

    let Some(first) = args.next() else {
        return Err(CliError::new("no command specified".to_owned()));
    };
    if first == "--help" || first == "-h" {
        return Ok(Command::Help);
    }

    match first.as_str() {
        "run" => parse_run(args.collect()),
        "status" => parse_status(args.collect()),
        "select" => parse_select(args.collect()),
        "flows" => {
            parse_client_command(args.collect(), |socket_path| Command::Flows { socket_path })
        }
        "policies" => parse_client_command(args.collect(), |socket_path| Command::Policies {
            socket_path,
        }),
        "events" => parse_client_command(args.collect(), |socket_path| Command::Events {
            socket_path,
        }),
        "reload" => parse_reload(args.collect()),
        "validate" => parse_validate(args.collect()),
        "connector" => parse_connector(args.collect()),
        "mode" => parse_mode(args.collect()),
        "tun" => {
            let remaining: Vec<String> = args.collect();
            match remaining.first().map(|s| s.as_str()) {
                Some("start") => parse_tun_start(remaining[1..].to_vec()),
                Some("stop") => parse_client_command(remaining[1..].to_vec(), |socket_path| {
                    Command::TunStop { socket_path }
                }),
                Some("status") => parse_client_command(remaining[1..].to_vec(), |socket_path| {
                    Command::TunStatus { socket_path }
                }),
                _ => Err(CliError::new(
                    "tun requires subcommand: start, stop, or status".to_owned(),
                )),
            }
        }
        "build_info" | "build-info" | "version" | "--version" | "-V" => Ok(Command::BuildInfo),
        "help" | "--help" | "-h" => Ok(Command::Help),
        _ if first.starts_with('-') => Err(CliError::new(format!(
            "unknown option `{first}`\n\n{}",
            usage()
        ))),
        _ => Err(CliError::new(format!(
            "unknown command `{first}`\n\n{}",
            usage()
        ))),
    }
}

pub fn usage() -> &'static str {
    "Usage:
  zero run [--status-listen HOST:PORT] [--control-socket PATH] CONFIG
  zero status [--json] [--socket PATH] [CONFIG]
  zero select <group> <target> [--socket PATH]
  zero flows [--socket PATH]
  zero policies [--socket PATH]
  zero events [--socket PATH]
  zero reload CONFIG [--socket PATH]
  zero validate CONFIG
  zero connector state [--json] CONFIG
  zero mode <rule|direct|global> [outbound] [--socket PATH]
  zero tun start --addr IP --tag TAG [--name NAME] [--mask MASK] [--secondary-addr CIDR] [--mtu MTU] [--include-cidr CIDR]... [--exclude-cidr CIDR]... [--no-auto-route] [--single-stack] [--no-strict-route] [--no-dns-hijack] [--socket PATH]
  zero tun stop [--socket PATH]
  zero tun status [--socket PATH]
  zero build-info
  zero version
  zero -V, --version
  zero help

Examples:
  zero run config.json
  zero run --status-listen 127.0.0.1:9090 config.json
  zero tun start --addr 10.0.0.1 --tag my-tun
  zero tun status
  zero select proxy direct
  zero status
  zero reload config.json"
}

fn parse_run(args: Vec<String>) -> Result<Command, CliError> {
    let mut status_listen = None;
    let mut control_socket = None;
    let mut ipc_hook_socket = None;
    let mut config_path = None;
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--status-listen" => {
                let listen = iter.next().ok_or_else(|| {
                    CliError::new(format!(
                        "`run --status-listen` requires a listen address\n\n{}",
                        usage()
                    ))
                })?;
                status_listen = Some(listen);
            }
            "--control-socket" => {
                let path = iter.next().ok_or_else(|| {
                    CliError::new(format!(
                        "`run --control-socket` requires a socket path\n\n{}",
                        usage()
                    ))
                })?;
                control_socket = Some(path);
            }
            "--ipc-hook-socket" => {
                let path = iter.next().ok_or_else(|| {
                    CliError::new(format!(
                        "`run --ipc-hook-socket` requires a socket path\n\n{}",
                        usage()
                    ))
                })?;
                ipc_hook_socket = Some(path);
            }
            _ if arg.starts_with('-') => {
                return Err(CliError::new(format!(
                    "unknown run option `{arg}`\n\n{}",
                    usage()
                )));
            }
            _ => {
                if config_path.is_some() {
                    return Err(CliError::new(format!(
                        "`run` accepts at most one config path\n\n{}",
                        usage()
                    )));
                }
                config_path = Some(arg);
            }
        }
    }

    let config_path = config_path.ok_or_else(|| {
        CliError::new(format!("`run` requires a config file path\n\n{}", usage()))
    })?;
    Ok(Command::Run {
        config_path,
        status_listen,
        control_socket,
        ipc_hook_socket,
    })
}

fn parse_status(args: Vec<String>) -> Result<Command, CliError> {
    let mut json = false;
    let mut config_path = None;
    let mut socket_path = None;

    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--json" => json = true,
            "--socket" => {
                socket_path = Some(
                    iter.next()
                        .ok_or_else(|| CliError::new("`--socket` requires a path argument"))?,
                );
            }
            _ if arg.starts_with('-') => {
                return Err(CliError::new(format!(
                    "unknown status option `{arg}`\n\n{}",
                    usage()
                )));
            }
            _ => {
                if config_path.is_some() {
                    return Err(CliError::new(format!(
                        "`status` accepts at most one config path\n\n{}",
                        usage()
                    )));
                }
                config_path = Some(arg);
            }
        }
    }

    Ok(Command::Status {
        config_path,
        json,
        socket_path,
    })
}

fn parse_select(args: Vec<String>) -> Result<Command, CliError> {
    let mut iter = args.into_iter();
    let mut positional = Vec::new();
    let mut socket_path = None;

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--socket" => {
                socket_path = Some(
                    iter.next()
                        .ok_or_else(|| CliError::new("`--socket` requires a path argument"))?,
                );
            }
            _ if arg.starts_with('-') => {
                return Err(CliError::new(format!(
                    "unknown option `{arg}`\n\n{}",
                    usage()
                )));
            }
            _ => positional.push(arg),
        }
    }

    if positional.len() != 2 {
        return Err(CliError::new(format!(
            "`select` requires <policy> and <target> arguments\n\n{}",
            usage()
        )));
    }

    Ok(Command::Select {
        policy_tag: positional.remove(0),
        target_tag: positional.remove(0),
        socket_path,
    })
}

fn parse_validate(args: Vec<String>) -> Result<Command, CliError> {
    let config_path = args
        .into_iter()
        .find(|a| !a.starts_with('-'))
        .ok_or_else(|| {
            CliError::new(format!(
                "`validate` requires a config file path\n\n{}",
                usage()
            ))
        })?;
    Ok(Command::Validate { config_path })
}

fn parse_connector(args: Vec<String>) -> Result<Command, CliError> {
    let mut iter = args.into_iter();
    match iter.next().as_deref() {
        Some("state") => parse_connector_state(iter.collect()),
        _ => Err(CliError::new(format!(
            "`connector` requires subcommand: state\n\n{}",
            usage()
        ))),
    }
}

fn parse_connector_state(args: Vec<String>) -> Result<Command, CliError> {
    let mut config_path = None;
    let mut json = false;
    for arg in args {
        match arg.as_str() {
            "--json" => json = true,
            _ if arg.starts_with('-') => {
                return Err(CliError::new(format!(
                    "unknown connector state option `{arg}`\n\n{}",
                    usage()
                )));
            }
            _ if config_path.is_none() => config_path = Some(arg),
            _ => {
                return Err(CliError::new(format!(
                    "`connector state` accepts one config path\n\n{}",
                    usage()
                )));
            }
        }
    }
    let config_path = config_path.ok_or_else(|| {
        CliError::new(format!(
            "`connector state` requires a config file path\n\n{}",
            usage()
        ))
    })?;
    Ok(Command::ConnectorState { config_path, json })
}

fn parse_tun_start(args: Vec<String>) -> Result<Command, CliError> {
    let mut name: Option<String> = None;
    let mut addr: Option<String> = None;
    let mut mask: Option<String> = None;
    let mut secondary_addr: Option<String> = None;
    let mut mtu: Option<u16> = None;
    let mut tag: Option<String> = None;
    let mut auto_route = true;
    let mut include_cidrs = Vec::new();
    let mut exclude_cidrs = Vec::new();
    let mut dual_stack = true;
    let mut strict_route = true;
    let mut dns_hijack = true;
    let mut socket_path: Option<String> = None;
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--name" => name = Some(iter.next().ok_or(CliError::new("--name requires value"))?),
            "--addr" => addr = Some(iter.next().ok_or(CliError::new("--addr requires value"))?),
            "--mask" => mask = Some(iter.next().ok_or(CliError::new("--mask requires value"))?),
            "--secondary-addr" => {
                secondary_addr = Some(
                    iter.next()
                        .ok_or(CliError::new("--secondary-addr requires value"))?,
                )
            }
            "--mtu" => {
                mtu = Some(
                    iter.next()
                        .ok_or(CliError::new("--mtu requires value"))?
                        .parse()
                        .map_err(|_| CliError::new("--mtu must be a number"))?,
                )
            }
            "--tag" => tag = Some(iter.next().ok_or(CliError::new("--tag requires value"))?),
            "--include-cidr" => include_cidrs.push(
                iter.next()
                    .ok_or(CliError::new("--include-cidr requires value"))?,
            ),
            "--exclude-cidr" => exclude_cidrs.push(
                iter.next()
                    .ok_or(CliError::new("--exclude-cidr requires value"))?,
            ),
            "--no-auto-route" => auto_route = false,
            "--single-stack" => dual_stack = false,
            "--no-strict-route" => strict_route = false,
            "--no-dns-hijack" => dns_hijack = false,
            "--socket" => {
                socket_path = Some(
                    iter.next()
                        .ok_or(CliError::new("--socket requires value"))?,
                );
            }
            a if a.starts_with('-') => return Err(CliError::new(format!("unknown option `{a}`"))),
            _ => {}
        }
    }
    let addr = addr.ok_or(CliError::new("--addr is required"))?;
    let tag = tag.ok_or(CliError::new("--tag is required"))?;
    Ok(Command::TunStart {
        name,
        addr,
        mask,
        secondary_addr,
        mtu,
        tag,
        auto_route,
        include_cidrs,
        exclude_cidrs,
        dual_stack,
        strict_route,
        dns_hijack,
        socket_path,
    })
}

fn parse_mode(args: Vec<String>) -> Result<Command, CliError> {
    let mut mode = None;
    let mut outbound = None;
    let mut socket_path = None;
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--socket" => {
                socket_path = Some(
                    iter.next()
                        .ok_or_else(|| CliError::new("`--socket` requires a path argument"))?,
                );
            }
            a if a.starts_with('-') => {
                return Err(CliError::new(format!(
                    "unknown option `{a}`\n\n{}",
                    usage()
                )));
            }
            _ => {
                if mode.is_none() {
                    mode = Some(arg);
                } else if outbound.is_none() {
                    outbound = Some(arg);
                } else {
                    return Err(CliError::new(format!(
                        "unexpected argument `{arg}`\n\n{}",
                        usage()
                    )));
                }
            }
        }
    }

    let mode = mode.ok_or_else(|| {
        CliError::new(format!(
            "mode is required (rule, direct, or global <outbound>)\n\n{}",
            usage()
        ))
    })?;

    Ok(Command::Mode {
        mode,
        outbound,
        socket_path,
    })
}

fn parse_reload(args: Vec<String>) -> Result<Command, CliError> {
    let mut config_path = None;
    let mut socket_path = None;
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--socket" => {
                socket_path = Some(
                    iter.next()
                        .ok_or_else(|| CliError::new("`--socket` requires a path argument"))?,
                );
            }
            _ if arg.starts_with('-') => {
                return Err(CliError::new(format!(
                    "unknown option `{arg}`\n\n{}",
                    usage()
                )));
            }
            _ => {
                if config_path.is_some() {
                    return Err(CliError::new(format!(
                        "`reload` accepts at most one config path\n\n{}",
                        usage()
                    )));
                }
                config_path = Some(arg);
            }
        }
    }

    let config_path = config_path.ok_or_else(|| {
        CliError::new(format!(
            "`reload` requires a config file path\n\n{}",
            usage()
        ))
    })?;
    Ok(Command::Reload {
        config_path,
        socket_path,
    })
}

fn parse_client_command(
    args: Vec<String>,
    make: impl FnOnce(Option<String>) -> Command,
) -> Result<Command, CliError> {
    let mut socket_path = None;
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--socket" => {
                socket_path = Some(
                    iter.next()
                        .ok_or_else(|| CliError::new("`--socket` requires a path argument"))?,
                );
            }
            _ if arg.starts_with('-') => {
                return Err(CliError::new(format!(
                    "unknown option `{arg}`\n\n{}",
                    usage()
                )));
            }
            _ => {
                return Err(CliError::new(format!(
                    "unexpected argument `{arg}`\n\n{}",
                    usage()
                )));
            }
        }
    }

    Ok(make(socket_path))
}
