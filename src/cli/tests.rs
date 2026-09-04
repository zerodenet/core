use super::{config_path_from_args, parse_args, Command};

#[test]
fn parses_private_macos_utun_helper_without_treating_socket_as_config() {
    let args = vec![
        "zero".to_owned(),
        "__macos-tun-create-helper".to_owned(),
        "--socket".to_owned(),
        "/tmp/descriptor.sock".to_owned(),
        "--name".to_owned(),
        "utun8".to_owned(),
    ];

    assert_eq!(config_path_from_args(&args), None);
    assert_eq!(
        parse_args(args).expect("helper command should parse"),
        Command::MacosTunCreateHelper {
            socket_path: "/tmp/descriptor.sock".to_owned(),
            name: Some("utun8".to_owned()),
        }
    );
}

#[test]
fn private_macos_utun_helper_requires_a_socket() {
    let error = parse_args(["zero".to_owned(), "__macos-tun-create-helper".to_owned()])
        .expect_err("helper without descriptor socket must fail");

    assert!(error.to_string().contains("requires `--socket PATH`"));
}

#[test]
fn run_parses_managed_parent_lifetime_flag() {
    let command = parse_args([
        "zero".to_owned(),
        "run".to_owned(),
        "--parent-lifetime-stdin".to_owned(),
        "--control-socket".to_owned(),
        "/tmp/managed.sock".to_owned(),
        "config.json".to_owned(),
    ])
    .expect("managed run command should parse");

    assert_eq!(
        command,
        Command::Run {
            config_path: "config.json".to_owned(),
            status_listen: None,
            control_socket: Some("/tmp/managed.sock".to_owned()),
            ipc_hook_socket: None,
            parent_lifetime_stdin: true,
        }
    );
}

#[test]
fn managed_run_finds_config_after_lifetime_and_socket_options() {
    let args = vec![
        "zero".to_owned(),
        "run".to_owned(),
        "--parent-lifetime-stdin".to_owned(),
        "--control-socket".to_owned(),
        "/tmp/managed.sock".to_owned(),
        "config.json".to_owned(),
    ];

    assert_eq!(config_path_from_args(&args), Some("config.json"));
}
