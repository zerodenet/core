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
