use super::*;
fn args(input: &[&str]) -> Vec<String> {
    input.iter().map(|s| (*s).to_owned()).collect()
}
#[test]
fn omitted_client_limits_preserve_the_original_command_variant() {
    assert_eq!(
        parse(args(&["zero", "flows"])).unwrap(),
        Command::Flows { socket_path: None }
    );
    let command = parse(args(&[
        "zero",
        "--control-client-max-frame-bytes",
        "1024",
        "tun",
        "recover",
        "--socket",
        "named",
    ]))
    .unwrap();
    assert!(
        matches!(command,Command::ControlClient{max_frame_bytes:1024,command,..} if matches!(*command,Command::TunRecover{socket_path:Some(ref path)} if path=="named"))
    );
    assert_eq!(
        config_path_from_args(&args(&[
            "zero",
            "--control-client-max-frame-bytes",
            "1024",
            "reload",
            "--control-client-total-max-bytes",
            "4096",
            "actual.json"
        ])),
        Some("actual.json")
    );
}
#[test]
fn explicit_client_limits_are_bounded_and_cannot_be_silently_ignored() {
    for input in [
        vec!["zero", "flows", "--control-client-max-frame-bytes", "0"],
        vec![
            "zero",
            "flows",
            "--control-client-max-frame-bytes",
            "8",
            "--control-client-total-max-bytes",
            "7",
        ],
        vec![
            "zero",
            "flows",
            "--control-client-total-max-bytes",
            "268435457",
        ],
        vec![
            "zero",
            "flows",
            "--control-client-max-frame-bytes",
            "8",
            "--control-client-max-frame-bytes",
            "8",
        ],
        vec![
            "zero",
            "run",
            "file.json",
            "--control-client-max-frame-bytes",
            "8",
        ],
    ] {
        assert!(parse(args(&input)).is_err(), "{input:?}");
    }
}
