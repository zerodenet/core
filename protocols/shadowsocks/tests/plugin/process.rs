use super::*;
use crate::validation::PluginMode;
#[test]
fn plugin_environment_is_opaque_and_obfsproxy_uses_reference_arguments() {
    let mut config = PluginConfig {
        command: "example-plugin".into(),
        options: Some("server;path=a b;key=x=y".into()),
        args: vec!["--flag".into()],
        mode: PluginMode::TcpOnly,
    };
    let local = "127.0.0.1:1234".parse().unwrap();
    let cmd = command(&config, ("::1", 5678), local, true);
    let env = cmd
        .as_std()
        .get_envs()
        .map(|(key, value)| {
            (
                key.to_str().unwrap(),
                value.and_then(|value| value.to_str()),
            )
        })
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(env["SS_REMOTE_HOST"], Some("::1"));
    assert_eq!(env["SS_REMOTE_PORT"], Some("5678"));
    assert_eq!(env["SS_LOCAL_HOST"], Some("127.0.0.1"));
    assert_eq!(env["SS_PLUGIN_OPTIONS"], config.options.as_deref());
    config.command = "obfsproxy".into();
    config.options = Some("obfs2 --shared-secret=test".into());
    let cmd = command(&config, ("::1", 5678), local, true);
    let args = cmd
        .as_std()
        .get_args()
        .map(|value| value.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        &args[2..],
        &[
            "obfs2",
            "--shared-secret=test",
            "--dest",
            "127.0.0.1:1234",
            "server",
            "[::1]:5678",
            "--flag"
        ]
    );
    assert!(!format!("{config:?}").contains("shared-secret"));
}
