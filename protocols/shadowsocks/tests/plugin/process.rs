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

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_self_connected_tcp_probe_cannot_confirm_plugin_readiness() {
    use tokio::io::AsyncWriteExt;
    let mut child = tokio::process::Command::new("sh")
        .args(["-c", "read line; exit 7"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take();
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let endpoint = socket.local_addr().unwrap();
    let mut socket = Some(socket);
    let result = ready_with_probe(
        || child.try_wait(),
        endpoint,
        true,
        || {
            let socket = socket.take();
            let input = input.take();
            async move {
                let Some(socket) = socket else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "no plugin listener",
                    ));
                };
                let stream = socket.connect(endpoint).await?;
                assert_eq!(stream.local_addr()?, stream.peer_addr()?);
                input.unwrap().write_all(b"exit\n").await?;
                Ok(stream)
            }
        },
    )
    .await;
    let status = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.code(), Some(7));
    assert!(
        result.is_err(),
        "self connection reported a failed, non-listening child as ready"
    );
}
