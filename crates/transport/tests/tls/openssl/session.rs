use std::{io, sync::Arc};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_traits::{ServerTlsOptions, TlsBackend, TlsParameters};

use super::Material;
use crate::profile::{OwnedClientTlsProfile, OwnedServerTlsProfile};

fn tls12_parameters() -> TlsParameters {
    TlsParameters {
        min_version: "1.2".into(),
        max_version: "1.2".into(),
        enable_session_resumption: true,
        ..Default::default()
    }
}

fn profiles(material: &Material) -> (OwnedServerTlsProfile, OwnedClientTlsProfile) {
    let parameters = tls12_parameters();
    let server = material.server(ServerTlsOptions {
        backend: TlsBackend::OpenSsl,
        one_time_loading: true,
        parameters: parameters.clone(),
        ..Default::default()
    });
    let mut client = material.client(parameters);
    client.server_name = None;
    (server, client)
}

async fn handshake(
    context: &super::super::super::OpenSslServerContext,
    profile: &OwnedClientTlsProfile,
    default_server_name: &str,
) -> io::Result<(bool, bool)> {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let (client, server) = tokio::join!(
        super::super::super::client::connect(client_io, profile, None, default_server_name),
        super::super::super::stream::OpenSslTlsStream::accept(context, server_io),
    );
    let mut client = client?;
    let mut server = server?;
    let client_reused = client.session_reused();
    let server_reused = server.session_reused();
    client.write_all(b"s").await?;
    client.flush().await?;
    let mut byte = [0];
    server.read_exact(&mut byte).await?;
    assert_eq!(byte, *b"s");
    let (client_shutdown, server_shutdown) = tokio::join!(client.shutdown(), server.shutdown());
    client_shutdown?;
    server_shutdown?;
    Ok((client_reused, server_reused))
}

#[tokio::test]
async fn openssl_tls12_reuses_session_for_the_same_effective_server_name() {
    let material = Material::new("resume.example");
    let (server, client) = profiles(&material);
    let context = super::super::super::OpenSslServerContext::build(&server, None).unwrap();

    assert_eq!(
        handshake(&context, &client, "resume.example")
            .await
            .unwrap(),
        (false, false)
    );
    assert_eq!(
        handshake(&context, &client, "resume.example")
            .await
            .unwrap(),
        (true, true)
    );
}

#[tokio::test]
async fn openssl_session_cache_isolates_effective_default_server_names() {
    let material = Material::new("isolate-a.example");
    let (server, client) = profiles(&material);
    let context =
        Arc::new(super::super::super::OpenSslServerContext::build(&server, None).unwrap());

    assert_eq!(
        handshake(&context, &client, "isolate-a.example")
            .await
            .unwrap(),
        (false, false)
    );
    assert_eq!(
        handshake(&context, &client, "isolate-b.example")
            .await
            .unwrap(),
        (false, false)
    );
    assert_eq!(
        handshake(&context, &client, "isolate-b.example")
            .await
            .unwrap(),
        (true, true)
    );
}
