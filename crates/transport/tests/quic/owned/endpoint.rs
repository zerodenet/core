use super::*;
use std::{future::Future, task::Poll, time::Duration};
use tokio::time::Instant;
use zero_platform_tokio::contract::{
    scope::{TaskClass, TaskScope},
    ChannelError, Reason,
};

fn configs() -> (quinn::ServerConfig, quinn::ClientConfig) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into());
    let server = quinn::ServerConfig::with_single_cert(vec![cert.cert.der().clone()], key).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client = quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
    (server, client)
}
fn owner(
    scope: &TaskScope,
    users: usize,
) -> (
    OwnedEndpointOwner,
    DriverExitReceiver,
    OwnedEndpointHandle,
    Arc<dyn quinn::AsyncUdpSocket>,
) {
    let native = socket();
    // A retained read-only diagnostic view is not used as another producer.
    // Its real native local_addr failure after shutdown is not a port-rebind test.
    let retained = native.socket();
    let (mut owner, receiver) = OwnedEndpointOwner::new(
        native,
        scope.registrar(),
        OwnedEndpointLimits {
            drivers: bound(3),
            users: bound(users),
        },
    );
    let (server, client) = configs();
    let handle = owner
        .start(Default::default(), Some(server), Some(client))
        .unwrap();
    (owner, receiver, handle, retained)
}
async fn collect(
    scope: &mut TaskScope,
    receiver: &mut DriverExitReceiver,
    count: usize,
) -> Vec<TaskClass> {
    let mut classes = Vec::new();
    for _ in 0..count {
        let exit = scope.next_exit().await.unwrap();
        exit.result.unwrap();
        classes.push(exit.class);
        receiver
            .receive(exit.id, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }
    classes
}
mod admission;
mod users;

mod hosted;

mod bidi;
