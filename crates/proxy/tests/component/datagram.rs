use super::fixtures::{build_context, deadline, open_context};
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
};
use zero_core::{
    component::AssociationId,
    packet::Datagram,
    request::{DatagramTargets, Destination},
    Address,
};
use zero_platform_tokio::{
    contract::{
        component::ComponentFactory,
        datagram::{recv, send, BoxDatagramSession, DatagramRequest},
        network::{CarrierContext, UpstreamBinding, UpstreamOpener},
        stream::BoxStream,
        BoxFuture, ChannelError, CloseReason,
    },
    TokioDatagramSocket,
};
use zero_proxy::component::{DirectFactory, DirectOptions};
struct Carrier {
    peer: SocketAddr,
    local: Mutex<Option<SocketAddr>>,
}
impl UpstreamOpener for Carrier {
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        panic!("datagram role cannot substitute a stream");
    }
    fn open_datagram(
        &self,
        binding: &UpstreamBinding,
        request: DatagramRequest,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        assert_eq!(binding.prefix.len(), 1);
        assert_eq!(binding.prefix[0].generation.0, 8);
        assert_eq!(binding.network_generation.0, 6);
        assert_eq!(binding.dial, Default::default());
        assert_eq!(context.session.unwrap().0, 5);
        let target = Destination::new(Address::Ipv4([127, 0, 0, 1]), self.peer.port());
        assert_eq!(request.targets, DatagramTargets::Fixed(target));
        Box::pin(async move {
            let socket = TokioDatagramSocket::bind_for_peer_on(self.peer, None)
                .await
                .unwrap();
            *self.local.lock().unwrap() = Some(socket.local_addr().unwrap());
            Ok(socket.into_fixed_datagram_session(request.association, self.peer, context.cancel))
        })
    }
}
#[tokio::test]
async fn direct_datagram_role_retains_bound_carrier_and_transferred_session_after_owner_revocation()
{
    let peer = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let target = Destination::new(
        Address::Ipv4([127, 0, 0, 1]),
        peer.local_addr().unwrap().port(),
    );
    let carrier = Arc::new(Carrier {
        peer: peer.local_addr().unwrap(),
        local: Mutex::new(None),
    });
    let (context, mut scope) = build_context();
    let options = Arc::new(DirectOptions::validate(Default::default()).unwrap());
    let mut instance = DirectFactory.build(options, context).unwrap();
    instance.lifecycle.start(deadline()).await.unwrap();
    let role = instance.roles.datagram.as_ref().unwrap().clone();
    let mut channel = role
        .open(
            DatagramRequest {
                association: AssociationId(41),
                targets: DatagramTargets::Fixed(target.clone()),
            },
            open_context(carrier.clone(), Default::default()),
        )
        .await
        .unwrap();
    instance.lifecycle.revoke(CloseReason::Replaced);
    let refused = role
        .open(
            DatagramRequest {
                association: AssociationId(42),
                targets: DatagramTargets::Fixed(target.clone()),
            },
            open_context(carrier.clone(), Default::default()),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(
        refused.reason,
        zero_platform_tokio::contract::Reason::Revoked
    );
    for payload in [b"transferred real socket".as_slice(), &[]] {
        let frame = Datagram {
            address: target.clone(),
            association: AssociationId(41),
            payload: payload.to_vec().into(),
        };
        assert_eq!(
            send(
                channel.tx.as_mut(),
                channel.control.as_mut(),
                &frame,
                &channel.limits
            )
            .await
            .unwrap()
            .accepted_bytes,
            payload.len()
        );
        let mut bytes = [0; 64];
        let (size, source) = tokio::time::timeout_at(deadline(), peer.recv_from(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&bytes[..size], payload);
        peer.send_to(&bytes[..size], source).await.unwrap();
        let response = tokio::time::timeout_at(deadline(), recv(channel.rx.as_mut()))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.payload.as_ref(), payload);
        assert_eq!(response.association, AssociationId(41));
        assert_eq!(response.address, target);
    }
    let report = channel.control.wait_closed(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    let local = carrier.local.lock().unwrap().unwrap();
    TokioDatagramSocket::bind_addr(local)
        .await
        .unwrap()
        .close()
        .unwrap();
    assert!(instance
        .lifecycle
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}
