use super::*;
struct BoundOpener {
    seen: std::sync::Mutex<Vec<UpstreamBinding>>,
}
impl UpstreamOpener for BoundOpener {
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        self.seen.lock().unwrap().push(binding.clone());
        Box::pin(async {
            Err(ChannelError::new(
                Stage::Dial,
                Reason::StalePlan,
                Impact::Session,
            ))
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        panic!("stream request must not turn into datagram")
    }
}

#[tokio::test]
async fn bound_upstream_retains_prefix_policy_and_generation_without_fallback() {
    use zero_core::{component::NetworkGeneration, Address};
    let opener = Arc::new(BoundOpener {
        seen: Default::default(),
    });
    let binding = UpstreamBinding {
        prefix: vec![RoleInstanceId {
            component: ComponentId(42),
            role: RoleId::Outbound(OutboundId(42)),
            generation: ResourceGeneration(1),
        }],
        dial: zero_traits::DialPolicy {
            address_family: zero_traits::AddressFamily::OnlyIpv6,
            interface: Some("contract-interface".into()),
            source_ip: Some("::1".parse().unwrap()),
        },
        network_generation: NetworkGeneration(7),
    };
    let mut rebuilt = binding.clone();
    rebuilt.prefix[0].generation = ResourceGeneration(2);
    let identities = std::collections::HashSet::from([binding.clone(), rebuilt]);
    assert_eq!(
        identities.len(),
        2,
        "a rebuilt prefix must not reuse its old pool key"
    );
    let upstream = UpstreamTransport::new(binding.clone(), opener.clone());
    let context = CarrierContext {
        session: None,
        deadline: Some(Instant::now()),
        cancel: Default::default(),
    };
    let error = upstream
        .open_stream(
            Destination::new(Address::Domain("carrier.test".into()), 443),
            context,
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.reason, Reason::StalePlan);
    assert_eq!(opener.seen.lock().unwrap().as_slice(), [binding]);
}
