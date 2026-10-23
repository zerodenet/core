use super::super::endpoint::EndpointStackRecipe;
use super::super::EndpointPacket;
use tokio::sync::{mpsc, watch};

#[test]
fn endpoint_recipe_is_cold_without_runtime_and_does_not_consume_endpoint_packets() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let (sender, mut packets) = mpsc::channel::<EndpointPacket>(1);
    let (updates, endpoint) = watch::channel(sender);
    let recipe = EndpointStackRecipe::prepare(
        vec!["10.0.0.1".parse().unwrap()],
        1420,
        0,
        endpoint,
        Default::default(),
    )
    .unwrap_or_else(|error| panic!("valid cold stack recipe: {error:?}"));
    assert_eq!(updates.receiver_count(), 1);
    assert!(matches!(
        packets.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    drop(recipe);
    assert_eq!(updates.receiver_count(), 0);
    // The packet receiver remains here: preparation neither takes nor polls it.
    assert!(matches!(
        packets.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[test]
fn endpoint_recipe_rejects_invalid_geometry_without_a_runtime() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let (sender, _packets) = mpsc::channel::<EndpointPacket>(1);
    let (_updates, endpoint) = watch::channel(sender);
    assert!(matches!(
        EndpointStackRecipe::prepare(
            vec!["::1".parse().unwrap()],
            68,
            0,
            endpoint,
            Default::default(),
        ),
        Err(zero_stack::client_udp::ClientUdpStackError::InvalidMtu)
    ));
}
