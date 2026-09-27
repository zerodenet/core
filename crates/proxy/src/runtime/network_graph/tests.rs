use zero_stack::packet::{IPPROTO_ICMP, IPPROTO_TCP, IPPROTO_UDP};

use super::{ConversionEdge, NetworkGraph, Plane, PlaneSet};

fn sinks(planes: &[Plane]) -> PlaneSet {
    let mut sinks = PlaneSet::empty();
    for plane in planes {
        sinks.insert(*plane);
    }
    sinks
}

#[test]
fn packet_endpoint_beats_flow_conversion_for_tcp_and_udp() {
    let graph = NetworkGraph::packet_ingress();
    for protocol in [IPPROTO_TCP, IPPROTO_UDP, IPPROTO_ICMP] {
        let path = graph
            .shortest_path(
                Plane::Packet,
                sinks(&[Plane::Packet, Plane::Stream, Plane::Datagram]),
                Some(protocol),
            )
            .expect("packet endpoint");
        assert_eq!(path.planes, [Plane::Packet]);
        assert_eq!(path.cost, 0);
    }
}

#[test]
fn flow_only_endpoint_requires_matching_ip_protocol() {
    let graph = NetworkGraph::packet_ingress();
    let tcp = graph.shortest_path(Plane::Packet, sinks(&[Plane::Stream]), Some(IPPROTO_TCP));
    assert_eq!(
        tcp.expect("TCP conversion").planes,
        [Plane::Packet, Plane::Stream]
    );
    let udp = graph.shortest_path(Plane::Packet, sinks(&[Plane::Datagram]), Some(IPPROTO_UDP));
    assert_eq!(
        udp.expect("UDP conversion").planes,
        [Plane::Packet, Plane::Datagram]
    );
    assert!(graph
        .shortest_path(
            Plane::Packet,
            sinks(&[Plane::Stream, Plane::Datagram]),
            Some(IPPROTO_ICMP)
        )
        .is_none());
    assert!(graph
        .shortest_path(Plane::Packet, sinks(&[Plane::Datagram]), Some(IPPROTO_TCP))
        .is_none());
}

#[test]
fn registered_reverse_adapter_can_extend_graph_without_changing_solver() {
    let edges = [ConversionEdge {
        from: Plane::Stream,
        to: Plane::Packet,
        protocol: IPPROTO_TCP,
        cost: 2,
    }];
    let graph = NetworkGraph::with_edges(&edges);
    let path = graph
        .shortest_path(Plane::Stream, sinks(&[Plane::Packet]), Some(IPPROTO_TCP))
        .expect("registered stream to packet adapter");
    assert_eq!(path.planes, [Plane::Stream, Plane::Packet]);
    assert_eq!(path.cost, 2);
    assert!(graph
        .shortest_path(Plane::Stream, sinks(&[Plane::Packet]), Some(IPPROTO_UDP))
        .is_none());
}

#[test]
fn registered_flow_to_packet_edges_choose_native_flow_first() {
    let graph = NetworkGraph::flow_ingress();
    let tcp = graph
        .shortest_path(
            Plane::Stream,
            sinks(&[Plane::Stream, Plane::Packet]),
            Some(IPPROTO_TCP),
        )
        .expect("native TCP flow");
    assert_eq!(tcp.planes, [Plane::Stream]);
    let udp = graph
        .shortest_path(Plane::Datagram, sinks(&[Plane::Packet]), Some(IPPROTO_UDP))
        .expect("UDP packet conversion");
    assert_eq!(udp.planes, [Plane::Datagram, Plane::Packet]);
}

#[test]
fn solver_uses_total_conversion_cost_instead_of_edge_order() {
    let edges = [
        ConversionEdge {
            from: Plane::Stream,
            to: Plane::Packet,
            protocol: IPPROTO_TCP,
            cost: 5,
        },
        ConversionEdge {
            from: Plane::Stream,
            to: Plane::Datagram,
            protocol: IPPROTO_TCP,
            cost: 1,
        },
        ConversionEdge {
            from: Plane::Datagram,
            to: Plane::Packet,
            protocol: IPPROTO_TCP,
            cost: 1,
        },
    ];
    let path = NetworkGraph::with_edges(&edges)
        .shortest_path(Plane::Stream, sinks(&[Plane::Packet]), Some(IPPROTO_TCP))
        .expect("cheapest registered path");
    assert_eq!(path.planes, [Plane::Stream, Plane::Datagram, Plane::Packet]);
    assert_eq!(path.cost, 2);
}
