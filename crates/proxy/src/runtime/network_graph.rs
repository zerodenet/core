//! Data-plane route planning over registered endpoints and executable adapters.
//!
//! Routing chooses the outbound first. This graph only chooses a path inside
//! that outbound; it never compares outbound tags or changes fallback order.

use zero_stack::packet::{IPPROTO_TCP, IPPROTO_UDP};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Plane {
    Packet,
    Stream,
    Datagram,
}

impl Plane {
    const ALL: [Self; 3] = [Self::Packet, Self::Stream, Self::Datagram];

    const fn index(self) -> usize {
        match self {
            Self::Packet => 0,
            Self::Stream => 1,
            Self::Datagram => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PlaneSet(u8);

impl PlaneSet {
    pub(crate) const fn empty() -> Self {
        Self(0)
    }

    pub(crate) fn insert(&mut self, plane: Plane) {
        self.0 |= 1 << plane.index();
    }

    pub(crate) fn retain_only(&mut self, plane: Plane) {
        self.0 &= 1 << plane.index();
    }

    fn contains(self, plane: Plane) -> bool {
        self.0 & (1 << plane.index()) != 0
    }
}

/// A conversion edge is registered only when its runtime adapter exists.
/// The protocol filter prevents an IP packet from becoming the wrong L4 flow.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ConversionEdge {
    pub(crate) from: Plane,
    pub(crate) to: Plane,
    pub(crate) protocol: u8,
    pub(crate) cost: u16,
}

pub(crate) struct NetworkGraph<'a> {
    edges: &'a [ConversionEdge],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PlanePath {
    pub(crate) planes: Vec<Plane>,
    pub(crate) cost: u32,
}

const PACKET_INGRESS_EDGES: [ConversionEdge; 2] = [
    ConversionEdge {
        from: Plane::Packet,
        to: Plane::Stream,
        protocol: IPPROTO_TCP,
        cost: 1,
    },
    ConversionEdge {
        from: Plane::Packet,
        to: Plane::Datagram,
        protocol: IPPROTO_UDP,
        cost: 1,
    },
];

const FLOW_INGRESS_EDGES: [ConversionEdge; 2] = [
    ConversionEdge {
        from: Plane::Stream,
        to: Plane::Packet,
        protocol: IPPROTO_TCP,
        cost: 1,
    },
    ConversionEdge {
        from: Plane::Datagram,
        to: Plane::Packet,
        protocol: IPPROTO_UDP,
        cost: 1,
    },
];

impl NetworkGraph<'static> {
    pub(crate) fn packet_ingress() -> Self {
        Self::with_edges(&PACKET_INGRESS_EDGES)
    }

    pub(crate) fn flow_ingress() -> Self {
        Self::with_edges(&FLOW_INGRESS_EDGES)
    }
}

impl<'a> NetworkGraph<'a> {
    pub(crate) fn with_edges(edges: &'a [ConversionEdge]) -> Self {
        Self { edges }
    }

    pub(crate) fn shortest_path(
        &self,
        source: Plane,
        sinks: PlaneSet,
        protocol: Option<u8>,
    ) -> Option<PlanePath> {
        let mut costs = [u32::MAX; 3];
        let mut previous = [None; 3];
        let mut settled = [false; 3];
        costs[source.index()] = 0;

        for _ in Plane::ALL {
            let current = Plane::ALL
                .into_iter()
                .filter(|plane| !settled[plane.index()])
                .min_by_key(|plane| (costs[plane.index()], plane.index()))?;
            if costs[current.index()] == u32::MAX {
                break;
            }
            settled[current.index()] = true;
            for edge in self
                .edges
                .iter()
                .filter(|edge| edge.from == current && Some(edge.protocol) == protocol)
            {
                let next = edge.to.index();
                let cost = costs[current.index()].saturating_add(u32::from(edge.cost));
                if cost < costs[next] {
                    costs[next] = cost;
                    previous[next] = Some(current);
                }
            }
        }

        let sink = Plane::ALL
            .into_iter()
            .filter(|plane| sinks.contains(*plane) && costs[plane.index()] != u32::MAX)
            .min_by_key(|plane| (costs[plane.index()], plane.index()))?;
        let mut planes = vec![sink];
        while let Some(plane) = previous[planes.last()?.index()] {
            planes.push(plane);
        }
        planes.reverse();
        Some(PlanePath {
            planes,
            cost: costs[sink.index()],
        })
    }
}

#[cfg(test)]
#[path = "network_graph/tests.rs"]
mod tests;
