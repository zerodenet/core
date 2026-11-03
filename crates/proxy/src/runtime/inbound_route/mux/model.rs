use crate::runtime::route_runtime::InboundRouteRuntime;

pub(crate) struct MuxRouteBridge<P, FMapTcp, FRunUdp, FRunMux> {
    pub(crate) runtime: InboundRouteRuntime,
    pub(crate) protocol: P,
    pub(crate) map_tcp_stream: FMapTcp,
    pub(crate) run_udp: FRunUdp,
    pub(crate) run_mux: FRunMux,
}
