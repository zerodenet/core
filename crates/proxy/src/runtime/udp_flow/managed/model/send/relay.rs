use tokio::task::JoinSet;
use zero_core::{Address, Session};

use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::udp_flow::managed::flow::{
    ManagedRelayStreamCarrier, ManagedRelayStreamFlow, ManagedUdpFlowResume,
};
use crate::runtime::udp_flow::packet_path::ChainTask;

pub(crate) struct ManagedRelayExistingSend<'a> {
    pub(crate) chain_tasks: &'a mut JoinSet<ChainTask>,
    pub(crate) session_id: u64,
    pub(crate) carrier: ManagedRelayStreamCarrier<'a>,
    pub(crate) tls_server_name: Option<&'a str>,
    pub(crate) services: Option<UdpRuntimeServices>,
    pub(crate) session: &'a Session,
    pub(crate) server: &'a str,
    pub(crate) port: u16,
    pub(crate) resume: ManagedUdpFlowResume,
    pub(crate) target: &'a Address,
    pub(crate) target_port: u16,
    pub(crate) payload: &'a [u8],
}

impl<'a> ManagedRelayExistingSend<'a> {
    pub(crate) fn relay_stream(request: ManagedRelayStreamFlow<'a>) -> Self {
        Self {
            chain_tasks: request.chain_tasks,
            session_id: request.session.id,
            carrier: request.carrier,
            tls_server_name: request.tls_server_name,
            services: request.services,
            session: request.session,
            server: request.server,
            port: request.port,
            resume: request.resume,
            target: &request.session.target,
            target_port: request.session.port,
            payload: request.payload,
        }
    }
}
