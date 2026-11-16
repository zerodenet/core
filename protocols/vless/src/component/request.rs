//! Protocol framing consumes the actual Business/Service facts, never a fake
//! destination domain or business session for a reverse control connection.
use super::Options;
use zero_core::{request::ServicePurpose, Error};
use zero_platform_tokio::contract::{
    context::{OpenContext, RequestIdentity},
    stream::ConnectRequest,
};

pub(super) fn dialed_ingress(context: &OpenContext) -> bool {
    matches!(&context.identity, RequestIdentity::Service(facts)
        if facts.purpose == ServicePurpose::DialedIngress)
}
impl Options {
    pub(super) fn stream_request(
        &self,
        request: &ConnectRequest,
        context: &OpenContext,
    ) -> Result<Vec<u8>, Error> {
        if dialed_ingress(context) {
            // ConnectRequest retains the real remote endpoint for carrier
            // selection. Rvs itself has no address or port on the wire.
            if request.destination != self.endpoint {
                return Err(Error::Config(
                    "reverse control endpoint differs from its owner",
                ));
            }
            crate::reverse::build_request(&self.uuid, self.flow)
        } else {
            crate::outbound::build_tcp_request_to(
                &request.destination.address,
                request.destination.port,
                &self.uuid,
                self.flow,
            )
        }
    }
}
