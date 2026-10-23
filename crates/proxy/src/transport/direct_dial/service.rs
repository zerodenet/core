//! Root service dialing shares candidate order, budgets, and hard policy with
//! business Direct without constructing a business Session or flow observation.
use super::*;
use zero_platform_tokio::contract::network::CarrierContext;
use zero_platform_tokio::contract::{
    context::with_deadline, ChannelError, DeliveryState, Impact, Reason, Stage,
};

pub(crate) async fn open_service_candidates(
    candidates: Vec<SocketAddr>,
    egress: &EgressInterfaceControl,
    policy: &zero_traits::DialPolicy,
    context: CarrierContext,
) -> Result<TokioSocket, ChannelError> {
    let failure = |reason| {
        let mut error = ChannelError::new(Stage::Dial, reason, Impact::Session);
        error.session = context.session;
        error.delivery = DeliveryState::NotSubmitted;
        error.retry = zero_platform_tokio::contract::RetryClass::BeforeSubmission;
        error
    };
    if context.cancel.is_cancelled() {
        return Err(failure(Reason::Cancelled));
    }
    if candidates.is_empty() {
        return Err(failure(Reason::Io).with_source(io::Error::new(
            io::ErrorKind::NotFound,
            "DNS returned no permitted root endpoint",
        )));
    }
    let dial = dial_tcp_candidates_with_policy(candidates, egress, policy);
    tokio::select! {biased;
        _=context.cancel.cancelled()=>Err(failure(Reason::Cancelled)),
        result=with_deadline(context.deadline,dial)=>{
            let opened=result.map_err(|source|failure(Reason::Timeout).with_source(source))?
                .map_err(|failed|failure(if failed.error.kind()==io::ErrorKind::TimedOut {Reason::Timeout} else {Reason::Io}).with_source(failed.error))?;
            Ok(opened.socket)
        },
    }
}
