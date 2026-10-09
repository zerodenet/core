use crate::adapters::direct::DirectAdapter;
use crate::protocol_registry::ClaimedUdpFlowLeaf;
use crate::runtime::udp_dispatch::operation::{DirectUdpFlowOperation, PreparedUdpFlowOperation};
use crate::runtime::udp_dispatch::FlowFailure;

struct ClaimedDirectUdpLeaf {
    tag: String,
    dial_policy: zero_traits::DialPolicy,
    policy_tag: Option<String>,
    dial_generation: u64,
}

impl<'a> ClaimedUdpFlowLeaf<'a> for ClaimedDirectUdpLeaf {
    fn prepare_udp_flow(
        &self,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        zero_platform_tokio::validate_dial_policy(&self.dial_policy).map_err(|error| {
            FlowFailure {
                stage: "validate_dial_policy",
                error: zero_engine::EngineError::Io(error),
                upstream: None,
            }
        })?;
        Ok(Box::new(DirectUdpFlowOperation {
            tag: self.tag.clone(),
            dial_policy: self.dial_policy.clone(),
            policy_tag: self.policy_tag.clone(),
            dial_generation: self.dial_generation,
        }))
    }
}

impl DirectAdapter {
    pub(super) fn claim_udp_flow_leaf_impl<'a>(
        &self,
        tag: String,
        policy_tag: Option<String>,
        dial_policy: zero_traits::DialPolicy,
        dial_generation: u64,
    ) -> Box<dyn ClaimedUdpFlowLeaf<'a> + 'a> {
        Box::new(ClaimedDirectUdpLeaf {
            tag,
            policy_tag,
            dial_policy,
            dial_generation,
        })
    }
}
