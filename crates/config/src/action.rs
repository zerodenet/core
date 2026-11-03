use crate::RouteActionConfig;
use zero_router::RouteAction;

impl RouteActionConfig {
    pub fn compile(&self) -> RouteAction {
        match self {
            Self::Direct => RouteAction::Direct,
            Self::Reject => RouteAction::Reject,
            Self::Route { outbound } => RouteAction::Route(outbound.clone()),
        }
    }

    pub(crate) fn target_ref(&self) -> Option<&str> {
        match self {
            Self::Route { outbound } => Some(outbound),
            Self::Direct | Self::Reject => None,
        }
    }
}
