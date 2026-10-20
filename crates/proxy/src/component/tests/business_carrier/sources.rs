//! Real installed caller variants; none can be made from a numeric identity.
#![cfg(feature = "vmess")]
use super::*;
use zero_core::Session;
use zero_platform_tokio::contract::scope::TaskJoinWitness;
#[derive(Clone, Copy)]
pub(in crate::component) enum Kind {
    Accepted,
    #[cfg(feature = "raw-ip-runtime")]
    Raw,
}
impl Kind {
    pub(in crate::component) fn all() -> Vec<Self> {
        vec![
            Self::Accepted,
            #[cfg(feature = "raw-ip-runtime")]
            Self::Raw,
        ]
    }
}
pub(in crate::component) enum Caller {
    Accepted(Box<accepted::Caller>),
    #[cfg(feature = "raw-ip-runtime")]
    Raw(Box<caller::Caller>),
}
impl Caller {
    pub(in crate::component) async fn new(proxy: &Proxy, kind: Kind) -> Self {
        match kind {
            Kind::Accepted => Self::Accepted(Box::new(accepted::Caller::new(proxy).await)),
            #[cfg(feature = "raw-ip-runtime")]
            Kind::Raw => Self::Raw(Box::new(caller::Caller::new(proxy).await)),
        }
    }
    pub(in crate::component) async fn new_for_session(
        proxy: &Proxy,
        kind: Kind,
        session: Session,
    ) -> Self {
        match kind {
            Kind::Accepted => Self::Accepted(Box::new(
                accepted::Caller::new_for_session(proxy, session).await,
            )),
            #[cfg(feature = "raw-ip-runtime")]
            Kind::Raw => Self::Raw(Box::new(
                caller::Caller::new_for_session(proxy, session).await,
            )),
        }
    }
    pub(in crate::component) fn session(&self) -> &Session {
        match self {
            Self::Accepted(caller) => &caller.session,
            #[cfg(feature = "raw-ip-runtime")]
            Self::Raw(caller) => &caller.session,
        }
    }
    pub(in crate::component) fn invocation(&self) -> &Invocation {
        match self {
            Self::Accepted(caller) => &caller.invocation,
            #[cfg(feature = "raw-ip-runtime")]
            Self::Raw(caller) => &caller.invocation,
        }
    }
    pub(in crate::component) fn witness(&self) -> TaskJoinWitness {
        match self {
            Self::Accepted(caller) => caller.witness(),
            #[cfg(feature = "raw-ip-runtime")]
            Self::Raw(caller) => caller.witness(),
        }
    }
    pub(in crate::component) fn verify_carrier_completion(&self) {
        match self {
            Self::Accepted(caller) => caller.verify_carrier_completion(),
            #[cfg(feature = "raw-ip-runtime")]
            Self::Raw(caller) => caller.verify_carrier_completion(),
        }
    }
    pub(in crate::component) async fn join(&mut self) {
        match self {
            Self::Accepted(caller) => caller.join().await,
            #[cfg(feature = "raw-ip-runtime")]
            Self::Raw(caller) => caller.join().await,
        }
    }
}
