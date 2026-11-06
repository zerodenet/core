use super::*;
#[derive(Debug)]
pub(crate) enum SourceJoinFailure {
    Legacy(Arc<tokio::task::JoinError>),
    Scope {
        witness: TaskJoinWitness,
        joined: TaskJoined,
        source: Option<Arc<ChannelError>>,
    },
}
impl SourceJoinFailure {
    pub(crate) fn legacy(error: tokio::task::JoinError) -> Self {
        Self::Legacy(Arc::new(error))
    }
    pub(super) fn scoped(witness: &TaskJoinWitness, joined: TaskJoined) -> Self {
        Self::Scope {
            witness: witness.clone(),
            source: joined.result().err(),
            joined,
        }
    }
    pub(crate) fn is_cancelled(&self) -> bool {
        match self {
            Self::Legacy(source) => source.is_cancelled(),
            // Successful real join without an output means that the actual
            // same worker observed its stop before yielding its typed output.
            Self::Scope { source, .. } => source
                .as_ref()
                .is_none_or(|source| source.reason == Reason::Cancelled),
        }
    }
}
impl std::fmt::Display for SourceJoinFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Legacy(source) => source.fmt(f),
            Self::Scope { joined, .. } => write!(
                f,
                "actual Source worker {} joined with {:?}",
                joined.id().0,
                joined.result()
            ),
        }
    }
}
impl std::error::Error for SourceJoinFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Legacy(source) => Some(source.as_ref()),
            Self::Scope { source, .. } => source.as_ref().map(|source| source.as_ref() as _),
        }
    }
}
