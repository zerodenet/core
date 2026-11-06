use super::*;
pub(super) fn failure(
    component: ComponentId,
    stage: Stage,
    reason: Reason,
    message: &'static str,
) -> ChannelError {
    let mut error = ChannelError::new(stage, reason, Impact::Component)
        .with_source(std::io::Error::other(message));
    error.component = Some(component);
    error
}

pub(super) fn startup_failure(component: ComponentId, source: std::io::Error) -> ChannelError {
    if let Some(grant) = source
        .get_ref()
        .and_then(|error| error.downcast_ref::<ChannelError>())
    {
        let mut error = ChannelError::new(grant.stage, grant.reason, grant.impact);
        error.component = Some(component);
        error.delivery = grant.delivery;
        error.retry = grant.retry;
        return error.with_source(source);
    }
    let reason = match source.kind() {
        std::io::ErrorKind::AlreadyExists => Reason::StalePlan,
        std::io::ErrorKind::TimedOut => Reason::Timeout,
        _ => Reason::Io,
    };
    failure(
        component,
        Stage::Prepare,
        reason,
        "plugin resource start failed",
    )
    .with_source(source)
}
