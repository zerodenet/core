//! One real DNS waiter/query responsibility, separate from persistent transports.
use super::*;
use zero_platform_tokio::contract::scope::TaskJoinWitness;

#[derive(Clone)]
pub(crate) enum DnsSourceCaller {
    DatagramChild(TaskJoinWitness),
    TcpConnection(TaskJoinWitness),
}
impl DnsSourceCaller {
    fn witness(&self) -> &TaskJoinWitness {
        match self {
            Self::DatagramChild(actual) | Self::TcpConnection(actual) => actual,
        }
    }
}
#[derive(Clone)]
pub(super) struct DnsOriginal {
    pub(super) call: DnsCall,
    caller: DnsSourceCaller,
}
#[derive(Clone)]
pub(super) enum DnsCall {
    Wire(zero_dns::DnsQueryCall),
    Resolution(zero_dns::DnsResolutionCall),
}
impl DnsCall {
    pub(super) fn cancel(&self) {
        match self {
            Self::Wire(call) => call.cancel(),
            Self::Resolution(call) => call.cancel(),
        }
    }
}
impl NativeObligations {
    pub(crate) fn bind_source(
        &self,
        scope: zero_platform_tokio::contract::scope::TaskScopeIdentity,
    ) -> Result<(), ChannelError> {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if state.closing
            || !state.records.is_empty()
            || state
                .scope
                .as_ref()
                .is_some_and(|old| !old.same_scope(&scope))
        {
            return Err(rejected(Reason::StalePlan));
        }
        state.scope = Some(scope);
        Ok(())
    }
}
impl NativePermit {
    /// Install the exact execution token and real Source child registration
    /// before the answer future's first poll can submit any DNS query IO.
    pub(crate) fn bind_dns_call(
        &mut self,
        call: zero_dns::DnsQueryCall,
        caller: DnsSourceCaller,
    ) -> Result<(), ChannelError> {
        self.bind_dns_original(DnsCall::Wire(call), caller)
    }
    fn bind_dns_original(
        &mut self,
        call: DnsCall,
        caller: DnsSourceCaller,
    ) -> Result<(), ChannelError> {
        let actual = caller.witness();
        let mut state = self
            .owner
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.closing
            || actual.is_joined()
            || !actual.scope_identity().resource_open()
            || !state
                .scope
                .as_ref()
                .is_some_and(|scope| scope.same_scope(&actual.scope_identity()))
        {
            return Err(rejected(Reason::Revoked));
        }
        let record = state
            .records
            .get_mut(&self.id)
            .ok_or_else(|| rejected(Reason::Revoked))?;
        if record.kind != ChildKind::Dns
            || record.entered
            || record.retiring
            || record.dns.is_some()
            || record.caller.is_some()
            || record.gateways != 0
        {
            return Err(rejected(Reason::InvalidConfig));
        }
        record.dns = Some(DnsOriginal { call, caller });
        record.entered = true;
        Ok(())
    }
}
impl NativeRegistration {
    /// Install the actual dual-family lookup before its future can submit I/O.
    /// The same Source's finite DNS quota and real TCP caller remain charged;
    /// persistent resolver transports stay in their canonical DNS Host.
    pub(crate) async fn bind_dns_resolution(
        &self,
        call: zero_dns::DnsResolutionCall,
        deadline: Instant,
    ) -> Result<NativePermit, ChannelError> {
        self.check_open()?;
        let caller = self
            .caller_witness()
            .ok_or_else(|| rejected(Reason::Revoked))?;
        let owner = NativeObligations(
            self.owner
                .upgrade()
                .ok_or_else(|| rejected(Reason::Revoked))?,
        );
        let mut permit = owner.reserve_reclaimed(ChildKind::Dns, deadline).await?;
        permit.bind_dns_original(
            DnsCall::Resolution(call),
            DnsSourceCaller::TcpConnection(caller),
        )?;
        Ok(permit)
    }
    /// Some is exclusively this DNS record. None preserves the existing
    /// Stream/Datagram whole-gateway settlement without broadening coverage.
    pub(super) async fn settle_dns(&self, deadline: Instant) -> Result<Option<bool>, ChannelError> {
        let Some(owner) = self.owner.upgrade() else {
            return Ok(Some(false));
        };
        let original = {
            let state = owner.lock().unwrap_or_else(|error| error.into_inner());
            let Some(record) = state.records.get(&self.id) else {
                return Ok(Some(true));
            };
            if record.kind != ChildKind::Dns {
                return Ok(None);
            }
            if !record.retiring {
                return Ok(Some(false));
            }
            let Some(original) = &record.dns else {
                return Ok(Some(false));
            };
            original.clone()
        };
        if matches!(&original.caller, DnsSourceCaller::DatagramChild(actual) if !actual.is_joined())
        {
            return Ok(Some(false));
        }
        // A TCP connection is one actual shared caller, not one new task per
        // question. Its view and Source Scope remain charged in tcp_dns. Each
        // question retires only its own returned waiter and actual Query owner.
        let failure = |source: std::io::Error| {
            let reason = if source.kind() == std::io::ErrorKind::TimedOut {
                Reason::Timeout
            } else {
                Reason::Io
            };
            rejected(reason).with_source(source)
        };
        match &original.call {
            DnsCall::Wire(call) => {
                let closed = call.wait_closed(deadline).await.map_err(failure)?;
                Ok(Some(closed.matches(call) && query_closed(&closed)?))
            }
            DnsCall::Resolution(call) => {
                let closed = call.wait_closed(deadline).await.map_err(failure)?;
                if !closed.matches(call) {
                    return Ok(Some(false));
                }
                for query in closed.queries() {
                    if !query_closed(query)? {
                        return Ok(Some(false));
                    }
                }
                Ok(Some(true))
            }
        }
    }
}
fn query_closed(closed: &zero_dns::DnsQueryCallClosed) -> Result<bool, ChannelError> {
    let matched = match (closed.query(), closed.joined()) {
        // Only the actual DNS execution can issue this no-query receipt.
        (None, None) => true,
        (Some(query), Some(joined)) => query.matches_joined(joined),
        _ => false,
    };
    if let Some(source) = closed.failure() {
        if !closed.failure_accepted() {
            return Err(rejected(Reason::Io).with_source(source.clone()));
        }
    }
    if let Some(source) = closed.cleanup_failure() {
        if !closed.cleanup_failure_accepted() {
            return Err(rejected(Reason::Io).with_source(source.clone()));
        }
    }
    // The sealed acceptance getters verify the canonical query owner's
    // finite original fault table, exact task registration/error Arc and
    // actual local failure receiver. Merely logging a cause, seeing a join
    // or assuming another Host retains it cannot release this Source entry.

    // This is query responsibility only. Persistent DoH/DoT/pooled native
    // transports remain with their original canonical Host, never with this
    // Source token; this receipt makes no whole-pool NativeClosed claim.
    Ok(matched)
}
