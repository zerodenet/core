//! The fixture retains the same dispatcher originals until its real Source joins.
use super::*;
use crate::runtime::{
    udp_dispatch::{DispatchDirectSockets, UdpDispatch},
    udp_flow::{response_join::ResponseSetOwner, sessions::CompletedUdpFlow},
};
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::sync::Mutex;
use zero_engine::EngineError;
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, ResourceLifecycle},
    CloseReason,
};

#[derive(Clone)]
pub(crate) struct Source {
    runtime: UdpIngressRuntime,
    state: Arc<Mutex<State>>,
}
impl Deref for Source {
    type Target = UdpIngressRuntime;
    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}
impl Source {
    // Run only inside this existing real accepted handler. The same installed
    // source identity/caller serves Stream tests; no new task or I/O grant.
    pub(crate) fn tcp_runtime(
        &self,
        inbound: &str,
    ) -> crate::runtime::tcp_ingress::TcpIngressRuntime {
        crate::runtime::tcp_ingress::TcpIngressRuntime::new(
            self.runtime.tcp_services.clone(),
            inbound.into(),
            None,
        )
        .with_accepted_caller(self.runtime.accepted_caller().unwrap().clone())
    }
    pub(super) fn new(runtime: UdpIngressRuntime, state: Arc<Mutex<State>>) -> Self {
        Self { runtime, state }
    }
    pub(crate) fn with_datagram_returns(
        mut self,
        returns: Arc<crate::component::datagram_retirement::DatagramReturns>,
    ) -> Self {
        self.state.try_lock().unwrap().returns = returns.clone();
        self.runtime = self.runtime.with_datagram_returns(returns);
        self
    }
    // The production relay owns and returns its actual raw dispatcher. Retain
    // that exact returned IO set in this already installed Source owner before
    // its original accepted task completes; cleanup still waits for its join.
    pub(crate) fn finish_dispatch(&self, inner: UdpDispatch) -> Vec<CompletedUdpFlow> {
        Dispatch {
            inner: Some(inner),
            state: self.state.clone(),
        }
        .finish_all()
    }
    pub(crate) async fn new_dispatch(&self, inbound: &str) -> Result<Dispatch, EngineError> {
        let inner = self.runtime.new_dispatch(inbound).await?;
        Ok(Dispatch {
            inner: Some(inner),
            state: self.state.clone(),
        })
    }
}

pub(crate) struct Dispatch {
    inner: Option<UdpDispatch>,
    state: Arc<Mutex<State>>,
}
impl Deref for Dispatch {
    type Target = UdpDispatch;
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap()
    }
}
impl DerefMut for Dispatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().unwrap()
    }
}
impl Dispatch {
    pub(crate) fn finish_all(mut self) -> Vec<CompletedUdpFlow> {
        self.retain()
    }
    fn retain(&mut self) -> Vec<CompletedUdpFlow> {
        let Some(inner) = self.inner.take() else {
            return vec![];
        };
        let (flows, direct, responses) = inner.finish_retaining_io();
        let mut state = self
            .state
            .try_lock()
            .expect("cleanup starts only after this original task joins");
        assert!(
            state.direct.is_none() && state.responses.is_none(),
            "one declared dispatcher original"
        );
        state.direct = Some(direct);
        state.responses = Some(responses);
        flows
    }
}
impl Drop for Dispatch {
    fn drop(&mut self) {
        self.retain();
    }
}

pub(super) struct State {
    direct: Option<DispatchDirectSockets>,
    responses: Option<Arc<ResponseSetOwner>>,
    returns: Arc<crate::component::datagram_retirement::DatagramReturns>,
    report: CloseReport,
}
pub(super) struct Owner {
    state: Arc<Mutex<State>>,
    cancel: CancellationToken,
    retired: Arc<crate::component::datagram_retirement::RetiredDatagrams>,
}
impl Owner {
    pub(super) fn new(proxy: &Proxy, cancel: CancellationToken) -> (Self, Arc<Mutex<State>>) {
        let state = Arc::new(Mutex::new(State {
            direct: None,
            responses: None,
            returns: Arc::new(Default::default()),
            report: Default::default(),
        }));
        let retired = proxy.tcp_runtime_services().execution().retired_datagrams();
        (
            Self {
                state: state.clone(),
                cancel,
                retired,
            },
            state,
        )
    }
    pub(super) fn returns(
        state: &Arc<Mutex<State>>,
    ) -> Arc<crate::component::datagram_retirement::DatagramReturns> {
        state.try_lock().unwrap().returns.clone()
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.cancel.cancel();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel.cancel();
        let mut state = tokio::time::timeout_at(deadline, self.state.lock())
            .await
            .map_err(failure)?;
        if let Some(responses) = state.responses.as_ref() {
            let receipt = responses.shutdown(deadline).await?;
            assert!(receipt.matches(responses));
            assert!(responses.first_fatal_failure().is_none());
            assert!(receipt.packet_failure().is_none());
            state.responses = None;
        }
        if let Some(direct) = state.direct.as_mut() {
            let report = direct.close_originals_until(deadline).await?;
            let complete = report.is_complete();
            state.report.append(report);
            if !complete {
                return Ok(std::mem::take(&mut state.report));
            }
            state.direct = None;
        }
        let report = self.retired.wait_returns(&state.returns, deadline).await?;
        state.report.append(report);
        Ok(std::mem::take(&mut state.report))
    }
}
