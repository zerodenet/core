//! Same actual connection future and original delegate drain.
use super::*;
struct ConnectionDriver {
    inner: Driver,
    _memory: PacketMetadataCredit,
}
struct Stop(PacketMetadataArc<State>);
impl Drop for Stop {
    fn drop(&mut self) {
        self.0.runtime.stop();
    }
}
pub(super) async fn run(
    io: TcpRelayStream,
    state: PacketMetadataArc<State>,
    version: TcpVersion,
    ready: Sender,
    credit: PacketMetadataCredit,
) -> Result<(), ChannelError> {
    let _stop = Stop(state.clone());
    let (sender, driver): (carrier::Sender, Driver) = match version {
        TcpVersion::Http1 => {
            let (sender, driver) = hyper::client::conn::http1::handshake(TokioIo::new(io))
                .await
                .map_err(|source| {
                    failure(Stage::Handshake, Reason::Protocol).with_source(source)
                })?;
            (carrier::Sender::Http1(sender), Box::pin(driver))
        }
        TcpVersion::Http2 { keepalive } => {
            let executor = executor::Executor(PacketMetadataArc::downgrade(&state));
            let mut builder = hyper::client::conn::http2::Builder::new(executor);
            // Read the authenticated settings from this exact carrier before
            // moving its IO into Hyper. No extra peer-settings Vec clone.
            if let Some(settings) = io
                .application_settings()
                .filter(|settings| settings.protocol == b"h2")
            {
                builder.peer_application_settings(&settings.peer);
            }
            builder.timer(hyper_util::rt::TokioTimer::new());
            let period = if keepalive == 0 {
                Some(std::time::Duration::from_secs(45))
            } else if keepalive > 0 {
                Some(std::time::Duration::from_secs(keepalive as u64))
            } else {
                None
            };
            builder
                .keep_alive_interval(period)
                .keep_alive_timeout(std::time::Duration::from_secs(15))
                .keep_alive_while_idle(true);
            let (sender, driver) = builder
                .handshake(TokioIo::new(io))
                .await
                .map_err(|source| {
                    failure(Stage::Handshake, Reason::Protocol).with_source(source)
                })?;
            (carrier::Sender::Http2(sender), Box::pin(driver))
        }
    };
    // Keep future and ticket in explicit field destruction order on every
    // early return, error, or cancellation as well as the normal path.
    let mut driver = ConnectionDriver {
        inner: driver,
        _memory: credit,
    };
    if ready.send(Ok(sender)).is_err() {
        return Ok(());
    }
    let result = loop {
        let changed = state.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if let Some(source) = state.causes.get(0) {
            return Err(
                failure(Stage::Receive, Reason::Capacity).with_metadata_source(source.clone())
            );
        }
        tokio::select! {biased;
            result=driver.inner.as_mut()=>break result,
            _=state.runtime.run()=>return Ok(()),
            _=&mut changed=>{},
        }
    };
    // An outer connection result is held until its own original delegates have
    // all returned. Only logical Session/Request work is sealed/cancelled now.
    state.runtime.ledger.cancel_logical();
    loop {
        let changed = state.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if state.delegates.load(Ordering::Acquire) == 0 {
            break;
        }
        tokio::select! {biased;_=state.runtime.run()=>break,_=&mut changed=>{}}
    }
    drop(driver);
    match result {
        Ok(()) => Ok(()),
        Err(source) => {
            let source = state.causes.record(1, io::Error::other(source));
            Err(failure(Stage::Receive, Reason::Io).with_metadata_source(source))
        }
    }
}
