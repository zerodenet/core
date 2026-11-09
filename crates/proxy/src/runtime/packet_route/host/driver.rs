//! The original one-task device loop, returning the original device for close.
use super::task::SharedIo;
use super::*;
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
use zero_stack::OwnedFragmentReassembler;
use zero_tun::packet_memory::{NativeReadBuffer, NativeReadGrant};
pub(super) enum DriverIo<D> {
    Stopped(D),
    Exited {
        device: D,
        failure: Option<Arc<io::Error>>,
    },
}
impl<D> DriverIo<D> {
    pub(super) fn parts(self) -> (D, Option<Arc<io::Error>>) {
        match self {
            Self::Stopped(device) => (device, None),
            Self::Exited { device, failure } => (device, failure),
        }
    }
}
pub(super) struct NativeReadInput {
    pub grant: NativeReadGrant,
    pub pause: Option<super::pause::ReadCheckpoint>,
}
pub(super) async fn run<D: zero_tun::TunDevice + 'static>(
    device: D,
    input: NativeReadInput,
    mut rx: mpsc::Receiver<Write>,
    packets: Arc<PacketReturns>,
    sessions: Arc<session::Sessions>,
    mut stopped: watch::Receiver<bool>,
    mut activated: watch::Receiver<bool>,
) -> DriverIo<D> {
    let NativeReadInput {
        grant: read_grant,
        pause,
    } = input;
    let _completion = SessionCompletion(sessions.clone());
    let read_credit = ReadCredit(read_grant);
    while !*activated.borrow_and_update() {
        tokio::select! { biased; _ = stopped.changed() => return DriverIo::Stopped(device), result = activated.changed() => { if result.is_err() { return DriverIo::Stopped(device); } } }
    }
    let (mut reader, mut writer) = tokio::io::split(device);
    let mut failure = None;
    let mut buffer: Option<NativeReadBuffer> = None;
    let mut fragments = OwnedFragmentReassembler::new();
    let mut returning: Option<
        zero_platform_tokio::contract::BoxFuture<'static, Result<(), ChannelError>>,
    > = None;
    let mut timer = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = stopped.changed() => break,
            request=async {match &pause {Some(pause)=>pause.checkpoint.requested().await,None=>std::future::pending().await}}=>{
                drop(buffer.take());
                let pause=pause.as_ref().expect("same original driver checkpoint");
                #[cfg(windows)]
                let sdk_pause=match &pause.sdk {
                    Some(control)=>tokio::select! {
                        _=stopped.changed()=>break,
                        _=pause.checkpoint.resumed(request)=>continue,
                        result=control.pause()=>match result {
                            Ok(paused)=>Some(paused),
                            Err(source)=>{
                                pause.checkpoint.acknowledge(request,Err(ChannelError::new(Stage::Prepare,Reason::Io,Impact::Component).with_source(source)));
                                tokio::select! {_=stopped.changed()=>break,_=pause.checkpoint.resumed(request)=>{}}
                                continue;
                            }
                        }
                    },
                    None=>{
                        pause.checkpoint.acknowledge(request,Err(ChannelError::new(Stage::Prepare,Reason::StalePlan,Impact::Component)));
                        tokio::select! {_=stopped.changed()=>break,_=pause.checkpoint.resumed(request)=>{}}
                        continue;
                    }
                };
                let idle=returning.is_none() && rx.is_empty() && fragments.pending_datagrams()==0
                    && packets.quiescent() && sessions.logical_closed() && read_credit.0.workspace_returned();
                #[cfg(windows)]
                let idle=idle && sdk_pause.as_ref().is_some_and(|pause|pause.payload_returned());
                pause.checkpoint.acknowledge(request,if idle {Ok(())}else{Err(ChannelError::new(Stage::Prepare,Reason::Capacity,Impact::Component).with_source(io::Error::new(io::ErrorKind::WouldBlock,"same Direct original still holds users or frames")))});
                tokio::select! {_=stopped.changed()=>break,_=pause.checkpoint.resumed(request)=>{}}
                #[cfg(windows)]
                drop(sdk_pause);
            },
            acquired = read_credit.0.buffer(), if buffer.is_none() && returning.is_none() => {
                match acquired {
                    Ok(acquired) => buffer = Some(acquired),
                    Err(source) => { failure = Some(Arc::new(io::Error::other(source))); break; }
                }
            },
            delivered = async { returning.as_mut().expect("same pending native frame").await }, if returning.is_some() => {
                returning = None;
                if let Err(source) = delivered { failure = Some(Arc::new(io::Error::other(source))); break; }
            },
            result = async { reader.read(buffer.as_mut().expect("same prepaid read allocation").as_mut_bytes()).await }, if buffer.is_some() && returning.is_none() => match result {
                Ok(0) => break,
                Err(error) => { failure = Some(Arc::new(error)); break; },
                Ok(n) => {
                    let original = match buffer.take().expect("same polled read").into_packet(n) {
                        Ok(original) => original,
                        Err(_) => unreachable!("read length fits the same actual allocation"),
                    };
                    match fragments.process((), original, Instant::now()).await {
                        zero_stack::OwnedFragmentOutcome::Packet { packet, .. } => {
                            match sessions.returns.prepare_owned(packet) {
                                Ok(pending) => returning = Some(pending),
                                Err(packet) => { let _ = packets.deliver_owned(packet); }
                            }
                        }
                        zero_stack::OwnedFragmentOutcome::Pending | zero_stack::OwnedFragmentOutcome::Rejected(_) => packets.lose_receive_coverage(),
                    }
                },
            },
            write = rx.recv() => {
                let Some(write) = write else { break; };
                let stopped=match write {
                    #[cfg(test)]
                    Write::Legacy(write)=>legacy_write(write,&mut writer,&mut stopped,&mut failure).await,
                    Write::Owned(write)=>owned_write(write,&mut writer,&mut stopped,&mut failure).await,
                };
                sessions.acknowledge_controls();
                if stopped {break;}

            },
            _ = sessions.changed.notified() => sessions.acknowledge_controls(),
            _ = timer.tick() => {fragments.cleanup_expired(Instant::now());packets.expire();sessions.returns.expire();sessions.acknowledge_controls();},
        }
    }
    rx.close();
    while let Ok(write) = rx.try_recv() {
        match write {
            #[cfg(test)]
            Write::Legacy(write) => {
                write.discarded();
                let _ = write.ack.send((
                    write.packet,
                    Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "host packet writer stopped",
                    )),
                ));
            }
            Write::Owned(write) => write.finish(Err(send_failure(
                Reason::Revoked,
                "original Packet driver stopped before submission",
                DeliveryState::NotSubmitted,
                None,
            ))),
        }
    }
    sessions.finish_driver();
    packets.clear();
    DriverIo::Exited {
        device: reader.unsplit(writer),
        failure,
    }
}

struct SessionCompletion(Arc<session::Sessions>);
impl Drop for SessionCompletion {
    fn drop(&mut self) {
        self.0.stop();
    }
}
#[cfg(test)]
async fn legacy_write<W: tokio::io::AsyncWrite + Unpin>(
    write: LegacyWrite,
    writer: &mut W,
    stopped: &mut watch::Receiver<bool>,
    failure: &mut Option<Arc<io::Error>>,
) -> bool {
    if write.return_channel.is_closed() {
        write.discarded();
        let _ = write.ack.send((
            write.packet,
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "packet route closed",
            )),
        ));
        return false;
    }
    let (result, stopped, discarded) = tokio::select! { biased;
        _ = stopped.changed() => (Err(io::Error::new(io::ErrorKind::Interrupted, "host packet device stopped")), true, true),
        _ = write.return_channel.closed() => (Err(io::Error::new(io::ErrorKind::Interrupted, "packet route closed")), false, true),
        result = writer.write(&write.packet) => {
            let result = result.and_then(|n| if n == write.packet.len() { Ok(()) } else { Err(io::Error::new(io::ErrorKind::WriteZero, "partial host packet write")) });
            let failed = result.is_err(); (result, failed, false)
        },
    };
    if discarded {
        write.discarded();
    } else if let Some(observer) = &write.observer {
        if result.is_ok() {
            observer.sent(write.packet.len());
        } else {
            observer.error();
        }
    }
    let result = result.map_err(|error| {
        let error = Arc::new(error);
        if !discarded && failure.is_none() {
            *failure = Some(error.clone());
        }
        io::Error::new(error.kind(), SharedIo(error))
    });
    let _ = write.ack.send((write.packet, result));
    stopped
}
mod owned;
use owned::{owned_write, send_failure};
use zero_platform_tokio::contract::DeliveryState;

struct ReadCredit(NativeReadGrant);
impl Drop for ReadCredit {
    fn drop(&mut self) {
        self.0.revoke();
    }
}
