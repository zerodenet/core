//! The original one-task device loop, returning the original device for close.
use super::task::SharedIo;
use super::*;
use zero_stack::FragmentReassembler;
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
pub(super) async fn run<D: zero_tun::TunDevice + 'static>(
    device: D,
    mut rx: mpsc::Receiver<Write>,
    packets: Arc<PacketReturns>,
    sessions: Arc<session::Sessions>,
    mut stopped: watch::Receiver<bool>,
    mut activated: watch::Receiver<bool>,
) -> DriverIo<D> {
    let _completion = SessionCompletion(sessions.clone());
    while !*activated.borrow_and_update() {
        tokio::select! { biased; _ = stopped.changed() => return DriverIo::Stopped(device), result = activated.changed() => { if result.is_err() { return DriverIo::Stopped(device); } } }
    }
    let (mut reader, mut writer) = tokio::io::split(device);
    let mut failure = None;
    let mut buffer = vec![0; 65_536];
    let mut fragments = FragmentReassembler::new();
    let mut timer = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = stopped.changed() => break,
            result = reader.read(&mut buffer) => match result {
                Ok(0) => break,
                Err(error) => { failure = Some(Arc::new(error)); break; },
                Ok(n) => {
                    let mut original = std::mem::replace(&mut buffer, vec![0;65_536]);
                    original.truncate(n);
                    match fragments.process_buffer(original.into(), Instant::now()) {
                        zero_stack::OwnedFragmentOutcome::Packet { packet, .. } => {
                            if let Err(packet) = sessions.returns.deliver_owned(packet) { let _ = packets.deliver_owned(packet); }
                        }
                        zero_stack::OwnedFragmentOutcome::Pending | zero_stack::OwnedFragmentOutcome::Rejected(_) => packets.lose_receive_coverage(),
                    }
                },
            },
            write = rx.recv() => {
                let Some(write) = write else { break; };
                let stopped=match write {
                    Write::Legacy(write)=>legacy_write(write,&mut writer,&mut stopped,&mut failure).await,
                    Write::Owned(write)=>owned_write(write,&mut writer,&mut stopped,&mut failure).await,
                };
                sessions.acknowledge_controls();
                if stopped {break;}

            },
            _ = sessions.changed.notified() => sessions.acknowledge_controls(),
            _ = timer.tick() => {packets.expire();sessions.returns.expire();sessions.acknowledge_controls();},
        }
    }
    rx.close();
    while let Ok(write) = rx.try_recv() {
        match write {
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
use zero_platform_tokio::contract::{DeliveryState, Reason};
