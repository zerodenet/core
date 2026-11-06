//! One actual Scope driver owns the unique read/write view; no packet pumps.
use super::*;
use std::{
    future::{poll_fn, Future},
    io,
    task::Poll,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::oneshot,
};
use zero_core::packet::{PacketIngress, ReturnAssociation};
use zero_platform_tokio::PollSender;

pub(super) async fn run(
    shared: Arc<Shared>,
    boot: oneshot::Sender<()>,
) -> Result<(), ChannelError> {
    let mut boot = Some(boot);
    let (grant, start) = loop {
        let changed = shared.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if shared.cancel.is_cancelled() {
            return Ok(());
        }
        {
            let mut state = shared.state.lock().unwrap();
            if state.io.is_some() && state.start.is_some() {
                break (state.io.take().unwrap(), state.start.take().unwrap());
            }
        }
        tokio::select! {
            _ = shared.cancel.cancelled() => return Ok(()),
            _ = poll_fn(|cx| {
                let polled = changed.as_mut().poll(cx);
                if polled.is_pending() {
                    if let Some(boot) = boot.take() { let _ = boot.send(()); }
                }
                polled
            }) => {}
        }
    };
    let Start {
        facts,
        observer,
        incoming,
        mut outgoing,
    } = start;
    if !grant.identity.same_provider(
        &shared
            .metadata
            .identity()
            .ok_or_else(|| error(Stage::Receive, Reason::StalePlan))?,
    ) || grant.identity.instance()
        != (ComponentInstanceId {
            component: facts.inbound.component,
            generation: facts.inbound.generation,
        })
    {
        return Err(error(Stage::Receive, Reason::StalePlan));
    }
    // The cookie binds the acquired native Source session, not a request or
    // individual read. A changing per-read cookie would erase real fragment
    // assembly and misrepresent subsequent native frames as another source.
    let correlation = grant.identity.instance().generation.0;
    let (mut read, mut write) = tokio::io::split(grant.io);
    let mut received = PollSender::new(incoming);
    let mut bytes = vec![0; 65_536];
    observer.peers(&[]);
    loop {
        if shared.cancel.is_cancelled() {
            break;
        }
        let receiving = shared.incoming.lock().unwrap().is_none();
        let sending = shared.outgoing.lock().unwrap().is_some();
        tokio::select! {
            _ = shared.cancel.cancelled() => break,
            result = poll_fn(|cx| {
                let mut buffer = ReadBuf::new(&mut bytes);
                let result = std::pin::Pin::new(&mut read).poll_read(cx, &mut buffer);
                if result.is_pending() || matches!(&result, Poll::Ready(Ok(()))) { observer.ready(); }
                result.map(|result| result.map(|()| buffer.filled().len()))
            }), if receiving => {
                let size = result.map_err(|source| {
                    observer.event(PacketPlane::Inner, None, PacketEvent::Error { identified: false });
                    error(Stage::Receive, Reason::Io).with_source(source)
                })?;
                if size == 0 { return Err(error(Stage::Receive, Reason::Io).with_source(io::Error::new(io::ErrorKind::UnexpectedEof, "original TUN read ended"))); }
                let association = ReturnAssociation { endpoint: facts.endpoint, generation: facts.inbound.generation, peer: None, correlation };
                let packet = std::mem::replace(&mut bytes, vec![0; 65_536]);
                let mut packet = packet;
                packet.truncate(size);
                observer.event(PacketPlane::Inner, None, PacketEvent::Received { identified: false, bytes: size });
                match IpPacket::new(packet.into(), association) {
                    Ok(packet) => {
                        let local_destination = zero_stack::packet::ip_destination(packet.payload()).is_some_and(|destination| shared.addresses.iter().any(|(address, _)| *address == destination));
                        *shared.incoming.lock().unwrap() = Some(packet.with_ingress(PacketIngress { local_destination, mtu: shared.mtu }));
                    }
                    Err(_) => dropped(&observer, zero_traits::PacketDropReason::InvalidPacket, 1),
                }
            }
            result = poll_fn(|cx| received.poll_reserve(cx)), if !receiving => {
                result.map_err(|source| error(Stage::Receive, Reason::Io).with_source(source))?;
                let packet = shared.incoming.lock().unwrap().take().ok_or_else(|| error(Stage::Receive, Reason::Internal))?;
                if let Err(returned) = received.send_item(packet) {
                    *shared.incoming.lock().unwrap() = returned.into_inner();
                    return Err(error(Stage::Receive, Reason::Revoked));
                }
            }
            packet = outgoing.recv(), if !sending => {
                let Some(packet) = packet else { break; };
                if packet.return_association.endpoint != facts.endpoint || packet.return_association.generation != facts.inbound.generation { return Err(error(Stage::Send, Reason::StalePlan)); }
                *shared.outgoing.lock().unwrap() = Some(packet);
            }
            result = poll_fn(|cx| {
                // Ownership remains in Shared even if this poll/future or the
                // entire native task is cancelled. The host releases it only
                // after consuming this same driver's actual exit.
                let packet = shared.outgoing.lock().unwrap();
                let packet = packet.as_ref().expect("actual queued original TUN frame");
                std::pin::Pin::new(&mut write).poll_write(cx, packet.payload())
            }), if sending => {
                let written = result.map_err(|source| {
                    observer.event(PacketPlane::Inner, None, PacketEvent::Error { identified: false });
                    error(Stage::Send, Reason::Io).with_source(source)
                })?;
                let expected = shared.outgoing.lock().unwrap().as_ref().unwrap().payload().len();
                if written != expected {
                    return Err(error(Stage::Send, Reason::Io).with_source(io::Error::new(io::ErrorKind::WriteZero, "original TUN packet write was partial")));
                }
                shared.outgoing.lock().unwrap().take();
                observer.event(PacketPlane::Inner, None, PacketEvent::Sent { bytes: written });
                observer.event(PacketPlane::Inner, None, PacketEvent::Responded { bytes: written });
            }
        }
    }
    dropped(
        &observer,
        zero_traits::PacketDropReason::QueueClosed,
        outgoing.len() as u64
            + u64::from(shared.incoming.lock().unwrap().is_some())
            + u64::from(shared.outgoing.lock().unwrap().is_some()),
    );
    // Queue drain is real view cleanup. Pending Shared originals remain until
    // the same actual Scope consumes this driver's exit, then Owner clears them.
    outgoing.close();
    while outgoing.try_recv().is_ok() {}
    Ok(())
}
fn dropped(observer: &Arc<dyn PacketObserver>, reason: zero_traits::PacketDropReason, count: u64) {
    if count != 0 {
        observer.event(
            PacketPlane::Inner,
            None,
            PacketEvent::Dropped {
                identified: false,
                reason,
                count,
            },
        );
    }
}
