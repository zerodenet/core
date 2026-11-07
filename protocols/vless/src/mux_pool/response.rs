use super::*;

pub(super) async fn run_mux_read_relay<R>(
    mut reader: R,
    streams: Arc<Mutex<HashMap<u16, MuxClientStreamState>>>,
    closed: Arc<AtomicBool>,
    shutdown: (watch::Sender<bool>, watch::Receiver<bool>),
    activity_tx: Option<watch::Sender<tokio::time::Instant>>,
    response_backlog: MuxResponseBacklog,
    response_pending: bool,
) where
    R: AsyncRead + Unpin + Send + 'static,
{
    let (shutdown_tx, mut shutdown_rx) = shutdown;
    if response_pending {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    closed.store(true, Ordering::Release);
                    let _ = shutdown_tx.send(true);
                    streams.lock().unwrap().clear();
                    return;
                }
            }
            response = crate::udp::read_vless_response_tokio(&mut reader) => {
                if response.is_err() {
                    closed.store(true, Ordering::Release);
                    let _ = shutdown_tx.send(true);
                    streams.lock().unwrap().clear();
                    return;
                }
            }
        }
    }
    loop {
        let received = tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() { break; }
                continue;
            }
            frame = crate::mux::codec::read_buffered_frame(&mut reader, &response_backlog, |frame| {
                let mut streams = streams.lock().unwrap();
                let state = streams.get_mut(&frame.session_id)?;
                if let Some(target) = &frame.target { state.target = Some((target.address.clone(), target.port)); }
                Some((state.downlink.clone(), state.target.clone()))
            }) => {
                match frame { Ok(frame) => frame, Err(_) => break }
            }
        };
        if let Some(activity_tx) = &activity_tx {
            activity_tx.send_replace(tokio::time::Instant::now());
        }
        let (received, recipient) = received;
        let (sid, downlink) = match received {
            crate::mux::codec::BufferedFrame::Overflow(sid) => {
                (sid, recipient.map(|(tx, _)| (tx, None)))
            }
            crate::mux::codec::BufferedFrame::Frame(frame) => {
                let sid = frame.value().session_id;
                if sid == 0 {
                    continue;
                }
                if frame.value().status == crate::mux::STATUS_END {
                    if let Some((tx, _)) = recipient {
                        let mut streams = streams.lock().unwrap();
                        if streams
                            .get(&sid)
                            .is_some_and(|state| state.downlink.same_channel(&tx))
                        {
                            streams.remove(&sid);
                        }
                    }
                    continue;
                }
                if frame.value().options & crate::mux::OPTION_DATA == 0 {
                    continue;
                }
                let tx = match recipient {
                    Some((MuxClientDownlink::Tcp(tx), _)) => {
                        if frame.value().payload.is_empty() {
                            continue;
                        }
                        Some((
                            MuxClientDownlink::Tcp(tx),
                            Some(Response::Tcp(frame.map(|frame| frame.payload))),
                        ))
                    }
                    Some((MuxClientDownlink::Udp(tx), Some((target, port)))) => Some((
                        MuxClientDownlink::Udp(tx),
                        Some(Response::Udp(frame.map(|frame| {
                            UdpFlowPacket::new(target, port, frame.payload)
                        }))),
                    )),
                    _ => None,
                };
                (sid, tx)
            }
        };
        let Some((tx, data)) = downlink else {
            continue;
        };
        let rejected = data.is_none();
        let send = async {
            match (&tx, data) {
                (MuxClientDownlink::Tcp(tx), Some(Response::Tcp(data))) => {
                    tx.send(MuxDownlink::Data(data)).await.is_ok()
                }
                (MuxClientDownlink::Udp(tx), Some(Response::Udp(data))) => {
                    tx.send(MuxDownlink::Data(data)).await.is_ok()
                }
                (MuxClientDownlink::Tcp(tx), None) => tx.send(MuxDownlink::Overflow).await.is_ok(),
                (MuxClientDownlink::Udp(tx), None) => tx.send(MuxDownlink::Overflow).await.is_ok(),
                _ => unreachable!("same original downlink"),
            }
        };
        let delivered = tokio::select! { biased;
            _ = shutdown_rx.changed() => break,
            delivered = send => delivered,
        };
        if rejected || !delivered {
            let mut streams = streams.lock().unwrap();
            if streams
                .get(&sid)
                .is_some_and(|state| state.downlink.same_channel(&tx))
            {
                streams.remove(&sid);
            }
        }
    }
    closed.store(true, Ordering::Release);
    let _ = shutdown_tx.send(true);
    streams.lock().unwrap().clear();
}

enum Response {
    Tcp(BufferedMuxResponse<Vec<u8>>),
    Udp(BufferedMuxResponse<UdpFlowPacket>),
}

impl MuxClientDownlink {
    fn same_channel(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Tcp(a), Self::Tcp(b)) => a.same_channel(b),
            (Self::Udp(a), Self::Udp(b)) => a.same_channel(b),
            _ => false,
        }
    }
}
