use super::*;

pub(super) async fn run_mux_read_relay_with_shutdown<R>(
    mut reader: R,
    streams: Arc<Mutex<std::collections::HashMap<u16, mpsc::Sender<VmessMuxDownlink>>>>,
    closed: Arc<AtomicBool>,
    shutdown_tx: watch::Sender<bool>,
    mut shutdown_rx: watch::Receiver<bool>,
    activity_tx: Option<watch::Sender<tokio::time::Instant>>,
    response_backlog: MuxResponseBacklog,
) where
    R: AsyncRead + Unpin + Send + 'static,
{
    loop {
        let frame = tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() { break; }
                continue;
            }
            frame = response::read_buffered(&mut reader, &response_backlog, |frame| streams.lock().unwrap().get(&frame.session_id).cloned()) => {
                match frame { Ok(frame) => frame, Err(_) => break }
            }
        };
        if let Some(activity_tx) = &activity_tx {
            activity_tx.send_replace(tokio::time::Instant::now());
        }
        let (frame, tx) = frame;
        let (sid, message) = match frame {
            response::BufferedFrame::Overflow(sid) => (sid, VmessMuxDownlink::Overflow),
            response::BufferedFrame::Frame(frame) => {
                let sid = frame.value().session_id;
                // Keep the existing validation of NEW's actual target fields.
                let event = frame.map(MuxFrame::try_into_server_event);
                match event.value() {
                    Ok(VmessMuxServerEvent::KeepAlive) => continue,
                    Ok(VmessMuxServerEvent::End { .. } | VmessMuxServerEvent::Unknown { .. }) => {
                        if let Some(tx) = &tx {
                            let mut streams = streams.lock().unwrap();
                            if streams
                                .get(&sid)
                                .is_some_and(|current| current.same_channel(tx))
                            {
                                streams.remove(&sid);
                            }
                        }
                        continue;
                    }
                    Err(_) => break,
                    _ => {}
                }
                let data = event.map(|event| match event.expect("validated actual MUX event") {
                    VmessMuxServerEvent::Data { payload, .. }
                    | VmessMuxServerEvent::NewStream { payload, .. } => payload,
                    _ => unreachable!("original DATA event"),
                });
                if data.value().is_empty() {
                    continue;
                }
                (sid, VmessMuxDownlink::Data(data))
            }
        };
        let Some(tx) = tx else {
            continue;
        };
        let rejected = matches!(&message, VmessMuxDownlink::Overflow);
        let delivered = tokio::select! { biased;
            _ = shutdown_rx.changed() => break,
            result = tx.send(message) => result.is_ok(),
        };
        if rejected || !delivered {
            let mut streams = streams.lock().unwrap();
            if streams
                .get(&sid)
                .is_some_and(|current| current.same_channel(&tx))
            {
                streams.remove(&sid);
            }
        }
    }
    closed.store(true, Ordering::Release);
    let _ = shutdown_tx.send(true);
    streams.lock().unwrap().clear();
}
