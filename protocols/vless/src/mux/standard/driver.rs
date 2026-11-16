//! One actual task drives both halves without cancelling partially read frames.
use super::*;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
pub(super) async fn run<S>(
    stream: S,
    mut server: VlessInboundMuxServer,
    opened: mpsc::Sender<Request>,
    stop: CancellationToken,
    facts: InboundConnectionFacts,
    uuid: [u8; 16],
    mut start: Option<crate::reverse::WorkerAttempt>,
) -> Result<(), ChannelError>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let _stopped = stop.clone().drop_guard();
    let (mut reader, writer) = tokio::io::split(stream);
    let mut writer = Writer(std::sync::Mutex::new(writer));
    loop {
        if let Some(reverse) = &server.reverse {
            reverse.connections(server.streams.streams.len());
        }
        server.writer.retry_end_requests();
        let endings = server.writer.end_changed.clone();
        let frame = {
            // This exact future survives every downlink while its header or body
            // is incomplete. Only shutdown may drop it together with the carrier.
            let next = codec::read_frame_tokio_mode(&mut reader, server.reverse.is_some());
            tokio::pin!(next);
            loop {
                tokio::select! {biased;
                    _=stop.cancelled()=>return Ok(()),
                    frame=&mut next=>break frame.map_err(datagram::failure)?,
                    _=endings.notified()=>server.writer.retry_end_requests(),
                    Some(downlink)=server.down_rx.recv()=>{
                        server.streams.send_inbound_downlink(&mut server.mux,&mut writer,downlink.into_inner()).await.map_err(datagram::failure)?;
                        server.writer.retry_end_requests();
                    }
                }
            }
        };
        let origin = frame.origin.clone();
        let action: VlessInboundMuxAction = MuxServer::frame_event(frame)
            .map_err(datagram::failure)?
            .into();
        // This first complete frame has crossed the same IO's real Rvs
        // response. Until then the original serialized attempt stays retained.
        drop(start.take());
        if let Some(reverse) = &mut server.reverse {
            if reverse.consume(&action).map_err(datagram::failure)? {
                continue;
            }
        }
        if matches!(&action,VlessInboundMuxAction::OpenStream{session,..} if session.port==0) {
            return Err(datagram::failure(Error::Protocol(
                "MUX application target port must not be 0",
            )));
        }
        if server.udp_only
            && matches!(&action,VlessInboundMuxAction::OpenStream{session,..} if session.network==Network::Tcp)
        {
            return Err(datagram::failure(Error::Unsupported(
                "Vision MUX only supports UDP",
            )));
        }
        if let Some(stream) = server
            .streams
            .apply_inbound_action(&mut server.mux, &mut writer, action)
            .await
            .map_err(datagram::failure)?
        {
            let mut facts = facts.clone();
            if let Some(origin) = origin {
                origin.apply_facts(&mut facts);
            }
            let route = stream.into_route_with_auth(server.auth.as_ref(), server.writer.clone());
            if let Err(rejected) = opened.try_send(Request { uuid, route, facts }) {
                match rejected.into_inner().route.state {
                    VlessInboundMuxOpenedRouteState::Tcp { relay, .. } => {
                        server.streams.close_inbound_stream(relay.session_id, true);
                        server
                            .mux
                            .end_inbound_stream(&mut writer, relay.session_id)
                            .await
                            .map_err(datagram::failure)?;
                    }
                    VlessInboundMuxOpenedRouteState::Udp { relay } => {
                        server.streams.close_inbound_stream(relay.session_id, true);
                        server
                            .mux
                            .end_inbound_stream(&mut writer, relay.session_id)
                            .await
                            .map_err(datagram::failure)?;
                    }
                }
            }
        }
        let output = server.writer.clone();
        for (sid, state) in &mut server.streams.streams {
            if state
                .upload
                .as_ref()
                .is_some_and(|upload| upload.is_closed())
                && !state.ending
            {
                // Retain metadata until END reaches the same FIFO behind all
                // previously accepted data. Removing it here would discard END.
                state.ending = output.end_inbound_stream(*sid).is_ok();
            }
        }
    }
}
// Only this actual driver polls the writer. Exclusive get_mut satisfies the
// old AsyncSocket Sync boundary without a lock across await or a second task.
struct Writer<W>(std::sync::Mutex<W>);
impl<W: AsyncWrite + Send + Unpin> AsyncSocket for Writer<W> {
    type Error = std::io::Error;
    async fn read(&mut self, _: &mut [u8]) -> Result<usize, Self::Error> {
        Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
    }
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        let writer = self.0.get_mut().expect("exclusive MUX writer");
        writer.write_all(bytes).await?;
        writer.flush().await
    }
    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        self.0
            .get_mut()
            .expect("exclusive MUX writer")
            .shutdown()
            .await
    }
}
