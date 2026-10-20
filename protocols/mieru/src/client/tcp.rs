use super::{driver::DriverHandle, ClientConnection, OpenRequest};
use crate::{
    inbound::multiplex::{
        reader::{read_segment, Reader},
        writer::{self, Command},
    },
    metadata::*,
    traffic_pattern::TrafficPattern,
    MieruOutbound,
};
use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
use zero_traits::AsyncSocket;

impl ClientConnection {
    #[cfg(feature = "runtime")]
    pub(crate) async fn tcp_managed<S>(
        socket: S,
        username: &str,
        password: &str,
        options: &mieru_config::MieruTransportOptions,
        lease: super::pool::managed::CarrierLease,
    ) -> io::Result<Arc<Self>>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        use zero_platform_tokio::contract::{
            lifecycle::TaskClass, ChannelError, Impact, Reason, Stage,
        };
        let owner = lease.owner.clone();
        let (socket, source) = super::native_io::Capture::new(socket);
        let prepared = tokio::select! {biased;
            _=owner.cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
            _=owner.scope_cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
            result=prepare(socket, username, password, options)=>result.map_err(|error|source.recover(error))?,
        };
        if !owner.ready() {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let driver = super::driver::ManagedDriver::new(owner.cancel.child_token());
        let completion = driver.completion();
        let stop = driver.cancel.clone();
        let scope_stop = owner.scope_cancel.clone();
        let component = owner.instance.component;
        let registration = lease.registering();
        let (_, witness)=owner.tasks.spawn_tracked(TaskClass::Critical,async move {
            let _lease=lease;
            let _completion=completion;
            tokio::select! {biased;
                _=stop.cancelled()=>Ok(()),
                _=scope_stop.cancelled()=>Ok(()),
                result=prepared.driver=>result.map_err(|source|{
                    let mut error=ChannelError::new(Stage::Receive,Reason::Io,Impact::Component).with_source(source);
                    error.component=Some(component); error
                }),
            }
        }).map_err(io::Error::other)?;
        driver.registered(witness.clone());
        registration.registered(witness);
        Ok(Self::from_prepared(
            prepared.first,
            prepared.opens,
            DriverHandle::Managed(driver),
        ))
    }

    pub async fn tcp<S>(socket: S, username: &str, password: &str) -> io::Result<Arc<Self>>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Unpin + 'static,
    {
        Self::tcp_with_options(
            socket,
            username,
            password,
            &mieru_config::MieruTransportOptions::default(),
        )
        .await
    }

    pub async fn tcp_with_options<S>(
        socket: S,
        username: &str,
        password: &str,
        options: &mieru_config::MieruTransportOptions,
    ) -> io::Result<Arc<Self>>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Unpin + 'static,
    {
        let prepared = prepare(socket, username, password, options).await?;
        let task = tokio::spawn(async move {
            let _ = prepared.driver.await;
        });
        Ok(Self::from_prepared(
            prepared.first,
            prepared.opens,
            DriverHandle::Legacy(task),
        ))
    }

    pub(super) fn from_prepared(
        first: crate::inbound::multiplex::stream::MieruLogicalStream,
        opens: mpsc::Sender<OpenRequest>,
        task: DriverHandle,
    ) -> Arc<Self> {
        Arc::new(Self {
            first: Mutex::new(Some(first)),
            opens,
            task,
            active_streams: std::sync::atomic::AtomicUsize::new(0),
            pending_opens: std::sync::atomic::AtomicUsize::new(0),
            max_streams: 256,
            created_at: std::time::Instant::now(),
        })
    }
}

pub(super) struct Prepared {
    first: crate::inbound::multiplex::stream::MieruLogicalStream,
    opens: mpsc::Sender<OpenRequest>,
    driver: std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>,
}

async fn prepare<S>(
    mut socket: S,
    username: &str,
    password: &str,
    options: &mieru_config::MieruTransportOptions,
) -> io::Result<Prepared>
where
    S: AsyncSocket + AsyncRead + AsyncWrite + Unpin + 'static,
{
    options.validate().map_err(io::Error::other)?;
    let pattern =
        TrafficPattern::from_config(options.traffic_pattern.as_ref()).map_err(io::Error::other)?;
    let initial = tokio::time::timeout(
        Duration::from_secs(15),
        MieruOutbound::connect_with_pattern(&mut socket, username, password, &pattern),
    )
    .await
    .map_err(|_| io::ErrorKind::TimedOut)?
    .map_err(io::Error::other)?;
    let id = initial.mieru_session.session_id;
    let (ready, _unused) = mpsc::channel(1);
    let (outgoing, commands) = mpsc::channel(64);
    let (dropped, drop_events) = mpsc::unbounded_channel();
    let mut reader = Reader::new(ready, outgoing, dropped, options.receive.clone());
    let first = reader.create(id, Vec::new())?;
    first.status.mark_open();
    let (opens, requests) = mpsc::channel(64);
    let (read, write) = tokio::io::split(socket);
    let writer_username = username.to_owned();
    let driver = Box::pin(async move {
        tokio::select! {
            result = read_loop(reader, read, initial.server_cipher, id, requests, drop_events) => result,
            result = writer::run_direction_with_pattern(
                write, initial.client_cipher, id, commands, true, pattern, writer_username
            ) => result,
        }
    });
    Ok(Prepared {
        first,
        opens,
        driver,
    })
}
async fn read_loop<R: AsyncRead + Unpin>(
    mut reader: Reader,
    mut socket: R,
    mut cipher: crate::crypto::MieruCipher,
    mut id: u32,
    mut opens: mpsc::Receiver<OpenRequest>,
    mut dropped: mpsc::UnboundedReceiver<(u32, Arc<crate::inbound::multiplex::stream::Status>)>,
) -> io::Result<()> {
    let mut buffer = Vec::new();
    let mut idle = tokio::time::interval(Duration::from_secs(60));
    let mut drain = tokio::time::interval(Duration::from_millis(10));
    drain.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    idle.tick().await;
    drain.tick().await;
    loop {
        tokio::select! {
            request = opens.recv() => {
                let Some(request) = request else { return Ok(()); };
                if reader.sessions.len() >= 256 {
                    let _ = request.send(Err(io::ErrorKind::WouldBlock.into())); continue;
                }
                id = id.checked_add(1).filter(|id| *id != 0).ok_or_else(|| io::Error::other("mieru session IDs exhausted"))?;
                let stream = reader.create(id, Vec::new())?;
                reader.outgoing.send(Command::OpenClient(id)).await.map_err(|_| io::ErrorKind::BrokenPipe)?;
                let _ = request.send(Ok(stream));
            }
            Some((id, status)) = dropped.recv() => {
                if reader.sessions.get(&id).is_some_and(|entry| Arc::ptr_eq(&entry.status, &status)) {
                    let entry = reader.remove_session(id).unwrap();
                    if !entry.status.closed() {
                        entry.status.close(None);
                        reader.outgoing.send(Command::Close { id, response: false }).await.map_err(|_| io::ErrorKind::BrokenPipe)?;
                    }
                }
            }
            segment = read_segment(&mut socket, &mut buffer, &mut cipher) => {
                let Some(mut segment) = segment? else { return Ok(()); };
                if let Some(meta) = segment.session_meta.as_ref() {
                    if meta.protocol_type == OPEN_SESSION_RESPONSE {
                        if meta.sequence_number!=0 {return Err(io::Error::other("mieru invalid open sequence"));}
                        if let Some(entry)=reader.sessions.get(&meta.session_id) {entry.status.mark_open();}
                        if meta.status_code != 0 {
                            if let Some(entry) = reader.remove_session(meta.session_id) {entry.status.close(Some(io::ErrorKind::PermissionDenied));}
                        }
                        continue;
                    }
                    if !matches!(meta.protocol_type, CLOSE_SESSION_REQUEST | CLOSE_SESSION_RESPONSE) {return Err(io::Error::other("mieru invalid server control"));}
                }
                if let Some(meta) = segment.data_meta.as_mut() {
                    meta.protocol_type = match meta.protocol_type {
                        DATA_SERVER_TO_CLIENT => DATA_CLIENT_TO_SERVER,
                        ACK_SERVER_TO_CLIENT => ACK_CLIENT_TO_SERVER,
                        _ => return Err(io::Error::other("mieru invalid server data")),
                    };
                }
                reader.dispatch(segment).await?;
            }
            _ = drain.tick() => reader.drain_pending().await?,
            _ = idle.tick() => { if reader.sessions.is_empty() { return Ok(()); } }
        }
    }
}
