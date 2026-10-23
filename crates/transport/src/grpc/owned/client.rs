use super::{budget::Lease, state::Physical, *};
use crate::grpc::{prepared::PreparedConnection, prepared_stream::PreparedStream};
use bytes::Bytes;
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::sync::mpsc;
type Driver = Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>>;
type Worker = Pin<Box<dyn Future<Output = ()> + Send>>;
struct Work {
    read: Worker,
    write: Worker,
}
#[derive(Debug)]
struct WorkerAdmission(std::sync::Mutex<mpsc::error::TrySendError<Work>>);
impl std::fmt::Display for WorkerAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&*self.0.lock().unwrap(), f)
    }
}
impl std::error::Error for WorkerAdmission {}
struct Pump {
    physical: Arc<Physical>,
    driver: Driver,
    receiver: mpsc::Receiver<Work>,
    workers: FuturesUnordered<Worker>,
}
impl Drop for Pump {
    fn drop(&mut self) {
        self.physical
            .done
            .store(true, std::sync::atomic::Ordering::Release);
    }
}
impl Pump {
    async fn run(mut self) -> std::io::Result<()> {
        let Self {
            driver,
            receiver,
            workers,
            ..
        } = &mut self;
        let mut accepting = true;
        loop {
            tokio::select! {
                result=driver.as_mut()=>return result,
                command=receiver.recv(),if accepting=>match command {
                    Some(Work{read,write})=>workers.push(Box::pin(async move {tokio::join!(read,write);})),
                    None=>accepting=false,
                },
                _=workers.next(),if !workers.is_empty()=>{},
            }
        }
    }
}
pub(in crate::grpc) struct NativeClient {
    sender: h2::client::SendRequest<Bytes>,
    commands: mpsc::Sender<Work>,
    active: Arc<std::sync::atomic::AtomicUsize>,
    pub(super) life: Arc<Life>,
}
pub(super) struct Life {
    pub(super) physical: Arc<Physical>,
    pub(super) stop: CancellationToken,
}
impl Drop for Life {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
impl NativeClient {
    pub(super) fn prepare(
        connection: PreparedConnection,
        physical: Arc<Physical>,
        limit: usize,
        parent: &CancellationToken,
    ) -> (Arc<Self>, Driver) {
        let (commands, receiver) = mpsc::channel::<Work>(limit);
        let PreparedConnection {
            sender,
            active,
            driver,
        } = connection;
        let life = Arc::new(Life {
            physical: physical.clone(),
            stop: parent.child_token(),
        });
        let tasks = Box::pin(
            Pump {
                physical,
                driver,
                receiver,
                workers: FuturesUnordered::new(),
            }
            .run(),
        );
        (
            Arc::new(Self {
                sender,
                commands,
                active,
                life,
            }),
            tasks,
        )
    }
    pub(in crate::grpc) fn closed(&self) -> bool {
        self.commands.is_closed()
    }
    pub(super) async fn open(
        &self,
        owner: &Managed,
        demand: &Demand,
        lease: Lease,
    ) -> Result<NativeGrpcStream, ChannelError> {
        lease.bind(self.life.physical.clone())?;
        let ready = self.sender.clone().ready();
        let mut sender = owner
            .wait(demand, async {
                ready.await.map_err(|source| {
                    error(owner.instance, Stage::Handshake, Reason::Io).with_source(source)
                })
            })
            .await?;
        let request =
            crate::grpc::options::request(&owner.options.settings.grpc, &owner.options.authority)
                .map_err(|source| {
                error(owner.instance, Stage::Prepare, Reason::InvalidConfig).with_source(source)
            })?;
        lease.attempting();
        let (response, send) = sender.send_request(request, false).map_err(|source| {
            error(owner.instance, Stage::Handshake, Reason::Io).with_source(source)
        })?;
        let PreparedStream {
            mut stream,
            read,
            write,
        } = crate::grpc::prepared_stream::prepare_stream(
            send,
            response,
            owner.options.settings.grpc.multi_mode,
        );
        let read_record = lease.record.clone();
        let write_record = lease.record.clone();
        let read = Box::pin(async move {
            read.await;
            read_record.worker_done(true);
        });
        let write = Box::pin(async move {
            write.await;
            write_record.worker_done(false);
        });
        self.commands
            .try_send(Work { read, write })
            .map_err(|source| {
                error(owner.instance, Stage::Handshake, Reason::Revoked)
                    .with_source(WorkerAdmission(std::sync::Mutex::new(source)))
            })?;
        self.active
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        stream.active = Some(self.active.clone());
        Ok(NativeGrpcStream::new(stream, lease, self.life.clone()))
    }
}
