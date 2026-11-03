//! Serialized, bounded delayed datagram writes. Dropping the socket cancels them.
use super::*;
use crate::datagram_driver::{DriverStop, PreparedIoDriver};
use futures_util::{future::poll_fn, task::AtomicWaker};
use tokio::sync::mpsc;
use tokio_util::sync::PollSender;
struct Packet {
    bytes: Vec<u8>,
    peer: SocketAddr,
    ecn: Option<quinn::udp::EcnCodepoint>,
    src_ip: Option<std::net::IpAddr>,
}
#[derive(Default)]
struct Failure {
    error: Mutex<Option<(io::ErrorKind, String)>>,
    reader: AtomicWaker,
}
pub(super) struct Sender {
    queue: mpsc::Sender<Packet>,
    failure: Arc<Failure>,
    task: DriverStop,
}
impl Drop for Sender {
    fn drop(&mut self) {
        self.task.request();
    }
}
impl Sender {
    pub(super) fn new(inner: Arc<dyn AsyncUdpSocket>, codec: Codec) -> Self {
        let (mut sender, driver) = Self::prepare(inner, codec);
        sender.task = DriverStop::Legacy(
            tokio::spawn(async move {
                let _ = driver.future.await;
            })
            .abort_handle(),
        );
        sender
    }
    pub(super) fn prepare(
        inner: Arc<dyn AsyncUdpSocket>,
        mut codec: Codec,
    ) -> (Self, PreparedIoDriver) {
        let (queue, mut receiver) = mpsc::channel::<Packet>(16);
        let failure = Arc::new(Failure::default());
        let output = failure.clone();
        let driver = PreparedIoDriver::new(async move {
            let mut poller = inner.clone().create_io_poller();
            while let Some(packet) = receiver.recv().await {
                let result = async {
                    for emission in codec.encode_for(packet.peer, &packet.bytes)? {
                        let transmit = Transmit {
                            destination: packet.peer,
                            contents: &emission.packet,
                            ecn: packet.ecn,
                            segment_size: None,
                            src_ip: packet.src_ip,
                        };
                        let sent = loop {
                            match inner.try_send(&transmit) {
                                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                                    if let Err(error) =
                                        poll_fn(|cx| poller.as_mut().poll_writable(cx)).await
                                    {
                                        break Err(error);
                                    }
                                }
                                result => break result,
                            }
                        };
                        if emission.payload {
                            sent?;
                        }
                        if !emission.delay.is_zero() {
                            tokio::time::sleep(emission.delay).await;
                        }
                    }
                    Ok::<_, io::Error>(())
                }
                .await;
                codec.finish_noise(packet.peer);
                if let Err(error) = result {
                    *output.error.lock().unwrap() = Some((error.kind(), error.to_string()));
                    output.reader.wake();
                    return Err(error);
                }
            }
            Ok(())
        });
        (
            Self {
                queue,
                failure,
                task: DriverStop::Hosted(driver.cancel.clone()),
            },
            driver,
        )
    }
    pub(super) fn send(&self, transmit: &Transmit) -> io::Result<()> {
        self.check(None)?;
        if transmit.contents.len() > 65507 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "oversized FinalMask datagram",
            ));
        }
        let permit = self.queue.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => io::ErrorKind::WouldBlock,
            mpsc::error::TrySendError::Closed(_) => io::ErrorKind::BrokenPipe,
        })?;
        permit.send(Packet {
            bytes: transmit.contents.to_vec(),
            peer: transmit.destination,
            ecn: transmit.ecn,
            src_ip: transmit.src_ip,
        });
        Ok(())
    }
    pub(super) fn check(&self, cx: Option<&Context<'_>>) -> io::Result<()> {
        if let Some(cx) = cx {
            self.failure.reader.register(cx.waker());
        }
        match &*self.failure.error.lock().unwrap() {
            Some((kind, message)) => Err(io::Error::new(*kind, message.clone())),
            None => Ok(()),
        }
    }
    pub(super) fn poller(&self) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(Poller(PollSender::new(self.queue.clone())))
    }
}
struct Poller(PollSender<Packet>);
impl std::fmt::Debug for Poller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FinalMaskWritePoller")
    }
}
impl UdpPoller for Poller {
    fn poll_writable(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        std::task::ready!(self.0.poll_reserve(cx))
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        self.0.abort_send();
        Poll::Ready(Ok(()))
    }
}
