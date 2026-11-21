//! Execute native acquisition in the original, already registered caller task.
use super::Invocation;
use std::{future::Future, pin::Pin};
use tokio::sync::{mpsc, oneshot};

type Job = Box<dyn FnOnce(Invocation) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;
pub(super) type Receiver = mpsc::Receiver<Job>;
pub(super) struct Sender(mpsc::Sender<Job>);

pub(super) fn channel() -> (Sender, Receiver) {
    let (send, receive) = mpsc::channel(1);
    (Sender(send), receive)
}
impl Sender {
    pub(super) async fn run<F, Fut, T>(&self, job: F) -> T
    where
        F: FnOnce(Invocation) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (send, receive) = oneshot::channel();
        self.0
            .send(Box::new(move |invocation| {
                Box::pin(async move {
                    let result = job(invocation).await;
                    assert!(send.send(result).is_ok(), "original caller result receiver");
                })
            }))
            .await
            .unwrap_or_else(|_| panic!("original caller exited before acquisition"));
        receive.await.expect("original caller result")
    }
}
