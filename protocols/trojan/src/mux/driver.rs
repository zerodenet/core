use std::{future::Future, pin::Pin};
pub type Driver = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

struct Exit<F: FnOnce()>(Option<F>);
impl<F: FnOnce()> Drop for Exit<F> {
    fn drop(&mut self) {
        if let Some(exit) = self.0.take() {
            exit();
        }
    }
}

/// Joining these protocol-owned futures spawns no child tasks. Dropping a
/// pending driver invalidates its logical channels; it proves no native close.
pub fn join<R, W, I, F>(read: R, write: W, idle: I, exit: F) -> Driver
where
    R: Future<Output = ()> + Send + 'static,
    W: Future<Output = ()> + Send + 'static,
    I: Future<Output = ()> + Send + 'static,
    F: FnOnce() + Send + 'static,
{
    // Install before the future is polled, including failed task registration.
    let exit = Exit(Some(exit));
    Box::pin(async move {
        let _exit = exit;
        tokio::join!(read, write, idle);
    })
}
