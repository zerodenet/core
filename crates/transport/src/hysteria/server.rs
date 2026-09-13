use super::*;
use std::sync::atomic::AtomicBool;
use tokio::sync::{mpsc, Mutex};
pub struct Incoming {
    pub(super) connection: quinn::Connection,
    driver: tokio::task::AbortHandle,
    dispatch: tokio::task::AbortHandle,
    pub(super) receiver: Mutex<mpsc::Receiver<HysteriaStream>>,
}
impl Drop for Incoming {
    fn drop(&mut self) {
        self.close();
    }
}
impl Incoming {
    pub async fn accept(&self) -> Option<HysteriaStream> {
        self.receiver.lock().await.recv().await
    }
    pub fn close(&self) {
        self.connection.close(0x100u32.into(), b"");
        self.driver.abort();
        self.dispatch.abort();
    }
}
pub fn accept_connection(connection: quinn::Connection, profile: &Profile) -> Incoming {
    let authenticated = Arc::new(AtomicBool::new(false));
    let (web_tx, web_rx) = mpsc::channel(128);
    let (data_tx, data_rx) = mpsc::channel(128);
    let dispatch = tokio::spawn(super::dispatch::run(
        connection.clone(),
        authenticated.clone(),
        web_tx,
        data_tx,
    ));
    let input = futures_util::stream::unfold(web_rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });
    let carrier = h3_quinn::Connection::with_incoming_bidi(connection.clone(), input);
    let profile = profile.clone();
    let owner = connection.clone();
    let task = tokio::spawn(async move {
        let mut builder = h3::server::builder();
        builder.max_field_section_size(65536);
        let Ok(mut server) = builder.build(carrier).await else {
            return;
        };
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                request=server.accept(),if tasks.len()<64=>{
                    let request=match request{Ok(Some(request))=>request,_=>break};
                    let profile=profile.clone();let authenticated=authenticated.clone();let connection=owner.clone();
                    tasks.spawn(async move {
                        let Ok(Ok((request,stream)))=tokio::time::timeout(std::time::Duration::from_secs(30),request.resolve_request()).await else {return;};
                        let _=super::request::serve(request,stream,&profile,&authenticated,&connection).await;

                    });
                }
                _=tasks.join_next(),if !tasks.is_empty()=>{}
                _=owner.closed()=>break,
            }
        }
        owner.close(0x100u32.into(), b"");
    });
    Incoming {
        connection,
        driver: task.abort_handle(),
        dispatch: dispatch.abort_handle(),
        receiver: Mutex::new(data_rx),
    }
}
