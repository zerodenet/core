//! One original logical session, retired by its actual accepted Scope.
use super::*;
use std::sync::Weak;

#[derive(Clone)]
pub(in crate::split_http) enum Source {
    Tcp {
        _original: tokio::sync::mpsc::Sender<super::super::XhttpStream>,
    },
    H3 {
        _original: tokio::sync::mpsc::Sender<zero_platform_tokio::TcpRelayStream>,
    },
}
pub(super) struct Original {
    map: Weak<Mutex<HashMap<String, Arc<Session>>>>,
    key: String,
    session: Arc<Session>,
    expires: tokio::time::Instant,
}
impl Original {
    pub(super) fn new(sessions: &Sessions, id: &str, session: Arc<Session>) -> Self {
        Self {
            map: Arc::downgrade(&sessions.0),
            key: id.into(),
            session,
            expires: tokio::time::Instant::now() + Duration::from_secs(30),
        }
    }
    pub(super) async fn run(self) {
        tokio::select! {
            _ = tokio::time::sleep_until(self.expires) => {
                let download = self.session.state.lock().unwrap().download;
                if download { self.session.life.cancelled().await; }
                else { self.session.life.fail("xhttp pending session expired"); }
            }
            _ = self.session.life.cancelled() => {}
        }
    }
}
impl Drop for Original {
    fn drop(&mut self) {
        if !self.session.life.is_closed() {
            self.session
                .life
                .fail("xhttp original session owner retired");
        }
        if let Some(map) = self.map.upgrade() {
            let mut map = map.lock().unwrap();
            if map
                .get(&self.key)
                .is_some_and(|session| Arc::ptr_eq(session, &self.session))
            {
                map.remove(&self.key);
            }
        }
        // Logical table/user retirement only, never native close or task join.
    }
}
pub(super) fn spawn_library(original: Original) {
    tokio::spawn(original.run());
}
