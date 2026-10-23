use std::net::TcpListener;

pub struct EchoListener {
    listener: TcpListener,
}
impl EchoListener {
    pub fn bind_avoiding(excluded: &[u16]) -> Self {
        loop {
            let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind owned echo listener");
            let port = listener.local_addr().expect("echo local address").port();
            if !excluded.contains(&port) {
                return Self { listener };
            }
        }
    }
    pub fn port(&self) -> u16 {
        self.listener
            .local_addr()
            .expect("echo local address")
            .port()
    }
    pub fn into_listener(self) -> TcpListener {
        self.listener
    }
}
