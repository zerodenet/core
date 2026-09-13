use super::{
    config::Settings,
    receive::Receiver,
    rtt::RoundTrip,
    send::Sender,
    wire::{Body, Segment, CLOSE, PING, TERMINATE},
};
use bytes::Bytes;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Active,
    ReadyToClose,
    PeerClosed,
    Terminating,
    PeerTerminating,
    Terminated,
}
pub(super) struct Connection {
    conv: u16,
    settings: Settings,
    phase: Phase,
    phase_since: u32,
    last_incoming: u32,
    last_ping: u32,
    pub sender: Sender,
    pub receiver: Receiver,
    rtt: RoundTrip,
}
impl Connection {
    pub fn new(conv: u16, settings: Settings) -> Self {
        Self {
            conv,
            settings,
            phase: Phase::Active,
            phase_since: 0,
            last_incoming: 0,
            last_ping: 0,
            sender: Sender::new(settings),
            receiver: Receiver::new(settings.receive_window()),
            rtt: RoundTrip::new(settings.tti_ms),
        }
    }
    fn set_phase(&mut self, phase: Phase, now: u32) {
        self.phase = phase;
        self.phase_since = now;
        if matches!(
            phase,
            Phase::PeerClosed | Phase::Terminating | Phase::PeerTerminating | Phase::Terminated
        ) {
            self.sender.close();
        }
    }
    pub fn writable(&self) -> bool {
        self.phase == Phase::Active && self.sender.can_push()
    }
    pub fn can_read(&self) -> bool {
        matches!(
            self.phase,
            Phase::Active | Phase::PeerClosed | Phase::PeerTerminating
        )
    }
    pub fn finished(&self) -> bool {
        self.phase == Phase::Terminated
    }
    pub fn write(&mut self, payload: Bytes) {
        self.sender.push(payload);
    }
    pub fn close(&mut self, now: u32) {
        let next = match self.phase {
            Phase::Active => Phase::ReadyToClose,
            Phase::PeerClosed => Phase::Terminating,
            Phase::PeerTerminating => Phase::Terminated,
            _ => return,
        };
        self.set_phase(next, now);
    }
    pub fn input(&mut self, mut bytes: &[u8], now: u32) {
        while !bytes.is_empty() {
            let Ok(segment) = Segment::decode(&mut bytes) else {
                break;
            };
            if segment.conv != self.conv {
                break;
            }
            self.last_incoming = now;
            if segment.option & CLOSE != 0 {
                match self.phase {
                    Phase::ReadyToClose => self.set_phase(Phase::Terminating, now),
                    Phase::Active => self.set_phase(Phase::PeerClosed, now),
                    _ => {}
                }
            }
            match segment.body {
                Body::Data {
                    timestamp,
                    number,
                    sending_next,
                    payload,
                } => self
                    .receiver
                    .input(timestamp, number, sending_next, payload),
                Body::Ack {
                    window,
                    next,
                    timestamp,
                    numbers,
                } => {
                    if let Some(rtt) =
                        self.sender
                            .ack(window, next, timestamp, &numbers, now, self.rtt.rto)
                    {
                        self.rtt.update(rtt, now);
                    }
                }
                Body::Command {
                    command,
                    sending_next,
                    receiving_next,
                    rto,
                } => {
                    if command == TERMINATE {
                        match self.phase {
                            Phase::Active | Phase::PeerClosed => {
                                self.set_phase(Phase::PeerTerminating, now)
                            }
                            Phase::ReadyToClose => self.set_phase(Phase::Terminating, now),
                            Phase::Terminating => self.set_phase(Phase::Terminated, now),
                            _ => {}
                        }
                    }
                    self.sender.clear(receiving_next);
                    self.receiver.clear(sending_next);
                    self.rtt.peer(rto, now);
                }
            }
        }
    }
    fn command(&mut self, command: u8, now: u32) -> Body {
        self.last_ping = now;
        Body::Command {
            command,
            sending_next: self.sender.una(),
            receiving_next: self.receiver.next,
            rto: self.rtt.rto,
        }
    }
    pub fn flush(&mut self, now: u32) -> Vec<Vec<u8>> {
        if self.finished() {
            return Vec::new();
        }
        if self.phase == Phase::Active && now.wrapping_sub(self.last_incoming) >= 30000 {
            self.close(now);
        }
        if self.phase == Phase::ReadyToClose && self.sender.is_empty() {
            self.set_phase(Phase::Terminating, now);
        }
        if self.phase == Phase::Terminating {
            let emit = now == self.phase_since || now.wrapping_sub(self.last_ping) >= 1000;
            let command = if emit {
                Some(self.command(TERMINATE, now))
            } else {
                None
            };
            if now.wrapping_sub(self.phase_since) > 8000 {
                self.set_phase(Phase::Terminated, now);
            }
            return command
                .into_iter()
                .map(|body| {
                    Segment {
                        conv: self.conv,
                        option: 0,
                        body,
                    }
                    .encode()
                })
                .collect();
        }
        if (self.phase == Phase::PeerTerminating && now.wrapping_sub(self.phase_since) > 4000)
            || (self.phase == Phase::ReadyToClose && now.wrapping_sub(self.phase_since) > 15000)
        {
            self.set_phase(Phase::Terminating, now);
        }
        let mut bodies = self
            .receiver
            .flush(now, self.rtt.rto, self.settings.mtu as usize);
        let (data, ping) = self.sender.flush(now, self.rtt.rto);
        bodies.extend(data);
        if ping || now.wrapping_sub(self.last_ping) >= 3000 {
            bodies.push(self.command(PING, now));
        }
        let option = if self.phase == Phase::ReadyToClose {
            CLOSE
        } else {
            0
        };
        bodies
            .into_iter()
            .map(|body| {
                Segment {
                    conv: self.conv,
                    option,
                    body,
                }
                .encode()
            })
            .collect()
    }
}
