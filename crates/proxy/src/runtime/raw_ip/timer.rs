//! Protocol-neutral scheduling. Exactly one deadline per live peer, with no
//! historical heap entries retained when traffic repeatedly rearms a timer.
use std::time::Duration;
use tokio::time::Instant;

/// Polling is a compatibility fallback, not a guessed protocol deadline.
#[derive(Clone, Copy)]
pub(crate) enum TimerSchedule {
    Polling,
    After(Duration),
    Parked,
}
impl TimerSchedule {
    pub(crate) fn deadline(self, previous: Option<Instant>) -> Option<Instant> {
        match self {
            Self::Polling => {
                let next = Instant::now() + Duration::from_millis(250);
                Some(previous.map_or(next, |old| old.min(next)))
            }
            Self::After(delay) => Some(Instant::now() + delay),
            Self::Parked => None,
        }
    }
}
impl From<Option<Duration>> for TimerSchedule {
    fn from(delay: Option<Duration>) -> Self {
        delay.map_or(Self::Parked, Self::After)
    }
}

#[derive(Default)]
pub(crate) struct PeerTimers {
    // Index-addressed min heap: rearming hot peers allocates no tree nodes.
    peers: Vec<Option<usize>>,
    ordered: Vec<(Instant, usize)>,
}

impl PeerTimers {
    pub(crate) fn rebuild(&mut self, count: usize, delay: impl Fn(usize) -> TimerSchedule) {
        self.peers.clear();
        self.peers.resize(count, None);
        self.ordered.clear();
        self.ordered.reserve(count);
        // Configuration contraction should release peak inventory storage.
        let retained = count.saturating_mul(4).max(16);
        if self.peers.capacity() > retained {
            self.peers.shrink_to(count);
        }
        if self.ordered.capacity() > retained {
            self.ordered.shrink_to(count);
        }
        for peer in 0..count {
            self.update(peer, delay(peer));
        }
    }

    pub(crate) fn update(&mut self, peer: usize, delay: TimerSchedule) {
        let Some(&position) = self.peers.get(peer) else {
            return;
        };
        let previous = position.map(|position| self.ordered[position].0);
        match (position, delay.deadline(previous)) {
            (Some(position), Some(deadline)) => {
                self.ordered[position].0 = deadline;
                self.reorder(position);
            }
            (None, Some(deadline)) => {
                let position = self.ordered.len();
                self.ordered.push((deadline, peer));
                self.peers[peer] = Some(position);
                self.reorder(position);
            }
            (Some(position), None) => self.remove(position),
            (None, None) => {}
        }
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.ordered.first().map(|(deadline, _)| *deadline)
    }

    pub(crate) fn pop_due(&mut self) -> Option<usize> {
        let &(deadline, peer) = self.ordered.first()?;
        if deadline > Instant::now() {
            return None;
        }
        self.remove(0);
        Some(peer)
    }

    fn remove(&mut self, position: usize) {
        let (_, peer) = self.ordered.swap_remove(position);
        self.peers[peer] = None;
        if let Some(&(_, peer)) = self.ordered.get(position) {
            self.peers[peer] = Some(position);
            self.reorder(position);
        }
    }

    fn swap(&mut self, a: usize, b: usize) {
        self.ordered.swap(a, b);
        self.peers[self.ordered[a].1] = Some(a);
        self.peers[self.ordered[b].1] = Some(b);
    }

    fn reorder(&mut self, mut position: usize) {
        if position > 0 && self.ordered[position] < self.ordered[(position - 1) / 2] {
            while position > 0 {
                let parent = (position - 1) / 2;
                if self.ordered[parent] <= self.ordered[position] {
                    break;
                }
                self.swap(parent, position);
                position = parent;
            }
        } else {
            loop {
                let left = position * 2 + 1;
                if left >= self.ordered.len() {
                    break;
                }
                let right = left + 1;
                let child =
                    if right < self.ordered.len() && self.ordered[right] < self.ordered[left] {
                        right
                    } else {
                        left
                    };
                if self.ordered[position] <= self.ordered[child] {
                    break;
                }
                self.swap(position, child);
                position = child;
            }
        }
    }
}

pub(crate) async fn wait(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests;
