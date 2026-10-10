// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

use super::*;

impl<R: rand::RngCore + Send> Tunn<R> {
    /// Time until `update_timers` next has work, using the protocol's clock and
    /// already sampled jitter. `None` means only packet I/O can revive timers.
    /// Recompute after packet I/O, timer execution or timer configuration changes.
    /// The caller owns sleeping; this method neither runs nor resamples timers.
    pub fn next_timer_delay(&self) -> Option<Duration> {
        if self.handshake.is_expired() {
            return None;
        }
        let now = self.timers.now();
        let established = self.timers[TimeSessionEstablished];
        let mut delay = (established + REJECT_AFTER_TIME * 3).saturating_sub(now);
        let mut include = |deadline: Duration| {
            delay = delay.min(deadline.saturating_sub(now));
        };
        // update_session_timers uses a strict comparison. Only occupied slots
        // need wiping; including empty slots would cause useless periodic wakes.
        for (slot, session) in self.sessions.iter().enumerate() {
            if session.is_some() {
                include(
                    self.timers.session_timers[slot] + REJECT_AFTER_TIME + Duration::from_nanos(1),
                );
            }
        }
        if self.handshake.has_cookie() {
            include(self.timers[TimeCookieReceived] + COOKIE_EXPIRATION_TIME);
        }
        if let Some(sent) = self.handshake.timer() {
            include(self.timers[TimeLastHandshakeStarted] + REKEY_ATTEMPT_TIME);
            return Some(delay.min(self.timers.rekey_timeout.saturating_sub(sent.elapsed())));
        }
        if self.timers.is_initiator() {
            if established < self.timers[TimeLastDataPacketSent] {
                include(established + self.timers.rekey_after_time);
            }
            if established < self.timers[TimeLastDataPacketReceived] {
                include(established + REJECT_AFTER_TIME - KEEPALIVE_TIMEOUT - REKEY_TIMEOUT);
            }
        }
        if let Some(since) = self.timers.want_handshake {
            include(since + self.timers.new_handshake_timeout);
        }
        if let Some(since) = self.timers.want_keepalive {
            include(since + self.timers.keepalive_timeout);
        }
        if let Some(interval) = self.timers.persistent_keepalive {
            if self.timers.persistent_keepalive_due {
                return Some(Duration::ZERO);
            }
            include(self.timers[TimePersistentKeepalive] + interval);
        }
        Some(delay)
    }
}

#[cfg(all(test, feature = "mock_instant"))]
#[path = "../../../tests/timer_deadline.rs"]
mod tests;
