// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

use super::*;

impl<R: rand::RngCore + Send> Tunn<R> {
    #[cfg(feature = "zero-owned-packets")]
    pub(in crate::noise) fn prepared_data_session_live(&self, slot: usize, now: Duration) -> bool {
        !self.handshake.is_expired()
            && now.saturating_sub(self.timers.session_timers[slot % N_SESSIONS])
                <= REJECT_AFTER_TIME
            && now.saturating_sub(self.timers[TimeSessionEstablished]) < REJECT_AFTER_TIME * 3
            && !(self.handshake.timer().is_some()
                && now.saturating_sub(self.timers[TimeLastHandshakeStarted]) >= REKEY_ATTEMPT_TIME)
    }

    /// Shared by timer execution and public crypto I/O. This only expires state;
    /// it cannot emit (and then lose) a retry/rekey packet before processing I/O.
    /// It also stamps packet-driven transitions at their actual owner-clock time.
    pub(in crate::noise) fn refresh_expiration(&mut self) -> bool {
        self.refresh_expiration_at(self.timers.now())
    }
    pub(in crate::noise) fn refresh_expiration_at(&mut self, now: Duration) -> bool {
        self.timers[TimeCurrent] = now;
        self.update_session_timers(now);
        if self.handshake.is_expired() {
            return true;
        }
        if self.handshake.has_cookie()
            && now.saturating_sub(self.timers[TimeCookieReceived]) >= COOKIE_EXPIRATION_TIME
        {
            self.handshake.clear_cookie();
        }
        let old_keys =
            now.saturating_sub(self.timers[TimeSessionEstablished]) >= REJECT_AFTER_TIME * 3;
        let attempts_exhausted = self.handshake.timer().is_some()
            && now.saturating_sub(self.timers[TimeLastHandshakeStarted]) >= REKEY_ATTEMPT_TIME;
        if old_keys || attempts_exhausted {
            tracing::debug!(old_keys, attempts_exhausted, "CONNECTION_EXPIRED");
            self.handshake.set_expired();
            self.clear_all();
            return true;
        }
        false
    }
}
