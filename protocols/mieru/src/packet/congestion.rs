//! Mieru v3.33.0 CUBIC send-window state.
use tokio::time::Instant;

const BETA: f64 = 0.7;
const C: f64 = 0.4;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    SlowStart,
    Normal,
}

pub(super) struct Cubic {
    min_window: usize,
    max_window: usize,
    mode: Mode,
    window: usize,
    window_before_last_reduction: usize,
    last_reduction: Option<Instant>,
    accumulated_acks: usize,
}

impl Cubic {
    pub(super) fn new(min_window: usize, max_window: usize) -> Self {
        assert!(min_window > 0 && min_window <= max_window);
        Self {
            min_window,
            max_window,
            mode: Mode::SlowStart,
            window: min_window,
            window_before_last_reduction: 0,
            last_reduction: None,
            accumulated_acks: 0,
        }
    }

    pub(super) fn window_size(&self) -> usize {
        self.window
    }

    pub(super) fn on_ack(&mut self, now: Instant) {
        if self.mode == Mode::SlowStart {
            self.window = self.in_range(self.window.saturating_add(1));
            return;
        }

        self.accumulated_acks = self.accumulated_acks.saturating_add(1);
        let k = (self.window_before_last_reduction as f64 * (1.0 - BETA) / C).cbrt();
        let elapsed = now
            .duration_since(
                self.last_reduction
                    .expect("normal CUBIC has a reduction epoch"),
            )
            .as_secs_f64();
        let curve = C * (elapsed - k).powi(3) + self.window_before_last_reduction as f64;
        self.window =
            self.in_range((curve.max(0.0) as usize).saturating_add(self.accumulated_acks / 16));
    }

    pub(super) fn on_loss(&mut self, now: Instant) {
        self.mode = Mode::Normal;
        self.last_reduction = Some(now);
        self.window_before_last_reduction = self.window;
        self.accumulated_acks = 0;
        self.window = self.in_range((self.window as f64 * BETA) as usize);
    }

    pub(super) fn on_timeout(&mut self) {
        self.mode = Mode::SlowStart;
        self.window = self.min_window;
        self.window_before_last_reduction = 0;
        self.last_reduction = None;
        self.accumulated_acks = 0;
    }

    fn in_range(&self, window: usize) -> usize {
        window.clamp(self.min_window, self.max_window)
    }
}
