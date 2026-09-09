//! Three-estimate windowed maximum (Kathleen Nichols' algorithm).
#[derive(Clone, Copy, Default)]
pub(super) struct Entry<T: Copy + Default> {
    pub value: T,
    pub round: u64,
}
#[derive(Clone)]
pub(super) struct Filter<T: Copy + Default> {
    pub entries: [Entry<T>; 3],
    key: fn(T) -> u64,
}
impl<T: Copy + Default> Filter<T> {
    pub fn new(key: fn(T) -> u64) -> Self {
        Self {
            entries: [Entry::default(); 3],
            key,
        }
    }
    pub fn best(&self) -> T {
        self.entries[0].value
    }
    pub fn reset(&mut self, value: T, round: u64) {
        self.entries = [Entry { value, round }; 3];
    }
    pub fn update(&mut self, value: T, round: u64) {
        let key = self.key;
        let e = &mut self.entries;
        let sample = Entry { value, round };
        if key(e[0].value) == 0
            || key(value) >= key(e[0].value)
            || round.saturating_sub(e[2].round) > 10
        {
            self.reset(value, round);
            return;
        }
        if key(value) >= key(e[1].value) {
            e[1] = sample;
            e[2] = sample;
        } else if key(value) >= key(e[2].value) {
            e[2] = sample;
        }
        if round.saturating_sub(e[0].round) > 10 {
            e[0] = e[1];
            e[1] = e[2];
            e[2] = sample;
            if round.saturating_sub(e[0].round) > 10 {
                e[0] = e[1];
                e[1] = e[2];
            }
        } else if key(e[1].value) == key(e[0].value) && round.saturating_sub(e[1].round) > 2 {
            e[1] = sample;
            e[2] = sample;
        } else if key(e[2].value) == key(e[1].value) && round.saturating_sub(e[2].round) > 5 {
            e[2] = sample;
        }
    }
}
