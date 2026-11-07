//! Production replay retains prepaid history originals, never cloned DTOs.
use super::*;
pub enum CatchUp {
    Original(zero_engine::RetainedReplay),
    #[cfg(test)]
    Fixture(Vec<RawApiEvent>),
}
impl From<zero_engine::RetainedReplay> for CatchUp {
    fn from(replay: zero_engine::RetainedReplay) -> Self {
        Self::Original(replay)
    }
}
#[cfg(test)]
impl From<Vec<RawApiEvent>> for CatchUp {
    fn from(events: Vec<RawApiEvent>) -> Self {
        Self::Fixture(events)
    }
}
impl CatchUp {
    pub fn len(&self) -> usize {
        match self {
            Self::Original(replay) => replay.len(),
            #[cfg(test)]
            Self::Fixture(events) => events.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn event(&self, index: usize) -> &RawApiEvent {
        match self {
            Self::Original(replay) => replay.event(index),
            #[cfg(test)]
            Self::Fixture(events) => &events[index],
        }
    }
}
