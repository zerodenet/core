//! Concrete private mask tables; one sorted original array, no shadow census.
use std::{
    io,
    ops::{Deref, DerefMut},
};
use zero_platform_tokio::contract::component::{PacketDriverMetadataView, PacketMetadataVec};
pub(super) struct Rows<K, V> {
    values: Storage<K, V>,
    memory: Option<PacketDriverMetadataView>,
    max: usize,
}
enum Storage<K, V> {
    Library(Vec<(K, V)>),
    Hosted(PacketMetadataVec<(K, V)>),
}
impl<K: Ord, V> Rows<K, V> {
    pub fn new(max: usize) -> Self {
        Self {
            values: Storage::Library(Vec::new()),
            memory: None,
            max,
        }
    }
    pub fn install(&mut self, memory: Option<PacketDriverMetadataView>) {
        assert!(self.is_empty(), "mask tables bind before original IO");
        self.values = if memory.is_some() {
            Storage::Hosted(PacketMetadataVec::default())
        } else {
            Storage::Library(Vec::new())
        };
        self.memory = memory;
    }
    pub fn locate(&self, key: &K) -> Result<usize, usize> {
        self.binary_search_by(|(actual, _)| actual.cmp(key))
    }
    pub fn get(&self, key: &K) -> Option<&V> {
        self.locate(key).ok().map(|i| &self[i].1)
    }
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.locate(key).ok().map(|i| &mut self[i].1)
    }
    pub fn reserve(&mut self, key: &K) -> io::Result<()> {
        if self.locate(key).is_ok() {
            return Ok(());
        }
        if self.len() >= self.max {
            return Err(io::ErrorKind::OutOfMemory.into());
        }
        match &mut self.values {
            Storage::Library(rows) => rows.try_reserve(1).map_err(io::Error::other),
            Storage::Hosted(rows) => rows
                .try_reserve_one_with_metadata(
                    self.memory.as_ref().expect("original table Root"),
                    self.max,
                )
                .map_err(io::Error::other),
        }
    }
    pub fn insert_reserved(&mut self, key: K, value: V) -> Option<V> {
        match self.locate(&key) {
            Ok(index) => Some(std::mem::replace(&mut self[index].1, value)),
            Err(index) => {
                match &mut self.values {
                    Storage::Library(rows) => rows.push((key, value)),
                    Storage::Hosted(rows) => assert!(
                        rows.push_reserved((key, value)).is_ok(),
                        "actual mask row prepaid"
                    ),
                }
                let last = self.len() - 1;
                self[index..=last].rotate_right(1);
                None
            }
        }
    }
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let index = self.locate(key).ok()?;
        Some(match &mut self.values {
            Storage::Library(rows) => rows.remove(index).1,
            Storage::Hosted(rows) => rows.remove(index).1,
        })
    }
    pub fn retain(&mut self, mut keep: impl FnMut(&K, &V) -> bool) {
        match &mut self.values {
            Storage::Library(rows) => rows.retain(|(k, v)| keep(k, v)),
            Storage::Hosted(rows) => rows.retain(|(k, v)| keep(k, v)),
        }
    }
}
impl<K, V> Deref for Rows<K, V> {
    type Target = [(K, V)];
    fn deref(&self) -> &Self::Target {
        match &self.values {
            Storage::Library(rows) => rows,
            Storage::Hosted(rows) => rows,
        }
    }
}
impl<K, V> DerefMut for Rows<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.values {
            Storage::Library(rows) => rows,
            Storage::Hosted(rows) => rows,
        }
    }
}
