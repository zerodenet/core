//! One paid file acquisition in the original dispatcher inventory.
use super::*;
impl DispatcherResources {
    fn reserve_file(&self) -> io::Result<Arc<Mutex<FileOriginal>>> {
        self.retire_files(false)?;
        self.available()?;
        let charge = self.0.capacity.original()?;
        let slot = Arc::new(Mutex::new(FileOriginal {
            acquiring: true,
            original: None,
            charge: Some(charge),
        }));
        self.0.files.lock().unwrap().push(slot.clone());
        Ok(slot)
    }
    fn file_not_acquired(&self, slot: &Arc<Mutex<FileOriginal>>) {
        let charge = {
            let mut state = slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(state.acquiring && state.original.is_none());
            state.acquiring = false;
            state.charge.take()
        };
        self.0
            .files
            .lock()
            .unwrap()
            .retain(|actual| !Arc::ptr_eq(actual, slot));
        if let Some(charge) = charge {
            charge.complete();
        }
    }
    pub(crate) fn open(&self, open: impl FnOnce() -> io::Result<File>) -> io::Result<FileView> {
        let slot = self.reserve_file()?;
        let file = match open() {
            Ok(file) => file,
            Err(error) => {
                self.file_not_acquired(&slot);
                return Err(error);
            }
        };
        let original = Arc::new(Mutex::new(OriginalFile::adopt(file)));
        {
            let mut state = slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.original = Some(original.clone());
            state.acquiring = false;
        }
        Ok(FileView { original })
    }
    pub(crate) fn temporary(
        &self,
        parent: &std::path::Path,
    ) -> io::Result<(FileView, tempfile::TempPath)> {
        let slot = self.reserve_file()?;
        let (file, path) = match tempfile::NamedTempFile::new_in(parent) {
            Ok(file) => file.into_parts(),
            Err(error) => {
                self.file_not_acquired(&slot);
                return Err(error);
            }
        };
        let original = Arc::new(Mutex::new(OriginalFile::adopt(file)));
        {
            let mut state = slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.original = Some(original.clone());
            state.acquiring = false;
        }
        Ok((FileView { original }, path))
    }
    pub(crate) fn retire_files(&self, require_all: bool) -> io::Result<()> {
        let mut files = self.0.files.lock().unwrap();
        let mut position = 0;
        let mut outstanding = false;
        while position < files.len() {
            let mut slot = files[position]
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if slot.acquiring {
                outstanding = true;
                drop(slot);
                position += 1;
                continue;
            }
            let Some(controller) = slot.original.as_ref() else {
                outstanding = true;
                drop(slot);
                position += 1;
                continue;
            };
            if Arc::strong_count(controller) != 1 {
                outstanding = true;
                drop(slot);
                position += 1;
                continue;
            }
            let mut original = controller.lock().unwrap_or_else(|error| error.into_inner());
            let outcome = original.close();
            match outcome {
                Ok(receipt) if original.matches_closed(&receipt) => {
                    drop(original);
                    let charge = slot.charge.take();
                    drop(slot);
                    files.remove(position);
                    if let Some(charge) = charge {
                        charge.complete();
                    }
                }
                Ok(_) => {
                    self.retain_failure(io::Error::other("file receipt mismatches original"));
                    drop(original);
                    drop(slot);
                    position += 1;
                }
                Err(error) => {
                    self.retain_failure(error);
                    drop(original);
                    drop(slot);
                    position += 1;
                }
            }
        }
        if require_all && outstanding {
            return Err(self.retain_failure(io::Error::other(
                "original dispatcher file still has a transferred view",
            )));
        }
        self.available()
    }
}
