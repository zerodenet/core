use super::*;
impl LegacyReplay {
    pub(crate) fn with_bloom_budget(
        mut self,
        budget: Arc<super::LegacyReplayBudget>,
    ) -> Result<Self, Error> {
        if !Arc::ptr_eq(&self.bloom_budget, &budget)
            && (Arc::strong_count(&self.state) != 1
                || self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .has_history())
        {
            return Err(Error::Config("cannot fork an original legacy Bloom book"));
        }
        self.bloom_budget = budget;
        Ok(self)
    }
    pub(crate) fn with_bloom_limit(mut self, max_bytes: usize) -> Result<Self, Error> {
        self.bloom_budget.validate(Some(max_bytes))?;
        self.bloom_limit = Some(max_bytes);
        Ok(self)
    }
    pub(crate) fn bloom_identity(&self) -> (usize, Option<usize>) {
        (Arc::as_ptr(&self.bloom_budget) as usize, self.bloom_limit)
    }
    pub(crate) fn activate_bloom_budget(&self) -> Result<(), Error> {
        if let Some(limit) = self.bloom_limit {
            self.bloom_budget.install(limit)?;
        }
        Ok(())
    }
    fn window<'a>(&self, book: &'a mut Book) -> Result<&'a mut Window, Error> {
        if book.window.is_none() {
            let charge = self.bloom_budget.claim(&self.state, self.server)?;
            book.window = Some(Window::new(self.server));
            book._charge = Some(charge);
        }
        Ok(book
            .window
            .as_mut()
            .expect("original legacy Bloom filters installed"))
    }
    pub(crate) fn activate_security_budgets(&self) -> Result<(), Error> {
        self.activate_bloom_budget()?;
        #[cfg(feature = "blake3")]
        self.activate_udp_budget()?;
        Ok(())
    }
}
