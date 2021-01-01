use std::marker::PhantomData;

use crate::log::{Log, LogEntry, LogEntryType};
use crate::persistent_storage::PersistentStorage;
use crate::state_machine::StateMachine;

pub struct PersistedLog<S: StateMachine, P: PersistentStorage<S>>(Log<S::Command>, PhantomData<P>);

impl<S: StateMachine, P: PersistentStorage<S>> PersistedLog<S, P> {
    pub fn new(log: Log<S::Command>) -> PersistedLog<S, P> {
        PersistedLog(log, PhantomData)
    }

    pub fn get_ref(&self) -> &Log<S::Command> {
        &self.0
    }

    pub fn try_append_entries(&mut self, storage: &mut P, prev_index: u32, prev_term: u32, entries: impl Iterator<Item=LogEntry<S::Command>>) -> Option<u32> {
        let result = self.0.try_append_entries(prev_index, prev_term, entries);
        if result.is_some() {
            self.save_log_entries(storage, prev_index + 1);
        }
        result
    }

    pub fn push_entry(&mut self, storage: &mut P, entry_type: LogEntryType<S::Command>) {
        self.0.push_entry(LogEntry { term: storage.current_term(), entry_type });
        self.save_log_entries(storage, self.0.last_log_index());
    }

    pub fn remove_entries_before(&mut self, storage: &mut P, index: u32) {
        self.0.remove_entries_before(index);
        storage.remove_log_entries_before(index);
    }

    pub fn save_log_entries(&self, storage: &mut P, start_index: u32) {
        storage.save_log_entries(start_index, self.0.entries(start_index).unwrap());
    }
}

