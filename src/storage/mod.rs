use std::marker::PhantomData;

use crate::bytes::WriteBytes;
use crate::config::Config;
use crate::state_machine::{RaftStateMachine, StateMachine};
use crate::storage::log::{Log, LogEntry, LogEntryType, LogMut, LogRef};

pub mod log;

pub struct RaftStorage<S, P> {
    inner: P,
    last_config_index: Option<usize>,
    phantom: PhantomData<S>,
}

pub trait Storage<S: StateMachine>: Sized {
    fn add_log_entry(&mut self, entry: LogEntry<S::Command>);
    fn remove_log_entries_before(&mut self, index: usize);
    fn remove_log_entries_starting_at(&mut self, index: usize);
    fn save_log(&mut self);

    fn log_entry(&self, index: usize) -> Option<&LogEntry<S::Command>>;
    fn log_entries(&self, start_index: usize) -> &[LogEntry<S::Command>];
    fn get_index_of_last_config_in_log(&self) -> Option<usize>;
    fn num_log_entries(&self) -> usize;

    fn set_snapshot(&mut self, last_index: u32, last_term: u32, bytes: &impl WriteBytes);

    fn snapshot(&self) -> RaftStateMachine<S>;
    fn snapshot_last_index(&self) -> u32;
    fn snapshot_last_term(&self) -> u32;

    fn add_new_snapshot_chunk(&mut self, offset: u32, data: &[u8]);
    fn try_use_chunks_as_new_snapshot(&mut self, last_index: u32, last_term: u32) -> Option<RaftStateMachine<S>>;
    fn snapshot_chunk(&self, offset: u32, amt: u32) -> Option<&[u8]>;

    fn total_snapshot_bytes(&self) -> u32;

    fn set_voted_for(&mut self, voted_for: Option<u32>);
    fn voted_for(&self) -> Option<u32>;

    fn set_current_term(&mut self, current_term: u32);
    fn current_term(&self) -> u32;
}

impl<S: StateMachine, P: Storage<S>> RaftStorage<S, P> {
    pub fn new(inner: P) -> RaftStorage<S, P> {
        let last_config_index = inner.get_index_of_last_config_in_log();
        RaftStorage {
            inner,
            last_config_index,
            phantom: PhantomData,
        }
    }

    pub fn set_snapshot(&mut self, last_index: u32, last_term: u32, bytes: &impl WriteBytes) {
        self.inner.set_snapshot(last_index, last_term, bytes)
    }

    pub fn add_new_snapshot_chunk(&mut self, offset: u32, data: &[u8]) {
        self.inner.add_new_snapshot_chunk(offset, data)
    }

    pub fn try_use_chunks_as_new_snapshot(&mut self, last_index: u32, last_term: u32) -> Option<RaftStateMachine<S>> {
        self.inner.try_use_chunks_as_new_snapshot(last_index, last_term)
    }

    pub fn set_voted_for(&mut self, voted_for: Option<u32>) {
        self.inner.set_voted_for(voted_for)
    }

    pub fn set_current_term(&mut self, current_term: u32) {
        self.inner.set_current_term(current_term)
    }

    pub fn get_last_config_in_log(&self) -> Option<&Config> {
        self.last_config_index.map(|idx|
            match self.inner().log_entry(idx) {
                Some(LogEntry { entry_type: LogEntryType::Config(ref cfg), .. }) => cfg,
                _ => panic!()
            })
    }

    pub fn inner(&self) -> &P {
        &self.inner
    }

    pub fn log(&self) -> LogRef<S, P> {
        Log::new(self)
    }

    pub fn log_mut(&mut self) -> LogMut<S, P> {
        Log::new(self)
    }

    fn add_log_entry(&mut self, entry: LogEntry<S::Command>) {
        if let LogEntryType::Config(_) = entry.entry_type {
            self.last_config_index = Some(self.inner.num_log_entries());
        }
        self.inner.add_log_entry(entry);
    }

    fn remove_log_entries_before(&mut self, index: usize) {
        self.last_config_index = self.last_config_index.filter(|idx| *idx >= index);
        self.inner.remove_log_entries_before(index);
    }

    fn remove_log_entries_starting_at(&mut self, index: usize) {
        self.last_config_index = self.last_config_index.filter(|idx| *idx < index);
        self.inner.remove_log_entries_starting_at(index);
        if self.last_config_index.is_none() {
            self.last_config_index = self.inner.get_index_of_last_config_in_log();
        }
    }

    fn save_log(&mut self) {
        self.inner.save_log();
    }
}
