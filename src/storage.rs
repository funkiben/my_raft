use std::marker::PhantomData;

use crate::bytes::WriteBytes;
use crate::config::Config;
use crate::core::{LogEntry, LogEntryType};
use crate::state_machine::{RaftStateMachine, StateMachine};

pub trait Storage<S: StateMachine>: Sized {
    fn add_log_entry(&mut self, entry: LogEntry<S::Command>);
    fn remove_log_entries_before(&mut self, index: usize);
    fn remove_log_entries_starting_at(&mut self, index: usize);
    fn save_log(&mut self);

    fn log_entry(&self, index: usize) -> Option<&LogEntry<S::Command>>;
    fn log_entries(&self, start_index: usize) -> &[LogEntry<S::Command>];
    fn get_last_config_in_log(&self) -> Option<&Config>;
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

pub struct RaftStorage<S, P> {
    inner: P,
    last_config_index: Option<usize>,
    phantom: PhantomData<S>,
}

pub struct Log<P>(P);

pub type LogRef<'a, S, P> = Log<&'a RaftStorage<S, P>>;
pub type LogMut<'a, S, P> = Log<&'a mut RaftStorage<S, P>>;

impl<S: StateMachine, P: Storage<S>> RaftStorage<S, P> {
    pub fn new(inner: P) -> RaftStorage<S, P> {
        let last_config_index = find_latest_config_idx(inner.log_entries(0));
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
        Log(self)
    }

    pub fn log_mut(&mut self) -> LogMut<S, P> {
        Log(self)
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
            self.last_config_index = find_latest_config_idx(self.inner.log_entries(0));
        }
    }

    fn save_log(&mut self) {
        self.inner.save_log();
    }
}

impl<'a, S: StateMachine, P: Storage<S>> Log<&'a mut RaftStorage<S, P>> {
    pub fn try_append_entries(&mut self, mut prev_index: u32, prev_term: u32, entries: impl Iterator<Item=LogEntry<S::Command>>) -> Option<u32> {
        // assume entries before the log starts will always match since they had to have been committed
        if prev_index < self.0.inner().snapshot_last_index() {
            return Some(self.0.inner().snapshot_last_index());
        }

        if !self.immut().matches(prev_index, prev_term) {
            return None;
        }

        let arr_start_index = self.immut().log_index_to_array_index(prev_index + 1);

        for (index, entry) in entries.enumerate().map(|(i, e)| (i + arr_start_index, e)) {
            match self.0.inner().log_entry(index) {
                None => self.0.add_log_entry(entry),
                Some(e) if e.term != entry.term => {
                    self.0.remove_log_entries_starting_at(index);
                    self.0.add_log_entry(entry);
                }
                _ => {}
            }

            prev_index += 1; // use prev_index to store the match_index
        }

        self.0.save_log();

        Some(prev_index)
    }

    pub fn remove_entries_before(&mut self, index: u32) {
        let arr_index = self.immut().log_index_to_array_index(index);
        self.0.remove_log_entries_before(arr_index);
        self.0.save_log();
    }

    pub fn add_entry(&mut self, entry_type: LogEntryType<S::Command>) {
        self.0.add_log_entry(LogEntry { term: self.0.inner().current_term(), entry_type });
        self.0.save_log();
    }

    pub fn immut(&self) -> Log<&RaftStorage<S, P>> {
        Log(self.0)
    }
}

impl<'a, S: StateMachine, P: Storage<S>> Log<&'a RaftStorage<S, P>> {
    pub fn has_uncommitted_command_from_client(&self, commit_idx: u32, client: u32, id: u32) -> bool {
        let arr_commit_idx = self.log_index_to_array_index(commit_idx);
        self.0.inner().log_entries(arr_commit_idx + 1).iter().any(|e|
            match e.entry_type {
                LogEntryType::Command { command_id, client_id, .. } => command_id == id && client_id == client,
                _ => false
            })
    }

    pub fn last_log_term(&self) -> u32 {
        self.0.inner().log_entry(self.0.inner().num_log_entries() - 1).map(|e| e.term).unwrap_or(self.0.inner().snapshot_last_index())
    }

    pub fn last_log_index(&self) -> u32 {
        self.0.inner().snapshot_last_index() + self.0.inner().num_log_entries() as u32
    }

    pub fn entries(&self, start_index: u32) -> Option<&'a [LogEntry<S::Command>]> {
        if start_index <= self.0.inner().snapshot_last_index() {
            None
        } else {
            Some(self.0.inner().log_entries(self.log_index_to_array_index(start_index)))
        }
    }

    pub fn term(&self, index: u32) -> Option<u32> {
        if index == self.0.inner().snapshot_last_index() {
            Some(self.0.inner().snapshot_last_term())
        } else {
            self.0.inner().log_entry(self.log_index_to_array_index(index)).map(|e| e.term)
        }
    }

    fn log_index_to_array_index(&self, index: u32) -> usize {
        (index - self.0.inner().snapshot_last_index() - 1) as usize
    }

    fn matches(&self, index: u32, term: u32) -> bool {
        self.term(index).map(|t| t == term).unwrap_or(false)
    }
}

fn find_latest_config_idx<C>(entries: &[LogEntry<C>]) -> Option<usize> {
    for (idx, entry) in entries.iter().enumerate().rev() {
        if let LogEntryType::Config(_) = entry.entry_type {
            return Some(idx);
        }
    }
    None
}