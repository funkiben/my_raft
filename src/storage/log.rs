use crate::config::Config;
use crate::state_machine::StateMachine;
use crate::storage::{RaftStorage, Storage};

pub struct Log<P>(P);

pub type LogRef<'a, S, P> = Log<&'a RaftStorage<S, P>>;
pub type LogMut<'a, S, P> = Log<&'a mut RaftStorage<S, P>>;

pub struct LogEntry<C> {
    pub entry_type: LogEntryType<C>,
    pub term: u32,
}

pub enum LogEntryType<C> {
    Command { command: C, client_id: u32, command_id: u32 },
    Noop,
    Config(Config),
}

impl<T> Log<T> {
    pub fn new(inner: T) -> Log<T> {
        Log(inner)
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

    pub fn last_term(&self) -> u32 {
        self.0.inner().log_entry(self.0.inner().num_log_entries() - 1).map(|e| e.term).unwrap_or(self.0.inner().snapshot_last_index())
    }

    pub fn last_index(&self) -> u32 {
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