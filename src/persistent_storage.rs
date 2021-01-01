use crate::config::Config;
use crate::log::LogEntry;
use crate::state_machine::{RaftStateMachine, StateMachine};

pub trait PersistentStorage<S: StateMachine> {
    fn init_config(&self) -> Config;

    fn save_log_entries(&mut self, start_index: u32, entries: &[LogEntry<S::Command>]);
    fn remove_log_entries_before(&mut self, end_index: u32);
    fn log_entries(&self) -> Vec<LogEntry<S::Command>>;

    fn save_voted_for(&mut self, voted_for: Option<u32>);
    fn voted_for(&self) -> Option<u32>;

    fn save_current_term(&mut self, current_term: u32);
    fn current_term(&self) -> u32;

    fn save_snapshot(&mut self, snapshot: &Snapshot<S>);
    fn snapshot(&self) -> Option<Snapshot<S>>;

    fn save_snapshot_chunk(&mut self, last_index: u32, last_term: u32, offset: u32, data: &[u8], done: bool);
    fn snapshot_chunk(&self, offset: u32, amt: u32) -> Option<&[u8]>;
    fn total_snapshot_bytes(&self) -> u32;
}

pub struct Snapshot<S> {
    pub state_machine: RaftStateMachine<S>,
    pub last_index: u32,
    pub last_term: u32,
}
