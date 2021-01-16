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
        let start_arr_idx = self.log_index_to_array_index(commit_idx + 1);
        self.0.inner().log_entries(start_arr_idx).iter().any(|e|
            match e.entry_type {
                LogEntryType::Command { command_id, client_id, .. } => command_id == id && client_id == client,
                _ => false
            })
    }

    pub fn last_term(&self) -> u32 {
        self.term(self.last_index()).unwrap()
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

#[cfg(test)]
mod tests {
    use std::io;
    use std::io::Write;

    use crate::bytes::{ReadBytes, TryFromBytes, WriteBytes};
    use crate::state_machine::{RaftStateMachine, StateMachine};
    use crate::storage::{RaftStorage, Storage};
    use crate::storage::log::{LogEntry, LogEntryType};

    struct MockCommand;

    struct MockStateMachine;

    struct MockStorage {
        log: Vec<LogEntry<MockCommand>>,
        last_snapshot_index: u32,
        last_snapshot_term: u32,
        current_term: u32
    }

    impl Storage<MockStateMachine> for MockStorage {
        fn add_log_entry(&mut self, entry: LogEntry<MockCommand>) {
            self.log.push(entry);
        }

        fn remove_log_entries_before(&mut self, index: usize) {
            self.log.drain(..index);
        }

        fn remove_log_entries_starting_at(&mut self, index: usize) {
            self.log.drain(index..);
        }

        fn save_log(&mut self) {}

        fn log_entry(&self, index: usize) -> Option<&LogEntry<MockCommand>> {
            self.log.get(index)
        }

        fn log_entries(&self, start_index: usize) -> &[LogEntry<MockCommand>] {
            &self.log[start_index..]
        }

        fn get_index_of_last_config_in_log(&self) -> Option<usize> {
            None
        }

        fn num_log_entries(&self) -> usize {
            self.log.len()
        }


        fn set_snapshot(&mut self, _last_index: u32, _last_term: u32, _state_machine: &RaftStateMachine<MockStateMachine>) {
            unimplemented!()
        }

        fn snapshot(&self) -> RaftStateMachine<MockStateMachine> {
            unimplemented!()
        }

        fn snapshot_last_index(&self) -> u32 {
            self.last_snapshot_index
        }

        fn snapshot_last_term(&self) -> u32 {
            self.last_snapshot_term
        }

        fn add_new_snapshot_chunk(&mut self, _offset: u32, _data: &[u8]) {
            unimplemented!()
        }

        fn try_use_chunks_as_new_snapshot(&mut self, _last_index: u32, _last_term: u32) -> Option<RaftStateMachine<MockStateMachine>> {
            unimplemented!()
        }

        fn snapshot_chunk(&self, _offset: u32, _amt: u32) -> &[u8] {
            unimplemented!()
        }

        fn total_snapshot_bytes(&self) -> u32 {
            unimplemented!()
        }

        fn set_voted_for(&mut self, _voted_for: Option<u32>) {
            unimplemented!()
        }

        fn voted_for(&self) -> Option<u32> {
            unimplemented!()
        }

        fn set_current_term(&mut self, current_term: u32) {
            self.current_term = current_term;
        }

        fn current_term(&self) -> u32 {
            self.current_term
        }
    }

    impl StateMachine for MockStateMachine {
        type Command = MockCommand;

        fn apply_command(&mut self, _command: &Self::Command) {
            unimplemented!()
        }
    }

    impl TryFromBytes for MockStateMachine {
        fn try_from_bytes(_bytes: impl ReadBytes) -> Option<Self> {
            unimplemented!()
        }
    }

    impl WriteBytes for MockStateMachine {
        fn write_bytes(&self, _writer: impl Write) -> io::Result<usize> {
            unimplemented!()
        }
    }

    impl TryFromBytes for MockCommand {
        fn try_from_bytes(_bytes: impl ReadBytes) -> Option<Self> {
            unimplemented!()
        }
    }

    impl WriteBytes for MockCommand {
        fn write_bytes(&self, _writer: impl Write) -> io::Result<usize> {
            unimplemented!()
        }
    }

    fn create_storage(last_snapshot_index: u32, last_snapshot_term: u32) -> RaftStorage<MockStateMachine, MockStorage> {
        RaftStorage::new(MockStorage {
            log: vec![],
            last_snapshot_index,
            last_snapshot_term,
            current_term: 0
        })
    }

    #[test]
    fn empty() {
        let storage = create_storage(0, 0);
        let log = storage.log();
        assert_eq!(log.last_term(), 0);
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.term(0), Some(0));
        assert_eq!(log.term(1), None);
        assert!(log.entries(0).is_none());
        assert_eq!(log.entries(1).unwrap().len(), 0);
    }

    #[test]
    fn append_entries() {
        let mut storage = create_storage(0, 0);
        let mut log = storage.log_mut();
        let res = log.try_append_entries(0, 0, vec![
            LogEntry { entry_type: LogEntryType::Command { client_id: 0, command_id: 1, command: MockCommand }, term: 0 },
            LogEntry { entry_type: LogEntryType::Command { client_id: 1, command_id: 2, command: MockCommand }, term: 1 },
            LogEntry { entry_type: LogEntryType::Command { client_id: 2, command_id: 3, command: MockCommand }, term: 2 },
        ].into_iter());
        assert_eq!(res, Some(3));

        let log = log.immut();
        assert_eq!(log.term(0).unwrap(), 0);
        assert_eq!(log.term(1).unwrap(), 0);
        assert_eq!(log.term(2).unwrap(), 1);
        assert_eq!(log.term(3).unwrap(), 2);
        assert_eq!(log.term(4), None);
        let entries = log.entries(1).unwrap();
        assert_eq!(entries[0].term, 0);
        assert_eq!(entries[1].term, 1);
        assert_eq!(entries[2].term, 2);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
    }

    #[test]
    fn append_entries_invalid() {
        let mut storage = create_storage(0, 0);
        let mut log = storage.log_mut();
        let res = log.try_append_entries(4, 2, vec![
            LogEntry { entry_type: LogEntryType::Command { client_id: 0, command_id: 1, command: MockCommand }, term: 0 },
            LogEntry { entry_type: LogEntryType::Command { client_id: 1, command_id: 2, command: MockCommand }, term: 1 },
            LogEntry { entry_type: LogEntryType::Command { client_id: 2, command_id: 3, command: MockCommand }, term: 2 },
        ].into_iter());
        assert!(res.is_none());

        let log = log.immut();
        assert_eq!(log.term(0).unwrap(), 0);
        assert_eq!(log.term(1), None);
        assert_eq!(log.entries(1).unwrap().len(), 0);
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
    }

    #[test]
    fn append_entries_twice() {
        let mut storage = create_storage(0, 0);
        let mut log = storage.log_mut();
        log.try_append_entries(0, 0, vec![
            LogEntry { entry_type: LogEntryType::Command { client_id: 0, command_id: 1, command: MockCommand }, term: 0 },
            LogEntry { entry_type: LogEntryType::Command { client_id: 1, command_id: 2, command: MockCommand }, term: 1 },
            LogEntry { entry_type: LogEntryType::Command { client_id: 2, command_id: 3, command: MockCommand }, term: 2 },
        ].into_iter()).unwrap();
        let res = log.try_append_entries(0, 0, vec![
            LogEntry { entry_type: LogEntryType::Command { client_id: 0, command_id: 1, command: MockCommand }, term: 0 },
            LogEntry { entry_type: LogEntryType::Command { client_id: 1, command_id: 2, command: MockCommand }, term: 1 },
            LogEntry { entry_type: LogEntryType::Command { client_id: 2, command_id: 3, command: MockCommand }, term: 2 },
        ].into_iter());
        assert_eq!(res, Some(3));

        let log = log.immut();
        assert_eq!(log.term(0).unwrap(), 0);
        assert_eq!(log.term(1).unwrap(), 0);
        assert_eq!(log.term(2).unwrap(), 1);
        assert_eq!(log.term(3).unwrap(), 2);
        assert_eq!(log.term(4), None);
        let entries = log.entries(1).unwrap();
        assert_eq!(entries[0].term, 0);
        assert_eq!(entries[1].term, 1);
        assert_eq!(entries[2].term, 2);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
    }

    #[test]
    fn empty_append_entries() {
        let mut storage = create_storage(0, 0);
        let mut log = storage.log_mut();
        log.try_append_entries(0, 0, vec![].into_iter()).unwrap();

        let log = log.immut();
        assert_eq!(log.term(0).unwrap(), 0);
        assert_eq!(log.term(1), None);
        assert_eq!(log.entries(1).unwrap().len(), 0);
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
    }

    #[test]
    fn add_entry() {
        let mut storage = create_storage(0, 0);
        storage.set_current_term(3);
        storage.log_mut().add_entry(LogEntryType::Noop);
        storage.set_current_term(4);
        storage.log_mut().add_entry(LogEntryType::Command { client_id: 0, command_id: 1, command: MockCommand });
        storage.set_current_term(5);
        storage.log_mut().add_entry(LogEntryType::Noop);

        let log = storage.log();
        assert_eq!(log.term(0).unwrap(), 0);
        assert_eq!(log.term(1).unwrap(), 3);
        assert_eq!(log.term(2).unwrap(), 4);
        assert_eq!(log.term(3).unwrap(), 5);
        assert_eq!(log.term(4), None);
        let entries = log.entries(1).unwrap();
        assert_eq!(entries[0].term, 3);
        assert_eq!(entries[1].term, 4);
        assert_eq!(entries[2].term, 5);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 5);
    }

    #[test]
    fn snapshot_empty() {
        let storage = create_storage(4, 6);
        let log = storage.log();
        assert_eq!(log.last_index(), 4);
        assert_eq!(log.last_term(), 6);
        assert_eq!(log.term(4), Some(6));
        assert_eq!(log.term(5), None);
        assert!(log.entries(0).is_none());
        assert!(log.entries(1).is_none());
        assert!(log.entries(2).is_none());
        assert!(log.entries(4).is_none());
        assert_eq!(log.entries(5).unwrap().len(), 0);
    }
}