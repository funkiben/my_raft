use crate::config::Config;

pub struct Log<C> {
    // TODO rename these because the names are confusing
    prev_index: u32,
    prev_term: u32,
    entries: Vec<LogEntry<C>>,
    // this is a normal array index starting at 0
    latest_config_idx: Option<usize>,
}

pub struct LogEntry<C> {
    pub entry_type: LogEntryType<C>,
    pub term: u32,
}

pub enum LogEntryType<C> {
    Command { command: C, client_id: u32, command_id: u32 },
    Noop,
    Config(Config),
}

impl<C> Log<C> {
    pub fn new(prev_index: u32, prev_term: u32, entries: Vec<LogEntry<C>>) -> Log<C> {
        let latest_config_idx = find_latest_config_idx(&entries);
        Log {
            prev_index,
            prev_term,
            entries,
            latest_config_idx,
        }
    }

    pub fn try_append_entries(&mut self, mut prev_index: u32, prev_term: u32, entries: impl Iterator<Item=LogEntry<C>>) -> Option<u32> {
        // assume entries before the log starts will always match since they had to have been committed
        if prev_index < self.prev_index {
            return Some(self.prev_index);
        }

        if !self.matches(prev_index, prev_term) {
            return None;
        }

        let arr_start_index = self.log_index_to_array_index(prev_index + 1);

        for (index, entry) in entries.enumerate().map(|(i, e)| (i + arr_start_index, e)) {
            match self.entries.get(index) {
                None => self.push_entry(entry),
                Some(e) if e.term != entry.term => {
                    self.entries.drain(index..);
                    self.latest_config_idx = self.latest_config_idx.filter(|&idx| idx < index);
                    self.push_entry(entry);
                }
                _ => {}
            }

            prev_index += 1; // use prev_index to store the match_index
        }

        Some(prev_index)
    }

    pub fn remove_entries_before(&mut self, index: u32) {
        let arr_index = self.log_index_to_array_index(index);

        self.latest_config_idx = self.latest_config_idx.filter(|&idx| idx >= arr_index);

        self.prev_index = (index - 1) as u32;
        self.prev_term = self.entries.get(arr_index - 1).map(|e| e.term).unwrap_or(0);

        self.entries.drain(..arr_index);
    }

    pub fn has_uncommitted_command_from_client(&self, commit_idx: u32, client: u32, id: u32) -> bool {
        let arr_commit_idx = self.log_index_to_array_index(commit_idx);
        self.entries[(arr_commit_idx + 1)..].iter().any(|e|
            match e.entry_type {
                LogEntryType::Command { command_id, client_id, .. } => command_id == id && client_id == client,
                _ => false
            })
    }

    pub fn config(&self) -> Option<&Config> {
        self.latest_config_idx.map(|idx|
            match self.entries.get(idx as usize) {
                Some(LogEntry { entry_type: LogEntryType::Config(config), .. }) => config,
                _ => panic!("Config not in log!")
            })
    }

    pub fn last_log_term(&self) -> u32 {
        self.entries.last().map(|e| e.term).unwrap_or(self.prev_term)
    }

    pub fn last_log_index(&self) -> u32 {
        self.prev_index + self.entries.len() as u32
    }

    pub fn index_before_first_log_entry(&self) -> u32 {
        self.prev_index
    }

    pub fn term_before_first_log_entry(&self) -> u32 {
        self.prev_term
    }

    pub fn entries(&self, start_index: u32) -> Option<&[LogEntry<C>]> {
        if start_index <= self.prev_index {
            None
        } else {
            Some(&self.entries[self.log_index_to_array_index(start_index)..])
        }
    }

    pub fn term(&self, index: u32) -> Option<u32> {
        if index == self.prev_index {
            Some(self.prev_term)
        } else {
            self.entries.get(self.log_index_to_array_index(index)).map(|e| e.term)
        }
    }

    pub fn push_entry(&mut self, entry: LogEntry<C>) {
        if let LogEntryType::Config(_) = entry.entry_type {
            self.latest_config_idx = Some(self.entries.len());
        }
        self.entries.push(entry);
    }

    fn log_index_to_array_index(&self, index: u32) -> usize {
        (index - self.prev_index - 1) as usize
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
