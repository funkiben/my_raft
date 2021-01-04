use std::collections::HashMap;

use crate::config::Config;
use crate::serialize::{TryFromReader, WriteBytes, TryFromBytes};
use crate::core::{LogEntry, LogEntryType};

pub trait StateMachine: TryFromReader + WriteBytes {
    type Command: TryFromBytes + WriteBytes;

    fn apply_command(&mut self, command: &Self::Command);
}

pub struct RaftStateMachine<S> {
    inner: S,
    config: Config,
    client_last_command_ids: HashMap<u32, u32>,
}

impl<S: StateMachine> RaftStateMachine<S> {
    pub fn new(config: Config, inner: S) -> RaftStateMachine<S> {
        RaftStateMachine {
            inner,
            config,
            client_last_command_ids: HashMap::new(),
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    pub fn is_last_client_command(&self, client_id: u32, command_id: u32) -> bool {
        self.client_last_command_ids.get(&client_id).map(|id| *id == command_id).unwrap_or(false)
    }
}

impl<S: StateMachine> StateMachine for RaftStateMachine<S> {
    type Command = LogEntry<S::Command>;

    fn apply_command(&mut self, command: &LogEntry<S::Command>) {
        match &command.entry_type {
            LogEntryType::Command { client_id, command_id, command } => {
                self.client_last_command_ids.insert(*client_id, *command_id);
                self.inner.apply_command(command);
            }
            LogEntryType::Config(config) => {
                self.config = config.clone();
            }
            _ => {}
        }
    }
}