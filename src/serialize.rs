use std::io;
use std::io::{Read, Write};
use std::marker::PhantomData;

use crate::config::Config;
use crate::message::{Message, OutgoingAppendEntries};
use crate::log::{LogEntry, LogEntryType};
use crate::persistent_storage::Snapshot;
use crate::state_machine::{RaftStateMachine, StateMachine};

pub trait WriteBytes {
    fn write_bytes(&self, writer: impl io::Write) -> io::Result<usize>;
}

pub trait TryFromReader: Sized {
    fn try_from_reader(reader: impl io::Read) -> Option<(Self, usize)>;
}

pub trait TryFromBytes: Sized {
    fn try_from_bytes(bytes: &[u8]) -> Option<(Self, usize)>;
}

pub trait TryFromExactBytes: Sized {
    fn try_from_bytes(bytes: &[u8]) -> Option<Self>;
}

fn get_u32(buf: &[u8], start: usize) -> Option<u32> {
    if start >= buf.len() {
        None
    } else {
        Some(u32::from_be_bytes([buf[start], buf[start + 1], buf[start + 2], buf[start + 3]]))
    }
}

impl<C: TryFromBytes> TryFromBytes for LogEntry<C> {
    fn try_from_bytes(bytes: &[u8]) -> Option<(Self, usize)> {
        const MIN: usize = 5;

        if bytes.len() < MIN { return None; }

        let term = get_u32(bytes, 0)?;
        let (entry_type, entry_type_bytes) = LogEntryType::try_from_bytes(&bytes[MIN..])?;
        Some((LogEntry { term, entry_type }, MIN + entry_type_bytes))
    }
}

impl<C: TryFromBytes> TryFromBytes for LogEntryType<C> {
    fn try_from_bytes(bytes: &[u8]) -> Option<(Self, usize)> {
        const MIN: usize = 1;

        if bytes.len() < MIN { return None; }

        match bytes[0] {
            0 => {
                const COMMAND_MIN: usize = MIN + 8;

                let client_id = get_u32(bytes, MIN)?;
                let command_id = get_u32(bytes, MIN + 4)?;

                C::try_from_bytes(&bytes[COMMAND_MIN..]).map(|(command, amt)| (LogEntryType::Command { client_id, command_id, command }, amt + COMMAND_MIN))
            }
            1 => Some((LogEntryType::Noop, MIN)),
            2 => Config::try_from_bytes(&bytes[MIN..]).map(|(cfg, amt)| (LogEntryType::Config(cfg), amt + MIN)),
            _ => None
        }
    }
}

pub struct LogEntriesBytes<'a, C>(&'a [u8], usize, PhantomData<C>);

impl<'a, C> LogEntriesBytes<'a, C> {
    pub fn new(bytes: &'a [u8]) -> Self {
        LogEntriesBytes(bytes, 0, PhantomData)
    }
}

impl<'a, C: TryFromBytes> Iterator for LogEntriesBytes<'a, C> {
    type Item = LogEntry<C>;

    fn next(&mut self) -> Option<LogEntry<C>> {
        match LogEntry::try_from_bytes(self.0) {
            Some((entry, amt)) => {
                self.1 += amt;
                Some(entry)
            }
            None => None
        }
    }
}

impl<C: WriteBytes> WriteBytes for LogEntry<C> {
    fn write_bytes(&self, mut writer: impl Write) -> io::Result<usize> {
        writer.write(&self.term.to_be_bytes())?;
        // TODO
        Ok(5)
    }
}

impl<'a> TryFromExactBytes for Message<'a> {
    fn try_from_bytes(bytes: &[u8]) -> Option<Self> {
        unimplemented!()
    }
}

impl<'a> WriteBytes for Message<'a> {
    fn write_bytes(&self, writer: impl Write) -> io::Result<usize> {
        unimplemented!()
    }
}

impl<S: StateMachine> TryFromReader for RaftStateMachine<S> {
    fn try_from_reader(reader: impl Read) -> Option<(Self, usize)> {
        unimplemented!()
    }
}

impl<S: StateMachine> WriteBytes for RaftStateMachine<S> {
    fn write_bytes(&self, writer: impl Write) -> io::Result<usize> {
        unimplemented!()
    }
}

impl<S: WriteBytes> WriteBytes for Snapshot<S> {
    fn write_bytes(&self, writer: impl Write) -> io::Result<usize> {
        unimplemented!()
    }
}

impl<S: TryFromReader> TryFromReader for Snapshot<S> {
    fn try_from_reader(reader: impl Read) -> Option<(Self, usize)> {
        unimplemented!()
    }
}

impl WriteBytes for Config {
    fn write_bytes(&self, writer: impl Write) -> io::Result<usize> {
        unimplemented!()
    }
}

impl TryFromBytes for Config {
    fn try_from_bytes(bytes: &[u8]) -> Option<(Self, usize)> {
        unimplemented!()
    }
}

impl<'a, C: WriteBytes> WriteBytes for OutgoingAppendEntries<'a, C> {
    fn write_bytes(&self, writer: impl Write) -> io::Result<usize> {
        unimplemented!()
    }
}

impl WriteBytes for &[u8] {
    fn write_bytes(&self, mut writer: impl Write) -> io::Result<usize> {
        let mut idx = 0;
        while idx < self.len() {
            let amt = writer.write(&self[idx..])?;
            idx += amt;
        }
        Ok(self.len())
    }
}