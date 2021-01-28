use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::bytes::{BytesRef, BytesWriter, ReadBytes, TryFromBytes, WriteBytes};
use crate::config::{Config, NodeAddress};
use crate::message::{Message, OutgoingAppendEntries};
use crate::state_machine::{RaftStateMachine, StateMachine};
use crate::storage::log::{LogEntry, LogEntryType};

const LOG_ENTRY_COMMAND_ID: u8 = 0u8;
const LOG_ENTRY_CONFIG_ID: u8 = 1u8;
const LOG_ENTRY_NOOP_ID: u8 = 2u8;

const MESSAGE_APPEND_ENTRIES_ID: u8 = 0u8;
const MESSAGE_APPEND_ENTRIES_RESPONSE_ID: u8 = 1u8;
const MESSAGE_REQUEST_VOTE_ID: u8 = 2u8;
const MESSAGE_REQUEST_VOTE_RESPONSE_ID: u8 = 3u8;
const MESSAGE_INSTALL_SNAPSHOT_ID: u8 = 4u8;
const MESSAGE_INSTALL_SNAPSHOT_RESPONSE_ID: u8 = 5u8;

const NODE_ADDRESS_SOCKET: u8 = 0u8;
const NODE_ADDRESS_STRING: u8 = 1u8;
const NODE_ADDRESS_CUSTOM: u8 = 2u8;

// TODO create better write API

impl<C: TryFromBytes> TryFromBytes for LogEntry<C> {
    fn try_from_bytes(mut bytes: impl ReadBytes) -> Option<Self> {
        let term = bytes.next_u32()?;
        let entry_type = bytes.next()?;
        Some(LogEntry { term, entry_type })
    }
}

impl<C: TryFromBytes> TryFromBytes for LogEntryType<C> {
    fn try_from_bytes(mut bytes: impl ReadBytes) -> Option<Self> {
        let entry_type = match bytes.next_u8()? {
            LOG_ENTRY_COMMAND_ID => {
                let client_id = bytes.next_u32()?;
                let command_id = bytes.next_u32()?;
                let command = bytes.next()?;
                LogEntryType::Command { client_id, command_id, command }
            }
            LOG_ENTRY_CONFIG_ID => LogEntryType::Config(bytes.next()?),
            LOG_ENTRY_NOOP_ID => LogEntryType::Noop,
            _ => return None
        };

        Some(entry_type)
    }
}

impl<C: WriteBytes> WriteBytes for LogEntry<C> {
    fn write_bytes<W: Write>(&self, writer: &mut BytesWriter<W>) -> io::Result<()> {
        writer.write_u32(self.term)?;
        self.entry_type.write_bytes(writer)
    }
}

impl<C: WriteBytes> WriteBytes for LogEntryType<C> {
    fn write_bytes<W: Write>(&self, writer: &mut BytesWriter<W>) -> io::Result<()> {
        match self {
            LogEntryType::Command { client_id, command_id, command } => {
                writer.write_u8(LOG_ENTRY_COMMAND_ID)?;
                writer.write_u32(*client_id)?;
                writer.write_u32(*command_id)?;
                command.write_bytes(writer)
            }
            LogEntryType::Noop => {
                writer.write_u8(LOG_ENTRY_NOOP_ID)
            }
            LogEntryType::Config(config) => {
                writer.write_u8(LOG_ENTRY_CONFIG_ID)?;
                config.write_bytes(writer)
            }
        }
    }
}

impl<'a> Message<'a> {
    pub fn try_from_bytes(bytes: &'a [u8]) -> Option<Message<'a>> {
        let mut bytes = BytesRef::new(bytes);
        match bytes.next_u8()? {
            MESSAGE_APPEND_ENTRIES_ID =>
                Some(Message::AppendEntries {
                    term: bytes.next_u32()?,
                    prev_log_index: bytes.next_u32()?,
                    prev_log_term: bytes.next_u32()?,
                    leader_commit: bytes.next_u32()?,
                    entries: bytes.remaining_ref(),
                }),
            MESSAGE_APPEND_ENTRIES_RESPONSE_ID =>
                Some(Message::AppendEntriesResponse {
                    term: bytes.next_u32()?,
                    match_index: bytes.next_u32()?,
                    success: bytes.next_bool()?,
                }),
            MESSAGE_REQUEST_VOTE_ID =>
                Some(Message::RequestVote {
                    term: bytes.next_u32()?,
                    last_log_index: bytes.next_u32()?,
                    last_log_term: bytes.next_u32()?,
                }),
            MESSAGE_REQUEST_VOTE_RESPONSE_ID =>
                Some(Message::RequestVoteResponse {
                    term: bytes.next_u32()?,
                    vote_granted: bytes.next_bool()?,
                }),
            MESSAGE_INSTALL_SNAPSHOT_ID =>
                Some(Message::InstallSnapshot {
                    term: bytes.next_u32()?,
                    last_included_index: bytes.next_u32()?,
                    last_included_term: bytes.next_u32()?,
                    offset: bytes.next_u32()?,
                    done: bytes.next_bool()?,
                    data: bytes.remaining_ref(),
                }),
            MESSAGE_INSTALL_SNAPSHOT_RESPONSE_ID =>
                Some(Message::InstallSnapshotResponse {
                    term: bytes.next_u32()?,
                    success: bytes.next_bool()?,
                }),
            _ => None
        }
    }
}


impl<'a> WriteBytes for Message<'a> {
    fn write_bytes<W: Write>(&self, writer: &mut BytesWriter<W>) -> io::Result<()> {
        match self {
            Message::AppendEntries { term, prev_log_index, prev_log_term, leader_commit, entries } => {
                writer.write_u8(MESSAGE_APPEND_ENTRIES_ID)?;
                writer.write_u32(*term)?;
                writer.write_u32(*prev_log_index)?;
                writer.write_u32(*prev_log_term)?;
                writer.write_u32(*leader_commit)?;
                writer.write(entries)
            }
            Message::AppendEntriesResponse { term, match_index, success } => {
                writer.write_u8(MESSAGE_APPEND_ENTRIES_RESPONSE_ID)?;
                writer.write_u32(*term)?;
                writer.write_u32(*match_index)?;
                writer.write_bool(*success)
            }
            Message::RequestVote { term, last_log_index, last_log_term } => {
                writer.write_u8(MESSAGE_REQUEST_VOTE_ID)?;
                writer.write_u32(*term)?;
                writer.write_u32(*last_log_index)?;
                writer.write_u32(*last_log_term)
            }
            Message::RequestVoteResponse { term, vote_granted } => {
                writer.write_u8(MESSAGE_REQUEST_VOTE_RESPONSE_ID)?;
                writer.write_u32(*term)?;
                writer.write_bool(*vote_granted)
            }
            Message::InstallSnapshot { term, last_included_index, last_included_term, offset, done, data } => {
                writer.write_u8(MESSAGE_INSTALL_SNAPSHOT_ID)?;
                writer.write_u32(*term)?;
                writer.write_u32(*last_included_index)?;
                writer.write_u32(*last_included_term)?;
                writer.write_u32(*offset)?;
                writer.write_bool(*done)?;
                writer.write(data)
            }
            Message::InstallSnapshotResponse { term, success } => {
                writer.write_u8(MESSAGE_INSTALL_SNAPSHOT_RESPONSE_ID)?;
                writer.write_u32(*term)?;
                writer.write_bool(*success)
            }
        }
    }
}

impl<S: StateMachine> TryFromBytes for RaftStateMachine<S> {
    fn try_from_bytes(mut bytes: impl ReadBytes) -> Option<Self> {
        let num_client_command_ids = bytes.next_u32()? as usize;
        let mut client_last_command_ids = HashMap::with_capacity(num_client_command_ids);

        for _ in 0..num_client_command_ids {
            let client_id = bytes.next_u32()?;
            let command_id = bytes.next_u32()?;
            client_last_command_ids.insert(client_id, command_id);
        }

        let config = Config::try_from_bytes(&mut bytes)?;
        let inner = S::try_from_bytes(&mut bytes)?;

        Some(RaftStateMachine {
            config,
            inner,
            client_last_command_ids,
        })
    }
}

impl<S: StateMachine> WriteBytes for RaftStateMachine<S> {
    fn write_bytes<W: Write>(&self, writer: &mut BytesWriter<W>) -> io::Result<()> {
        writer.write_u32(self.client_last_command_ids.len() as u32)?;
        for (client_id, command_id) in &self.client_last_command_ids {
            writer.write_u32(*client_id)?;
            writer.write_u32(*command_id)?;
        }

        self.config.write_bytes(writer)?;
        self.inner.write_bytes(writer)
    }
}

impl WriteBytes for Config {
    fn write_bytes<W: Write>(&self, writer: &mut BytesWriter<W>) -> io::Result<()> {
        let Config {
            election_timeout_min,
            election_timeout_range,
            heartbeat_timeout,
            rpc_response_timeout,
            max_entries_in_append_entries,
            max_bytes_in_install_snapshot,
            next_index_decrease_rate,
            snapshot_min_log_size,
            id,
            nodes
        } = self;
        writer.write_u64(*election_timeout_min)?;
        writer.write_u64(*election_timeout_range)?;
        writer.write_u64(*heartbeat_timeout)?;
        writer.write_u64(*rpc_response_timeout)?;
        writer.write_u32(*max_entries_in_append_entries)?;
        writer.write_u32(*max_bytes_in_install_snapshot)?;
        writer.write_u32(*next_index_decrease_rate)?;
        writer.write_u32(*snapshot_min_log_size)?;
        writer.write_u32(*id)?;
        writer.write_u32(nodes.len() as u32)?;
        for (id, addr) in nodes {
            writer.write_u32(*id)?;
            addr.write_bytes(writer)?;
        }
        Ok(())
    }
}

impl TryFromBytes for Config {
    fn try_from_bytes(mut bytes: impl ReadBytes) -> Option<Self> {
        let election_timeout_min = bytes.next_u64()?;
        let election_timeout_range = bytes.next_u64()?;
        let heartbeat_timeout = bytes.next_u64()?;
        let rpc_response_timeout = bytes.next_u64()?;
        let max_entries_in_append_entries = bytes.next_u32()?;
        let max_bytes_in_install_snapshot = bytes.next_u32()?;
        let next_index_decrease_rate = bytes.next_u32()?;
        let snapshot_min_log_size = bytes.next_u32()?;
        let id = bytes.next_u32()?;
        let nodes_len = bytes.next_u32()?;

        let mut nodes = HashMap::new();

        for _ in 0..nodes_len {
            let id = bytes.next_u32()?;
            let address = bytes.next()?;
            nodes.insert(id, address);
        }

        Some(Config {
            election_timeout_min,
            election_timeout_range,
            heartbeat_timeout,
            rpc_response_timeout,
            max_entries_in_append_entries,
            max_bytes_in_install_snapshot,
            next_index_decrease_rate,
            snapshot_min_log_size,
            id,
            nodes,
        })
    }
}

impl TryFromBytes for NodeAddress {
    fn try_from_bytes(mut bytes: impl ReadBytes) -> Option<Self> {
        let addr = match bytes.next_u8()? {
            NODE_ADDRESS_SOCKET => {
                let ip = match bytes.next_u8()? {
                    4 => Ipv4Addr::from(bytes.next_u32()?).into(),
                    16 => Ipv6Addr::from(bytes.next_u128()?).into(),
                    _ => return None
                };
                let port = bytes.next_u16()?;
                NodeAddress::SocketAddress(SocketAddr::new(ip, port))
            }
            NODE_ADDRESS_STRING => {
                let len = bytes.next_u32()?;
                let str = String::from_utf8_lossy(bytes.next_bytes(len as usize)?).to_string();
                NodeAddress::String(str)
            }
            NODE_ADDRESS_CUSTOM => {
                let len = bytes.next_u32()?;
                let vec = bytes.next_bytes(len as usize)?.to_vec();
                NodeAddress::Custom(vec)
            }
            _ => return None
        };
        Some(addr)
    }
}

impl WriteBytes for NodeAddress {
    fn write_bytes<W: Write>(&self, writer: &mut BytesWriter<W>) -> io::Result<()> {
        match self {
            NodeAddress::SocketAddress(addr) => {
                writer.write_u8(NODE_ADDRESS_SOCKET)?;
                match addr {
                    SocketAddr::V4(addr) => {
                        let octets = addr.ip().octets();
                        writer.write_u8(octets.len() as u8)?;
                        writer.write(&octets)?;
                    }
                    SocketAddr::V6(addr) => {
                        let octets = addr.ip().octets();
                        writer.write_u8(octets.len() as u8)?;
                        writer.write(&octets)?;
                    }
                };
                writer.write_u16(addr.port())
            }
            NodeAddress::String(str) => {
                writer.write_u8(NODE_ADDRESS_STRING)?;
                writer.write_u32(str.len() as u32)?;
                writer.write(str.as_bytes())
            }
            NodeAddress::Custom(bytes) => {
                writer.write_u8(NODE_ADDRESS_CUSTOM)?;
                writer.write_u32(bytes.len() as u32)?;
                writer.write(bytes)
            }
        }
    }
}

impl<'a, C: WriteBytes> WriteBytes for OutgoingAppendEntries<'a, C> {
    fn write_bytes<W: Write>(&self, writer: &mut BytesWriter<W>) -> io::Result<()> {
        writer.write_u8(MESSAGE_APPEND_ENTRIES_ID)?;
        writer.write_u32(self.term)?;
        writer.write_u32(self.prev_log_index)?;
        writer.write_u32(self.prev_log_term)?;
        writer.write_u32(self.leader_commit)?;
        for entry in self.entries {
            entry.write_bytes(writer)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io;
    use std::io::Write;

    use crate::bytes::{BytesIterator, BytesRef, BytesWriter, ReadBytes, TryFromBytes, WriteBytes};
    use crate::config::{Config, NodeAddress};
    use crate::message::{Message, OutgoingAppendEntries};
    use crate::state_machine::{RaftStateMachine, StateMachine};
    use crate::storage::log::{LogEntry, LogEntryType};

    #[derive(Eq, PartialEq, Debug)]
    struct DummyData(Vec<u8>);

    impl WriteBytes for DummyData {
        fn write_bytes<W: Write>(&self, writer: &mut BytesWriter<W>) -> io::Result<()> {
            writer.write_u32(self.0.len() as u32)?;
            writer.write(&self.0)
        }
    }

    impl TryFromBytes for DummyData {
        fn try_from_bytes(mut bytes: impl ReadBytes) -> Option<Self> {
            let size = bytes.next_u32()?;
            let vec = bytes.next_bytes(size as usize)?.to_vec();
            Some(DummyData(vec))
        }
    }

    impl StateMachine for DummyData {
        type Command = DummyData;

        fn apply_command(&mut self, command: &Self::Command) {
            self.0.extend_from_slice(&command.0);
        }
    }

    fn dummy_config() -> Config {
        let mut nodes = HashMap::new();
        nodes.insert(1, NodeAddress::SocketAddress("1.4.32.6:4356".parse().unwrap()));
        nodes.insert(11, NodeAddress::SocketAddress("0.0.0.0:0".parse().unwrap()));
        nodes.insert(2, NodeAddress::SocketAddress("[4325:5242:ABCD:4356:FFFF:0000:FEFE:1234]:423".parse().unwrap()));
        nodes.insert(22, NodeAddress::SocketAddress("[0:0:0:0:0:0:0:0]:0".parse().unwrap()));
        nodes.insert(3, NodeAddress::String("helllooo".to_string()));
        nodes.insert(4, NodeAddress::String("".to_string()));
        nodes.insert(545345, NodeAddress::String("".to_string()));
        nodes.insert(32, NodeAddress::Custom(vec![]));
        nodes.insert(5436, NodeAddress::Custom(vec![0, 3, 41, 1, 5, 3, 6, 3, 0, 0, 0]));
        Config {
            election_timeout_min: 4530000006,
            election_timeout_range: 654743008347,
            heartbeat_timeout: 564563,
            rpc_response_timeout: 3463462,
            max_bytes_in_install_snapshot: 24532573,
            max_entries_in_append_entries: 54345,
            next_index_decrease_rate: 543,
            snapshot_min_log_size: 54326,
            id: 986543,
            nodes,
        }
    }

    fn dummy_config_no_nodes() -> Config {
        Config {
            election_timeout_min: 40006,
            election_timeout_range: 6530047,
            heartbeat_timeout: 564563,
            rpc_response_timeout: 3463462,
            max_bytes_in_install_snapshot: 32,
            max_entries_in_append_entries: 543,
            next_index_decrease_rate: 543,
            snapshot_min_log_size: 54326,
            id: 986543,
            nodes: HashMap::new(),
        }
    }

    fn test_log_entries() -> Vec<LogEntry<DummyData>> {
        vec![
            LogEntry { term: 452, entry_type: LogEntryType::Command { command: DummyData("hello".as_bytes().to_vec()), client_id: 42, command_id: 0 } },
            LogEntry { term: 0, entry_type: LogEntryType::Noop },
            LogEntry { term: 1, entry_type: LogEntryType::Config(dummy_config()) },
            LogEntry { term: 32, entry_type: LogEntryType::Command { command: DummyData("yo".as_bytes().to_vec()), client_id: 0, command_id: 98 } },
            LogEntry { term: 542, entry_type: LogEntryType::Config(dummy_config()) },
            LogEntry { term: 64, entry_type: LogEntryType::Command { command: DummyData("sethefwegfwrehg".as_bytes().to_vec()), client_id: 432, command_id: 7000 } },
            LogEntry { term: 3, entry_type: LogEntryType::Noop },
            LogEntry { term: 9999, entry_type: LogEntryType::Config(dummy_config_no_nodes()) },
        ]
    }

    fn assert_entries_equal(a: &LogEntry<DummyData>, b: &LogEntry<DummyData>) {
        assert_eq!(a.term, b.term);
        match (&a.entry_type, &b.entry_type) {
            (LogEntryType::Command { command: a_command, client_id: a_client_id, command_id: a_command_id },
                LogEntryType::Command { command: b_command, client_id: b_client_id, command_id: b_command_id }) => {
                assert_eq!(a_command, b_command);
                assert_eq!(a_client_id, b_client_id);
                assert_eq!(a_command_id, b_command_id);
            }
            (LogEntryType::Config(a_config), LogEntryType::Config(b_config)) => {
                assert_eq!(a_config, b_config)
            }
            (LogEntryType::Noop, LogEntryType::Noop) => {}
            _ => panic!()
        }
    }

    #[test]
    fn log_entry_command() {
        let expected = LogEntry { term: 452, entry_type: LogEntryType::Command { command: DummyData("hello".as_bytes().to_vec()), client_id: 42, command_id: 0 } };
        let mut buf = vec![];
        expected.write_bytes_with_writer(&mut buf).unwrap();
        let actual = LogEntry::try_from_slice(&buf).unwrap();
        assert_entries_equal(&actual, &expected);
    }

    #[test]
    fn read_log_entry_command_more_bytes_after() {
        let expected = LogEntry { term: 452, entry_type: LogEntryType::Command { command: DummyData("hello".as_bytes().to_vec()), client_id: 42, command_id: 0 } };
        let mut buf = vec![];

        let write_amt = expected.write_bytes_with_writer(&mut buf).unwrap();
        buf.write(&[32, 0, 5, 43, 1, 34, 4, 3, 53, 13, 4]).unwrap();

        let mut bytes = BytesRef::new(&buf);
        let actual = LogEntry::try_from_bytes(&mut bytes).unwrap();

        assert_eq!(write_amt, bytes.read_amount());
        assert_entries_equal(&actual, &expected);
    }

    #[test]
    fn read_log_entry_command_too_few_bytes() {
        let mut buf = vec![];
        buf.write(&[32, 34, 2]).unwrap();
        assert!(LogEntry::<DummyData>::try_from_slice(&buf).is_none())
    }

    #[test]
    fn read_log_entry_command_no_bytes() {
        assert!(LogEntry::<DummyData>::try_from_slice(&[]).is_none())
    }

    #[test]
    fn read_log_entries_iterator() {
        let entries = test_log_entries();

        let mut buf = vec![];

        for entry in &entries {
            entry.write_bytes_with_writer(&mut buf).unwrap();
        }

        let mut bytes = BytesRef::new(&buf);
        let mut iter = bytes.iter();
        for expected in &entries {
            assert_entries_equal(&expected, &iter.next().unwrap());
        }

        assert!(iter.next().is_none());
    }

    #[test]
    fn read_log_entries_iterator_no_bytes() {
        let mut bytes = BytesRef::new(&[]);
        let mut iter: BytesIterator<&mut BytesRef, DummyData> = bytes.iter();
        assert!(iter.next().is_none());
    }

    #[test]
    fn messages() {
        let messages = vec![
            Message::AppendEntries {
                term: 32,
                prev_log_index: 54,
                prev_log_term: 463,
                leader_commit: 234,
                entries: &[43, 64, 0, 3, 5, 1],
            },
            Message::AppendEntries {
                term: 32,
                prev_log_index: 54,
                prev_log_term: 463,
                leader_commit: 234,
                entries: &[],
            },
            Message::AppendEntriesResponse {
                term: 4432,
                success: false,
                match_index: 32,
            },
            Message::AppendEntriesResponse {
                term: 4432,
                success: true,
                match_index: 32,
            },
            Message::RequestVote {
                term: 4432,
                last_log_index: 564854,
                last_log_term: 3425,
            },
            Message::RequestVoteResponse {
                term: 0,
                vote_granted: false,
            },
            Message::RequestVoteResponse {
                term: 435,
                vote_granted: true,
            },
            Message::InstallSnapshot {
                term: 3452,
                last_included_index: 325,
                last_included_term: 5436,
                offset: 53425,
                data: &[1, 2, 3, 4, 5, 6, 67, 7, 8, 9, 0],
                done: false,
            },
            Message::InstallSnapshot {
                term: 3452,
                last_included_index: 325,
                last_included_term: 5436,
                offset: 53425,
                data: &[],
                done: true,
            },
            Message::InstallSnapshotResponse {
                term: 32,
                success: true,
            },
            Message::InstallSnapshotResponse {
                term: 3200,
                success: false,
            }
        ];
        let mut buf = vec![];
        for expected in messages {
            buf.clear();
            let amt = expected.write_bytes_with_writer(&mut buf).unwrap();
            let actual = Message::try_from_bytes(&buf[..amt]).unwrap();
            assert_eq!(actual, expected)
        }
    }

    #[test]
    fn read_empty_message_bytes() {
        assert!(Message::try_from_bytes(&[]).is_none())
    }

    #[test]
    fn invalid_message_id() {
        assert!(Message::try_from_bytes(&[6u8]).is_none())
    }

    #[test]
    fn too_few_message_bytes() {
        assert!(Message::try_from_bytes(&[0u8, 5u8, 3u8, 1u8]).is_none())
    }

    #[test]
    fn outgoing_append_entries() {
        let entries = test_log_entries();

        let append_entries = OutgoingAppendEntries {
            term: 43,
            prev_log_index: 523,
            prev_log_term: 4,
            leader_commit: 13,
            entries: &entries,
        };

        let mut buf = vec![];
        let amt = append_entries.write_bytes_with_writer(&mut buf).unwrap();

        let message = Message::try_from_bytes(&buf[..amt]).unwrap();

        if let Message::AppendEntries { term, prev_log_index, prev_log_term, leader_commit, entries: entry_bytes } = message {
            assert_eq!(term, append_entries.term);
            assert_eq!(prev_log_index, append_entries.prev_log_index);
            assert_eq!(prev_log_term, append_entries.prev_log_term);
            assert_eq!(leader_commit, append_entries.leader_commit);

            let mut bytes = BytesRef::new(entry_bytes);
            let mut iter = bytes.iter();
            for expected in entries {
                assert_entries_equal(&expected, &iter.next().unwrap());
            }
            assert!(iter.next().is_none());
        } else {
            panic!()
        }
    }

    #[test]
    fn state_machine() {
        let mut client_last_command_ids = HashMap::new();

        client_last_command_ids.insert(432, 6543);
        client_last_command_ids.insert(1, 432);
        client_last_command_ids.insert(0, 99);
        client_last_command_ids.insert(3, 2353466235);

        let expected = RaftStateMachine {
            inner: DummyData("hewfiojweiuge8394tv p93q8vty4vqon39847tpvq984thiwjfgoeiugpoqw0".as_bytes().to_vec()),
            config: dummy_config(),
            client_last_command_ids,
        };

        let mut buf = vec![];

        let write_amt = expected.write_bytes_with_writer(&mut buf).unwrap();

        let mut bytes = BytesRef::new(&buf[..write_amt]);
        let actual = RaftStateMachine::<DummyData>::try_from_bytes(&mut bytes).unwrap();

        assert_eq!(write_amt, bytes.read_amount());
        assert_eq!(actual.client_last_command_ids, expected.client_last_command_ids);
        assert_eq!(actual.config, expected.config);
        assert_eq!(actual.inner, expected.inner);
    }

    #[test]
    fn empty_state_machine() {
        let expected = RaftStateMachine {
            inner: DummyData(vec![]),
            config: dummy_config_no_nodes(),
            client_last_command_ids: HashMap::new(),
        };

        let mut buf = vec![];

        let write_amt = expected.write_bytes_with_writer(&mut buf).unwrap();

        let mut bytes = BytesRef::new(&buf[..write_amt]);
        let actual = RaftStateMachine::<DummyData>::try_from_bytes(&mut bytes).unwrap();

        assert_eq!(write_amt, bytes.read_amount());
        assert_eq!(actual.client_last_command_ids, expected.client_last_command_ids);
        assert_eq!(actual.config, expected.config);
        assert_eq!(actual.inner, expected.inner);
    }
}