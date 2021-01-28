use std::collections::HashMap;
use std::io;
use std::io::{ErrorKind, Write};
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

use my_raft::bytes::{BytesWriter, ReadBytes, TryFromBytes, WriteBytes};
use my_raft::config::{Config, NodeAddress};
use my_raft::network::{ClientCommandRequest, MessageEvent, NetworkInterface};
use my_raft::state_machine::StateMachine;

const PACKET_SIZE: usize = 65527;

fn main() {}

struct UdpNetwork {
    addresses: HashMap<u32, SocketAddr>,
    socket: UdpSocket,
    write_buffer: [u8; PACKET_SIZE],
}

impl NetworkInterface<KVStore> for UdpNetwork {
    type ReadRequest = KVStoreReadRequest;

    fn on_config_update(&mut self, Config { id, nodes, .. }: &Config) {
        self.addresses.clear();

        let my_addr = get_ip_addr(nodes.get(id).unwrap());
        self.socket = UdpSocket::bind(my_addr).unwrap();
        for (id, addr) in nodes {
            self.addresses.insert(*id, get_ip_addr(addr));
        }
    }

    fn wait_for_message(&mut self, timeout: Duration, buf: &mut Vec<u8>) -> MessageEvent<KVStoreCommand, KVStoreReadRequest> {
        self.socket.set_read_timeout(Some(timeout)).unwrap();
        match self.socket.recv_from(buf) {
            Ok((.., src)) => MessageEvent::Node { src_node_id: find_node_id_given_address(&self.addresses, src) },
            Err(err) if err.kind() == ErrorKind::TimedOut => MessageEvent::Timeout,
            Err(_) => panic!()
        }
        // return MessageEvent::Request(Request {
        //     request_id: 14,
        //     client_id: 12,
        //     read: false,
        //     command: KVStoreCommand::Put("yo".to_string(), "hi".to_string()),
        // });
    }

    fn send_raft_message(&mut self, node: u32, _leader_id: Option<u32>, msg: impl WriteBytes) {
        msg.write_bytes_with_writer(self.write_buffer.as_mut()).unwrap_or_default();
        self.socket.send_to(&self.write_buffer, *self.addresses.get(&node).unwrap()).unwrap_or_default();
    }

    fn handle_command_applied(&mut self, request: ClientCommandRequest<&<KVStore as StateMachine>::Command>, state_machine: &KVStore) {
        unimplemented!()
    }

    fn handle_ready_to_read(&mut self, request: Self::ReadRequest, state_machine: &KVStore) {
        unimplemented!()
    }

    fn redirect_command_request(&mut self, leader_id: u32, request: ClientCommandRequest<<KVStore as StateMachine>::Command>) {
        unimplemented!()
    }

    fn redirect_read_request(&mut self, leader_id: u32, req: Self::ReadRequest) {
        unimplemented!()
    }
}

fn find_node_id_given_address(map: &HashMap<u32, SocketAddr>, value: SocketAddr) -> u32 {
    map.iter().find_map(|(key, &val)| if val == value { Some(*key) } else { None }).unwrap()
}

fn get_ip_addr(node: &NodeAddress) -> SocketAddr {
    match node {
        NodeAddress::SocketAddress(addr) => *addr,
        _ => panic!()
    }
}

struct KVStore(HashMap<String, String>);

impl StateMachine for KVStore {
    type Command = KVStoreCommand;

    fn apply_command(&mut self, cmd: &Self::Command) {
        match cmd {
            KVStoreCommand::Put(key, value) => { self.0.insert(key.clone(), value.clone()); }
            KVStoreCommand::Delete(key) => { self.0.remove(key); }
        };
    }
}

enum KVStoreCommand {
    Put(String, String),
    Delete(String),
}

enum KVStoreReadRequest {
    Get(String),
    Size,
}


impl WriteBytes for KVStore {
    fn write_bytes<W: Write>(&self, _writer: &mut BytesWriter<W>) -> io::Result<()> {
        unimplemented!()
    }
}

impl TryFromBytes for KVStore {
    fn try_from_bytes(bytes: impl ReadBytes) -> Option<Self> {
        unimplemented!()
    }
}

impl WriteBytes for KVStoreCommand {
    fn write_bytes<W: Write>(&self, _writer: &mut BytesWriter<W>) -> io::Result<()> {
        unimplemented!()
    }
}

impl TryFromBytes for KVStoreCommand {
    fn try_from_bytes(bytes: impl ReadBytes) -> Option<Self> {
        unimplemented!()
    }
}