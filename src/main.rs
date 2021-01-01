use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io;
use std::io::{ErrorKind, Read, Write};
use std::marker::PhantomData;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::ops::Index;
use std::time::Duration;

use my_raft::config::{Config, NodeAddress};
use my_raft::core::Raft;
use my_raft::network::{ MessageEvent, NetworkInterface};
use my_raft::state_machine::StateMachine;
use my_raft::serialize::{WriteBytes, TryFromReader, TryFromBytes};

fn main() {}

struct UdpNetwork {
    addresses: HashMap<u32, SocketAddr>,
    socket: UdpSocket,
    write_buffer: Vec<u8>
}

impl NetworkInterface<KVStore> for UdpNetwork {
    type ReadRequest = KVStoreReadRequest;

    fn on_config_update(&mut self, Config { id, nodes, max_message_bytes, .. }: &Config) {
        self.addresses.clear();
        self.write_buffer.resize_with(*max_message_bytes as usize, || 0u8);

        let my_addr = get_ip_addr(nodes.get(id).unwrap());
        self.socket = UdpSocket::bind(my_addr).unwrap();
        for (id, addr) in nodes {
            self.addresses.insert(*id, get_ip_addr(addr));
        }
    }

    fn wait_for_message(&mut self, timeout: Duration, buf: &mut [u8]) -> MessageEvent<KVStoreCommand, KVStoreReadRequest> {
        self.socket.set_read_timeout(Some(timeout)).unwrap();
        match self.socket.recv_from(buf) {
            Ok((size, src)) => MessageEvent::Node { size, src_node_id: find_node_id_given_address(&self.addresses, src) },
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

    fn send_raft_message(&mut self, node: u32, msg: impl WriteBytes) {
        msg.write_bytes(&mut self.write_buffer).unwrap_or_default();
        self.socket.send_to(&self.write_buffer, *self.addresses.get(&node).unwrap()).unwrap_or_default();
    }

    fn handle_command_executed(&mut self, client_id: u32, request_id: u32, state_machine: &KVStore) {
        unimplemented!()
    }

    fn handle_read(&mut self, client_id: u32, request_id: u32, request: KVStoreReadRequest, state_machine: &KVStore) {
        match request {
            KVStoreReadRequest::Get(key) => {}
            KVStoreReadRequest::Size => {}
        }
    }

    fn redirect(&mut self, leader_id: u32, client_id: u32, request_id: u32) {
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
    fn write_bytes(&self, writer: impl Write) -> io::Result<usize> {
        unimplemented!()
    }
}

impl TryFromReader for KVStore {
    fn try_from_reader(reader: impl Read) -> Option<(Self, usize)> {
        unimplemented!()
    }
}

impl WriteBytes for KVStoreCommand {
    fn write_bytes(&self, writer: impl Write) -> io::Result<usize> {
        unimplemented!()
    }
}

impl TryFromBytes for KVStoreCommand {
    fn try_from_bytes(bytes: &[u8]) -> Option<(Self, usize)> {
        unimplemented!()
    }
}