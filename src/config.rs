use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use rand::{Rng, thread_rng};

use crate::timeout::Timeout;

#[derive(Clone)]
pub struct Config {
    pub election_timeout_min: u64,
    pub election_timeout_range: u64,
    pub heartbeat_timeout: u64,
    pub rpc_response_timeout: u64,
    pub max_message_bytes: u32,
    pub next_index_decrease_rate: u32,
    pub id: u32,
    pub nodes: HashMap<u32, NodeAddress>,
}

impl Config {
    pub fn new_election_timeout(&self) -> Timeout {
        Timeout::new(Duration::from_millis(thread_rng().gen_range(self.election_timeout_min..(self.election_timeout_min + self.election_timeout_range))))
    }

    pub fn new_heartbeat_timeout(&self) -> Timeout {
        Timeout::new(Duration::from_millis(self.heartbeat_timeout))
    }

    pub fn new_rpc_response_timeout(&self) -> Timeout {
        Timeout::new(Duration::from_millis(self.rpc_response_timeout))
    }

    pub fn other_node_ids(&self) -> impl Iterator<Item=u32> + '_ {
        self.nodes.iter().map(|(n, _)| *n).filter(move |n| *n != self.id)
    }
}

#[derive(Clone)]
pub enum NodeAddress {
    SocketAddress(SocketAddr),
    String(String),
    Custom(Vec<u8>),
}