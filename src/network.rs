use std::time::Duration;

use crate::bytes::WriteBytes;
use crate::config::Config;
use crate::state_machine::StateMachine;

pub trait NetworkInterface<S: StateMachine> {
    type ReadRequest;

    fn on_config_update(&mut self, config: &Config);

    fn wait_for_message(&mut self, timeout: Duration, buf: &mut [u8]) -> MessageEvent<S::Command, Self::ReadRequest>;

    fn send_raft_message(&mut self, node: u32, msg: impl WriteBytes);

    fn handle_command_executed(&mut self, client_id: u32, request_id: u32, state_machine: &S);

    fn handle_read(&mut self, client_id: u32, request_id: u32, request: Self::ReadRequest, state_machine: &S);

    fn redirect(&mut self, leader_id: u32, client_id: u32, request_id: u32);
}

pub enum MessageEvent<C, R> {
    Node { size: usize, src_node_id: u32 },
    ClientCommand(ClientRequest<C>),
    ClientRead(ClientRequest<R>),
    Timeout,
}

pub struct ClientRequest<C> {
    pub request_id: u32,
    pub client_id: u32,
    pub data: C,
}