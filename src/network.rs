use std::time::Duration;

use crate::bytes::WriteBytes;
use crate::config::Config;
use crate::state_machine::StateMachine;

pub trait NetworkInterface<S: StateMachine> {
    type ReadRequest;

    fn on_config_update(&mut self, config: &Config);

    // TODO have network interface own buf somehow?
    fn wait_for_message(&mut self, timeout: Duration, raft_message: &mut Vec<u8>) -> MessageEvent<S::Command, Self::ReadRequest>;

    fn send_raft_message(&mut self, node: u32, leader_id: Option<u32>, msg: impl WriteBytes);

    // TODO get rid of request and/or client id?
    fn handle_command_applied(&mut self, request: ClientRequest<&S::Command>, state_machine: &S);

    fn handle_ready_to_read(&mut self, request: ClientRequest<Self::ReadRequest>, state_machine: &S);

    fn redirect_command_request(&mut self, leader_id: u32, request: ClientRequest<S::Command>);

    fn redirect_read_request(&mut self, leader_id: u32, request: ClientRequest<Self::ReadRequest>);
}

pub enum MessageEvent<C, R> {
    Node { src_node_id: u32 },
    ClientCommand(ClientRequest<C>),
    ClientRead(ClientRequest<R>),
    Timeout,
    Fail
}

// TODO rethink this
pub struct ClientRequest<C> {
    pub request_id: u32,
    pub client_id: u32,
    pub data: C,
}