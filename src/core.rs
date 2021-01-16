use std::collections::HashMap;
use std::option::Option::Some;
use std::time::Duration;

use crate::bytes::{BytesRef, ReadBytes};
use crate::config::Config;
use crate::message::{Message, OutgoingAppendEntries};
use crate::network::{ClientRequest, MessageEvent, NetworkInterface};
use crate::state_machine::{RaftStateMachine, StateMachine};
use crate::storage::{RaftStorage, Storage};
use crate::storage::log::{LogEntry, LogEntryType, LogMut, LogRef};
use crate::timeout::Timeout;

pub struct Raft<S: StateMachine, P: Storage<S>, N: NetworkInterface<S>> {
    storage: RaftStorage<S, P>,
    network: N,
    commit_index: u32,
    state_machine: RaftStateMachine<S>,
    role: Role,
    command_request_queue: Vec<ClientRequest<S::Command>>,
    read_request_queue: Vec<ClientRequest<N::ReadRequest>>,
}

enum Role {
    Leader {
        noop_index: u32,
        views: HashMap<u32, LeaderView>,
    },
    Candidate {
        election_timeout: Timeout,
        views: HashMap<u32, CandidateView>,
    },
    Follower {
        election_timeout: Timeout,
        leader_id: Option<u32>,
    },
}

#[derive(Copy, Clone)]
struct LeaderView {
    match_index: u32,
    next_beat: Timeout,
    outstanding_rpc: Option<OutstandingRPC>,
}

#[derive(Copy, Clone)]
enum OutstandingRPC {
    AppendEntries { index: u32 },
    InstallSnapshot { offset: u32, amt: u32 },
}

#[derive(Copy, Clone)]
enum CandidateView {
    VoteGranted,
    VoteDenied,
    Waiting(Timeout),
}

#[derive(Copy, Clone)]
enum TimeoutType {
    ElectionTimeout,
    HeartbeatTimeout(u32),
    RequestVoteTimeout(u32),
}

enum Event<'a, C, R> {
    Timeout(TimeoutType),
    NodeMessage(u32, Message<'a>),
    ClientCommand(ClientRequest<C>),
    ClientRead(ClientRequest<R>),
}

impl<S: StateMachine, P: Storage<S>, N: NetworkInterface<S>> Raft<S, P, N> {
    pub fn new(storage: P, mut network: N) -> Raft<S, P, N> {
        let storage = RaftStorage::new(storage);

        let state_machine = storage.inner().snapshot();
        let config = storage.get_last_config_in_log().unwrap_or(&state_machine.config);

        network.on_config_update(config);

        let election_timeout = config.new_election_timeout();
        let commit_index = storage.inner().snapshot_last_index();

        Raft {
            storage,
            network,
            commit_index,
            state_machine,
            role: Role::Follower { election_timeout, leader_id: None },
            read_request_queue: vec![],
            command_request_queue: vec![],
        }
    }

    pub fn start(&mut self) {
        let mut msg_buf = vec![];

        loop {
            msg_buf.clear();
            if let Some(event) = self.next_event(&mut msg_buf) {
                self.handle_event(event);
            }
        }
    }

    fn next_event<'a>(&mut self, msg_buf: &'a mut Vec<u8>) -> Option<Event<'a, S::Command, N::ReadRequest>> {
        let (timeout, timeout_type) = self.next_timeout();

        let time_left = timeout.time_left();

        if time_left == Duration::from_secs(0) {
            return Some(Event::Timeout(timeout_type));
        }

        match self.network.wait_for_message(time_left, msg_buf) {
            MessageEvent::Node { src_node_id } =>
                Message::try_from_bytes(msg_buf).map(|m| Event::NodeMessage(src_node_id, m)),
            MessageEvent::ClientCommand(req) => Some(Event::ClientCommand(req)),
            MessageEvent::ClientRead(req) => Some(Event::ClientRead(req)),
            MessageEvent::Timeout => Some(Event::Timeout(timeout_type)),
            MessageEvent::Fail => None
        }
    }

    fn next_timeout(&self) -> (Timeout, TimeoutType) {
        match &self.role {
            Role::Leader { views, .. } =>
                views.iter()
                    .map(|(n, v)| (n, v.next_beat))
                    .min_by_key(|(_, to)| *to)
                    .map(|(n, to)| (to, TimeoutType::HeartbeatTimeout(*n)))
                    .unwrap(),
            Role::Candidate { election_timeout, views } => {
                let request_vote_timeout = views.iter()
                    .filter_map(|(n, v)| v.next_request_vote_timeout().map(|to| (n, to)))
                    .min_by_key(|(_, to)| *to)
                    .map(|(n, to)| (to, TimeoutType::RequestVoteTimeout(*n)));

                match request_vote_timeout {
                    Some(to) if to.0 < *election_timeout => to,
                    _ => (*election_timeout, TimeoutType::ElectionTimeout)
                }
            }
            Role::Follower { election_timeout, .. } => (*election_timeout, TimeoutType::ElectionTimeout)
        }
    }

    fn handle_event(&mut self, event: Event<S::Command, N::ReadRequest>) {
        match event {
            Event::Timeout(timeout_type) => self.handle_timeout(timeout_type),
            Event::NodeMessage(src_node_id, msg) => self.handle_node_message(src_node_id, msg),
            Event::ClientCommand(command_request) => self.handle_client_command_request(command_request),
            Event::ClientRead(read_request) => self.handle_client_read_request(read_request)
        }
    }

    fn handle_timeout(&mut self, timeout_type: TimeoutType) {
        match timeout_type {
            TimeoutType::ElectionTimeout => self.handle_election_timeout(),
            TimeoutType::HeartbeatTimeout(node_id) => self.handle_heartbeat_timeout(node_id),
            TimeoutType::RequestVoteTimeout(node_id) => self.handle_request_vote_timeout(node_id),
        }
    }

    fn handle_node_message(&mut self, src_node_id: u32, msg: Message) {
        match msg {
            Message::AppendEntries { term, prev_log_index, prev_log_term, leader_commit, entries } =>
                self.handle_append_entries(src_node_id, term, prev_log_index, prev_log_term, leader_commit, entries),
            Message::AppendEntriesResponse { term, success, match_index } =>
                self.handle_append_entries_response(src_node_id, term, success, match_index),
            Message::RequestVote { term, last_log_index, last_log_term } =>
                self.handle_request_vote(src_node_id, term, last_log_index, last_log_term),
            Message::RequestVoteResponse { term, vote_granted } =>
                self.handle_request_vote_response(src_node_id, term, vote_granted),
            Message::InstallSnapshot { term, last_included_index, last_included_term, offset, data, done } =>
                self.handle_install_snapshot(src_node_id, term, last_included_index, last_included_term, offset, data, done),
            Message::InstallSnapshotResponse { term, success } =>
                self.handle_install_snapshot_response(src_node_id, term, success)
        }
    }

    // TODO handle config entry
    fn handle_append_entries(&mut self, src_node_id: u32, term: u32, prev_log_index: u32, prev_log_term: u32, leader_commit: u32, entries: &[u8]) {
        if self.check_term_and_update_self(term) {
            self.receive_message_from_leader(src_node_id);

            if let Some(match_index) = self.storage.log_mut().try_append_entries(prev_log_index, prev_log_term, BytesRef::new(entries).iter()) {
                self.network.send_raft_message(src_node_id, Some(src_node_id), Message::AppendEntriesResponse { term: self.storage.inner().current_term(), success: true, match_index });

                update_commit_index(leader_commit, &mut self.commit_index, self.storage.log(), &mut self.state_machine);
                try_make_snapshot(&mut self.storage, &self.state_machine, self.commit_index);

                return;
            }
        }

        self.network.send_raft_message(src_node_id, self.get_leader_id(), Message::AppendEntriesResponse { term: self.storage.inner().current_term(), success: false, match_index: 0 })
    }

    fn handle_append_entries_response(&mut self, src_node_id: u32, term: u32, success: bool, match_index: u32) {
        if !self.check_term_and_update_self(term) {
            return;
        }

        // TODO clean all this up
        if let Role::Leader { ref mut views, noop_index } = self.role {
            let view = views.get_mut(&src_node_id).unwrap();
            let config = config(&self.storage, &self.state_machine);

            let mut next_index = match view.outstanding_rpc {
                Some(OutstandingRPC::AppendEntries { index }) => index,
                _ => panic!()
            };

            if success {
                view.match_index = match_index;
                next_index = match_index + 1;
            } else {
                next_index -= config.next_index_decrease_rate.min(next_index - 1);
            }

            if next_index <= self.storage.log().last_index() {
                if !view.try_send_append_entries(src_node_id, &mut self.network, &self.storage, config, self.commit_index, next_index) {
                    view.send_install_snapshot(src_node_id, &mut self.network, &self.storage, config, 0);
                }
            } else {
                view.outstanding_rpc = None;
                view.next_beat = config.new_heartbeat_timeout();
            }

            // TODO this is kinda poopy
            if success {
                let new_commit_index = max_commit(views, self.storage.log(), self.storage.inner().current_term(), self.commit_index);
                for entry in update_commit_index(new_commit_index, &mut self.commit_index, self.storage.log(), &mut self.state_machine) {
                    if let LogEntryType::Command { client_id, command_id, ref command } = entry.entry_type {
                        self.network.handle_command_applied(ClientRequest { client_id, request_id: command_id, data: command }, &self.state_machine.inner)
                    }
                }

                if self.commit_index >= noop_index {
                    for req in self.read_request_queue.drain(..) {
                        self.network.handle_ready_to_read(req, &self.state_machine.inner);
                    }
                }

                try_make_snapshot(&mut self.storage, &self.state_machine, self.commit_index);
            }
        }
    }

    fn handle_request_vote(&mut self, src_node_id: u32, term: u32, last_log_index: u32, last_log_term: u32) {
        let mut grant_vote = false;

        if self.check_term_and_update_self(term) {
            let new_election_timeout = self.config().new_election_timeout();

            if let Role::Follower { ref mut election_timeout, .. } = self.role {
                if last_log_term >= self.storage.log().last_term() && last_log_index >= self.storage.log().last_index() {
                    grant_vote = match self.storage.inner().voted_for() {
                        Some(id) if id == src_node_id => true,
                        None => true,
                        _ => false
                    };

                    if grant_vote {
                        self.storage.set_voted_for(Some(src_node_id));
                        *election_timeout = new_election_timeout;
                    }
                }
            }
        }

        self.network.send_raft_message(src_node_id, self.get_leader_id(), Message::RequestVoteResponse { term: self.storage.inner().current_term(), vote_granted: grant_vote });
    }

    fn handle_request_vote_response(&mut self, src_node_id: u32, term: u32, vote_granted: bool) {
        if !self.check_term_and_update_self(term) {
            return;
        }

        let become_leader = if let Role::Candidate { ref mut views, .. } = self.role {
            views.insert(src_node_id, if vote_granted { CandidateView::VoteGranted } else { CandidateView::VoteDenied });
            has_majority_votes(views)
        } else {
            false
        };

        if become_leader {
            let next_index = self.storage.log().last_index() + 1;
            self.role = Role::Leader {
                noop_index: next_index,
                views: self.config().other_node_ids().map(|id| (id, LeaderView {
                    match_index: 0,
                    next_beat: self.config().new_rpc_response_timeout(),
                    outstanding_rpc: Some(OutstandingRPC::AppendEntries { index: next_index }),
                })).collect(),
            };

            self.storage.log_mut().add_entry(LogEntryType::Noop);
            for req in self.command_request_queue.drain(..) {
                try_append_new_client_command_to_log(&mut self.network, self.storage.log_mut(), self.commit_index, &self.state_machine, req);
            }

            let config = config(&self.storage, &self.state_machine);
            for other_id in config.other_node_ids() {
                try_send_append_entries(other_id, config.id, &mut self.network, self.storage.log(), self.storage.inner().current_term(), self.commit_index, next_index);
            }
        }
    }

    fn handle_install_snapshot(&mut self, src_node_id: u32, term: u32, last_included_index: u32, last_included_term: u32, offset: u32, data: &[u8], done: bool) {
        let success = if self.check_term_and_update_self(term) {
            self.receive_message_from_leader(src_node_id);

            self.storage.add_new_snapshot_chunk(offset, data);

            if done {
                if let Some(new_state_machine) = self.storage.try_use_chunks_as_new_snapshot(last_included_index, last_included_term) {
                    self.storage.log_mut().remove_entries_before(last_included_index + 1);
                    self.state_machine = new_state_machine;
                    self.commit_index = last_included_index;
                    self.network.on_config_update(config(&self.storage, &self.state_machine));
                }
            }

            true
        } else {
            false
        };

        self.network.send_raft_message(src_node_id, self.get_leader_id(), Message::InstallSnapshotResponse { success, term: self.storage.inner().current_term() });
    }

    fn handle_install_snapshot_response(&mut self, src_node_id: u32, term: u32, success: bool) {
        if self.check_term_and_update_self(term) {
            if let Role::Leader { ref mut views, .. } = self.role {
                let view = views.get_mut(&src_node_id).unwrap();
                let config = config(&self.storage, &self.state_machine);

                if success {
                    let offset = match view.outstanding_rpc {
                        Some(OutstandingRPC::InstallSnapshot { offset, amt }) => offset + amt,
                        _ => panic!()
                    };

                    if offset >= self.storage.inner().total_snapshot_bytes() {
                        view.try_send_append_entries(src_node_id, &mut self.network, &self.storage, &config, self.commit_index, self.storage.inner().snapshot_last_index() + 1);
                    } else {
                        view.send_install_snapshot(src_node_id, &mut self.network, &self.storage, &config, offset);
                    }
                } else {
                    view.outstanding_rpc = None;
                    view.next_beat = config.new_heartbeat_timeout();
                }
            }
        }
    }

    fn handle_client_command_request(&mut self, req: ClientRequest<S::Command>) {
        if let Role::Leader { ref mut views, .. } = self.role {
            if try_append_new_client_command_to_log(&mut self.network, self.storage.log_mut(), self.commit_index, &self.state_machine, req) {
                let config = config(&self.storage, &self.state_machine);

                for (other_id, view) in views {
                    if view.outstanding_rpc.is_none() {
                        view.try_send_append_entries(*other_id, &mut self.network, &self.storage, &config, self.commit_index, self.storage.log().last_index());
                    }
                }
            }
        } else if let Role::Follower { leader_id: Some(leader_id), .. } = self.role {
            self.network.redirect_command_request(leader_id, req);
        } else {
            self.command_request_queue.push(req);
        }
    }

    fn handle_client_read_request(&mut self, req: ClientRequest<N::ReadRequest>) {
        match self.role {
            Role::Leader { noop_index, .. } if self.commit_index >= noop_index => self.network.handle_ready_to_read(req, &self.state_machine.inner),
            Role::Follower { leader_id: Some(leader_id), .. } => self.network.redirect_read_request(leader_id, req),
            _ => {
                self.read_request_queue.push(req);
            }
        }
    }

    fn handle_election_timeout(&mut self) {
        self.storage.set_current_term(self.storage.inner().current_term() + 1);
        self.storage.set_voted_for(None);
        self.role = Role::Candidate {
            election_timeout: self.config().new_election_timeout(),
            views: self.config().other_node_ids().map(|n| (n, CandidateView::Waiting(self.config().new_rpc_response_timeout()))).collect(),
        };
        for node_id in config(&self.storage, &self.state_machine).other_node_ids() {
            send_request_vote(&mut self.network, self.storage.log(), self.storage.inner().current_term(), node_id);
        }
    }

    fn handle_request_vote_timeout(&mut self, node_id: u32) {
        let timeout = self.config().new_rpc_response_timeout();
        if let Role::Candidate { ref mut views, .. } = self.role {
            views.entry(node_id).and_modify(|view| *view = CandidateView::Waiting(timeout));
            send_request_vote(&mut self.network, self.storage.log(), self.storage.inner().current_term(), node_id);
        }
    }

    fn handle_heartbeat_timeout(&mut self, node_id: u32) {
        if let Role::Leader { ref mut views, .. } = self.role {
            let view = views.get_mut(&node_id).unwrap();
            let config = config(&self.storage, &self.state_machine);

            if let Some(OutstandingRPC::InstallSnapshot { offset, .. }) = view.outstanding_rpc {
                view.send_install_snapshot(node_id, &mut self.network, &self.storage, config, offset);
            } else {
                let index = match view.outstanding_rpc {
                    Some(OutstandingRPC::AppendEntries { index }) => index,
                    _ => self.storage.log().last_index() + 1
                };
                view.try_send_append_entries(node_id, &mut self.network, &self.storage, config, self.commit_index, index);
            }
        }
    }

    fn config(&self) -> &Config {
        config(&self.storage, &self.state_machine)
    }

    fn check_term_and_update_self(&mut self, new_term: u32) -> bool {
        if new_term < self.storage.inner().current_term() {
            return false;
        } else if new_term > self.storage.inner().current_term() {
            self.storage.set_current_term(new_term);
            self.storage.set_voted_for(None);

            self.role = Role::Follower { election_timeout: self.config().new_election_timeout(), leader_id: None };
        }
        true
    }

    fn receive_message_from_leader(&mut self, leader_id: u32) {
        self.role = Role::Follower { election_timeout: self.config().new_election_timeout(), leader_id: Some(leader_id) };

        for req in self.read_request_queue.drain(..) {
            self.network.redirect_read_request(leader_id, req);
        }

        for req in self.command_request_queue.drain(..) {
            self.network.redirect_command_request(leader_id, req);
        }
    }

    fn get_leader_id(&self) -> Option<u32> {
        match self.role {
            Role::Follower { leader_id, .. } => leader_id,
            Role::Leader { .. } => Some(self.config().id),
            _ => None
        }
    }
}

impl CandidateView {
    fn next_request_vote_timeout(&self) -> Option<Timeout> {
        match self {
            CandidateView::VoteGranted => None,
            CandidateView::VoteDenied => None,
            CandidateView::Waiting(timeout) => Some(*timeout)
        }
    }
}

impl LeaderView {
    fn try_send_append_entries<S: StateMachine, P: Storage<S>, N: NetworkInterface<S>>
    (&mut self, node_id: u32, network: &mut N, storage: &RaftStorage<S, P>, config: &Config, commit_index: u32, index: u32) -> bool {
        if try_send_append_entries(node_id, config.id, network, storage.log(), storage.inner().current_term(), commit_index, index) {
            self.next_beat = config.new_rpc_response_timeout();
            self.outstanding_rpc = Some(OutstandingRPC::AppendEntries { index });
            true
        } else {
            false
        }
    }

    fn send_install_snapshot<S: StateMachine, P: Storage<S>, N: NetworkInterface<S>>
    (&mut self, node_id: u32, network: &mut N, storage: &RaftStorage<S, P>, config: &Config, offset: u32) {
        let total_bytes = storage.inner().total_snapshot_bytes();

        let amt = config.max_bytes_in_install_snapshot.min(total_bytes - offset);

        self.outstanding_rpc = Some(OutstandingRPC::InstallSnapshot { offset, amt });
        self.next_beat = config.new_rpc_response_timeout();

        network.send_raft_message(node_id, Some(config.id), Message::InstallSnapshot {
            term: storage.inner().current_term(),
            last_included_index: storage.inner().snapshot_last_index(),
            last_included_term: storage.inner().snapshot_last_term(),
            offset,
            data: storage.inner().snapshot_chunk(offset, amt).unwrap(),
            done: offset + amt >= total_bytes,
        })
    }
}

fn config<'a, S: StateMachine, P: Storage<S>>(storage: &'a RaftStorage<S, P>, state_machine: &'a RaftStateMachine<S>) -> &'a Config {
    storage.get_last_config_in_log().unwrap_or(&state_machine.config)
}

fn try_send_append_entries<S: StateMachine, N: NetworkInterface<S>, P: Storage<S>>
(to_node_id: u32, our_id: u32, network: &mut N, log: LogRef<S, P>, term: u32, commit_index: u32, index: u32) -> bool {
    if let Some(entries) = log.entries(index) {
        network.send_raft_message(to_node_id, Some(our_id), OutgoingAppendEntries {
            term,
            prev_log_index: index - 1,
            prev_log_term: log.term(index - 1).unwrap_or(0),
            leader_commit: commit_index,
            entries,
        });
        true
    } else {
        false
    }
}

fn send_request_vote<S: StateMachine, N: NetworkInterface<S>, P: Storage<S>>(network: &mut N, log: LogRef<S, P>, term: u32, to_node_id: u32) {
    network.send_raft_message(to_node_id, None, Message::RequestVote { term, last_log_index: log.last_index(), last_log_term: log.last_term() })
}

fn has_majority_votes(others: &HashMap<u32, CandidateView>) -> bool {
    let votes = others.iter().filter(|(_, v)| match v {
        CandidateView::VoteGranted => true,
        _ => false
    }).count() + 1; // include our own vote for ourself
    votes > ((others.len() + 1) / 2) // add 1 to include ourselves
}

fn try_append_new_client_command_to_log<S: StateMachine, N: NetworkInterface<S>, P: Storage<S>>
(network: &mut N, mut log: LogMut<S, P>, commit_index: u32, state_machine: &RaftStateMachine<S>, req: ClientRequest<S::Command>) -> bool {
    // ignore request if we already have it but haven't committed it yet
    if log.immut().has_uncommitted_command_from_client(commit_index, req.client_id, req.request_id) {
        return false;
    }

    // reply immediately if request has already been committed
    if state_machine.is_clients_last_command(req.client_id, req.request_id) {
        network.handle_command_applied(ClientRequest { client_id: req.client_id, request_id: req.request_id, data: &req.data }, &state_machine.inner);
        return false;
    }

    log.add_entry(LogEntryType::Command { command: req.data, client_id: req.client_id, command_id: req.request_id });
    true
}

fn update_commit_index<'a, S: StateMachine, P: Storage<S>>(mut new_commit: u32, current_commit: &mut u32, log: LogRef<'a, S, P>, state_machine: &mut RaftStateMachine<S>) -> &'a [LogEntry<S::Command>] {
    if new_commit > *current_commit {
        new_commit = log.last_index().min(new_commit);

        // commit idx should always be greater than the first log index
        let committed_entries = &log.entries(*current_commit + 1).unwrap()[..((new_commit - *current_commit) as usize)];
        *current_commit = new_commit;

        for entry in committed_entries {
            state_machine.apply_command(entry);
        }

        committed_entries
    } else {
        &[]
    }
}

fn max_commit<S: StateMachine, P: Storage<S>>(views: &HashMap<u32, LeaderView>, log: LogRef<S, P>, current_term: u32, commit_index: u32) -> u32 {
    views.iter()
        .map(|(_, v)| v.match_index)
        .filter(|match_index| *match_index > commit_index)
        .filter(|match_index| log.term(*match_index).map(|term| term == current_term).unwrap_or(false))
        .filter(|match_index| majority_matches(views, *match_index))
        .max()
        .unwrap_or(commit_index)
}

fn majority_matches(views: &HashMap<u32, LeaderView>, match_index: u32) -> bool {
    let match_count = views.values().filter(|v| v.match_index >= match_index).count() + 1;
    match_count > ((views.len() + 1) / 2)
}

fn try_make_snapshot<S: StateMachine, P: Storage<S>>(storage: &mut RaftStorage<S, P>, state_machine: &RaftStateMachine<S>, commit_index: u32) {
    let config = config(storage, state_machine);
    if commit_index - storage.inner().snapshot_last_index() >= config.snapshot_min_log_size {
        storage.set_snapshot(commit_index, storage.log().term(commit_index).unwrap(), state_machine);
        storage.log_mut().remove_entries_before(commit_index);
    }
}