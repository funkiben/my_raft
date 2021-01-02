use std::collections::HashMap;
use std::option::Option::Some;
use std::time::Duration;

use crate::config::Config;
use crate::log::{Log, LogEntry, LogEntryType};
use crate::message::{Message, OutgoingAppendEntries};
use crate::network::{ClientRequest, MessageEvent, NetworkInterface};
use crate::persisted_log::PersistedLog;
use crate::persistent_storage::{PersistentStorage, Snapshot};
use crate::serialize::{LogEntriesBytes, TryFromExactBytes};
use crate::state_machine::{RaftStateMachine, StateMachine};
use crate::timeout::Timeout;

// TODO put more log stuff in storage, storage should just hold the base array of log entries

pub struct Raft<S: StateMachine, P: PersistentStorage<S>, N: NetworkInterface<S>> {
    storage: P,
    network: N,
    commit_index: u32,
    state_machine: RaftStateMachine<S>,
    log: PersistedLog<S, P>,
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

impl<S: StateMachine, P: PersistentStorage<S>, N: NetworkInterface<S>> Raft<S, P, N> {
    pub fn new(mut storage: P, mut network: N, init_state_machine: S) -> Raft<S, P, N> {
        let (log, state_machine) = Self::load_log_and_state_machine(&mut storage, init_state_machine);

        let config = log.get_ref().config().unwrap_or(state_machine.config());

        network.on_config_update(config);

        let election_timeout = config.new_election_timeout();
        let commit_index = log.get_ref().index_before_first_log_entry();

        Raft {
            storage,
            network,
            commit_index,
            state_machine,
            log,
            role: Role::Follower { election_timeout },
            read_request_queue: vec![],
            command_request_queue: vec![],
        }
    }

    fn load_log_and_state_machine(storage: &mut P, init_inner_state_machine: S) -> (PersistedLog<S, P>, RaftStateMachine<S>) {
        let snapshot = storage.snapshot();
        let log_entries = storage.log_entries();

        if let Some(Snapshot { last_index, last_term, state_machine }) = snapshot {
            let log = Log::new(last_index, last_term, log_entries);
            let log = PersistedLog::new(log);
            (log, state_machine)
        } else {
            let config = storage.init_config();
            let state_machine = RaftStateMachine::new(config, init_inner_state_machine);
            let log = Log::new(0, 0, log_entries);
            let log = PersistedLog::new(log);
            (log, state_machine)
        }
    }

    pub fn start(&mut self) {
        let mut msg_buf = vec![0u8; self.config().max_message_bytes as usize];

        loop {
            let event = self.next_event(&mut msg_buf);
            self.handle_event(event);

            msg_buf.resize_with(self.config().max_message_bytes as usize, || 0u8);
        }
    }


    fn next_event<'a>(&mut self, msg_buf: &'a mut [u8]) -> Event<'a, S::Command, N::ReadRequest> {
        let (timeout, timeout_type) = self.next_timeout();

        let time_left = timeout.time_left();

        if time_left == Duration::from_secs(0) {
            return Event::Timeout(timeout_type);
        }

        match self.network.wait_for_message(time_left, msg_buf) {
            MessageEvent::Node { size, src_node_id } =>
                Message::try_from_bytes(&msg_buf[..size]).map(|m| Event::NodeMessage(src_node_id, m)).unwrap_or_else(move || self.next_event(msg_buf)),
            MessageEvent::ClientCommand(req) => Event::ClientCommand(req),
            MessageEvent::ClientRead(req) => Event::ClientRead(req),
            MessageEvent::Timeout => Event::Timeout(timeout_type)
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
            Message::AppendEntries { term, leader_id, prev_log_index, prev_log_term, leader_commit, entries } =>
                self.handle_append_entries(src_node_id, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries),
            Message::AppendEntriesResponse { term, success, match_index } =>
                self.handle_append_entries_response(src_node_id, term, success, match_index),
            Message::RequestVote { term, last_log_index, last_log_term } =>
                self.handle_request_vote(src_node_id, term, last_log_index, last_log_term),
            Message::RequestVoteResponse { term, vote_granted } =>
                self.handle_request_vote_response(src_node_id, term, vote_granted),
            Message::InstallSnapshot { term, leader_id, last_included_index, last_included_term, offset, data, done } =>
                self.handle_install_snapshot(src_node_id, term, leader_id, last_included_index, last_included_term, offset, data, done),
            Message::InstallSnapshotResponse { term, success } =>
                self.handle_install_snapshot_response(src_node_id, term, success)
        }
    }

    // TODO handle config entry
    fn handle_append_entries(&mut self, src_node_id: u32, term: u32, leader_id: u32, prev_log_index: u32, prev_log_term: u32, leader_commit: u32, entries: &[u8]) {
        if self.check_term_and_update_self(term) {
            self.receive_message_from_leader(leader_id);

            if let Some(match_index) = self.log.try_append_entries(&mut self.storage, prev_log_index, prev_log_term, LogEntriesBytes::new(entries)) {
                self.network.send_raft_message(src_node_id, Message::AppendEntriesResponse { term: self.storage.current_term(), success: true, match_index });

                update_commit_index(leader_commit, &mut self.commit_index, &self.log.get_ref(), &mut self.state_machine);

                return;
            }
        }

        self.network.send_raft_message(src_node_id, Message::AppendEntriesResponse { term: self.storage.current_term(), success: false, match_index: 0 })
    }

    fn handle_append_entries_response(&mut self, src_node_id: u32, term: u32, success: bool, match_index: u32) {
        if !self.check_term_and_update_self(term) {
            return;
        }

        if let Role::Leader { views, noop_index } = &mut self.role {
            let view = views.get_mut(&src_node_id).unwrap();
            let config = config(&self.log, &self.state_machine);

            let mut next_index = match view.outstanding_rpc {
                Some(OutstandingRPC::AppendEntries { index }) => index,
                _ => 0
            };

            if success {
                view.match_index = match_index;
                next_index = match_index + 1;
            } else {
                next_index -= config.next_index_decrease_rate;
            }

            if next_index <= self.log.get_ref().last_log_index() {
                if !view.try_send_append_entries(src_node_id, &mut self.network, self.log.get_ref(), config, self.storage.current_term(), self.commit_index, next_index) {
                    view.send_install_snapshot(src_node_id, &mut self.network, &mut self.storage, config, self.log.get_ref(), 0);
                }
            } else {
                view.outstanding_rpc = None;
                view.next_beat = config.new_heartbeat_timeout();
            }

            // TODO this is kinda poopy
            if success {
                let new_commit_index = max_commit(views, self.log.get_ref(), self.storage.current_term(), self.commit_index);
                for entry in update_commit_index(new_commit_index, &mut self.commit_index, self.log.get_ref(), &mut self.state_machine) {
                    if let LogEntryType::Command { client_id, command_id, .. } = entry.entry_type {
                        self.network.handle_command_executed(client_id, command_id, &self.state_machine.inner())
                    }
                }

                if self.commit_index >= *noop_index {
                    for req in self.read_request_queue.drain(..) {
                        self.network.handle_read(req.client_id, req.request_id, req.data, &self.state_machine.inner());
                    }
                }
            }
        }
    }

    fn handle_request_vote(&mut self, src_node_id: u32, term: u32, last_log_index: u32, last_log_term: u32) {
        let mut grant_vote = false;

        if self.check_term_and_update_self(term) {
            let new_election_timeout = self.config().new_election_timeout();

            if let Role::Follower { election_timeout } = &mut self.role {
                if last_log_term >= self.log.get_ref().last_log_term() && last_log_index >= self.log.get_ref().last_log_index() {
                    grant_vote = match self.storage.voted_for() {
                        Some(id) if id == src_node_id => true,
                        None => true,
                        _ => false
                    };

                    if grant_vote {
                        self.storage.save_voted_for(Some(src_node_id));
                        *election_timeout = new_election_timeout;
                    }
                }
            }
        }

        self.network.send_raft_message(src_node_id, Message::RequestVoteResponse { term: self.storage.current_term(), vote_granted: grant_vote });
    }

    fn handle_request_vote_response(&mut self, src_node_id: u32, term: u32, vote_granted: bool) {
        if !self.check_term_and_update_self(term) {
            return;
        }

        let become_leader = if let Role::Candidate { views, .. } = &mut self.role {
            views.insert(src_node_id, if vote_granted { CandidateView::VoteGranted } else { CandidateView::VoteDenied });
            has_majority_votes(views)
        } else {
            false
        };

        if become_leader {
            let next_index = self.log.get_ref().last_log_index() + 1;
            self.role = Role::Leader {
                noop_index: next_index,
                views: self.config().other_node_ids().map(|id| (id, LeaderView {
                    match_index: 0,
                    next_beat: self.config().new_rpc_response_timeout(),
                    outstanding_rpc: Some(OutstandingRPC::AppendEntries { index: next_index }),
                })).collect(),
            };

            self.log.push_entry(&mut self.storage, LogEntryType::Noop);
            for req in self.command_request_queue.drain(..) {
                try_append_new_client_command_to_log(&mut self.network, &mut self.storage, &mut self.log, self.commit_index, &self.state_machine, req);
            }

            let config = config(&self.log, &self.state_machine);
            for other_id in config.other_node_ids() {
                try_send_append_entries(&mut self.network, &config, self.log.get_ref(), self.storage.current_term(), self.commit_index, other_id, next_index);
            }
        }
    }

    fn handle_install_snapshot(&mut self, src_node_id: u32, term: u32, leader_id: u32, last_included_index: u32, last_included_term: u32, offset: u32, data: &[u8], done: bool) {
        let success = if self.check_term_and_update_self(term) {
            self.receive_message_from_leader(leader_id);

            self.storage.save_snapshot_chunk(last_included_index, last_included_term, offset, data, done);

            if done {
                let snapshot = self.storage.snapshot().unwrap();
                self.log.remove_entries_before(&mut self.storage, last_included_index + 1);
                self.state_machine = snapshot.state_machine;
                self.commit_index = snapshot.last_index;
                self.network.on_config_update(config(&self.log, &self.state_machine));
            }

            true
        } else {
            false
        };

        self.network.send_raft_message(src_node_id, Message::InstallSnapshotResponse { success, term: self.storage.current_term() });
    }

    fn handle_install_snapshot_response(&mut self, src_node_id: u32, term: u32, success: bool) {
        if self.check_term_and_update_self(term) {
            if let Role::Leader { views, .. } = &mut self.role {
                let view = views.get_mut(&src_node_id).unwrap();
                let config = config(&self.log, &self.state_machine);

                if success {
                    let offset = match view.outstanding_rpc {
                        Some(OutstandingRPC::InstallSnapshot { offset, amt }) => offset + amt,
                        _ => 0
                    };

                    if offset >= self.storage.total_snapshot_bytes() {
                        view.try_send_append_entries(src_node_id, &mut self.network, self.log.get_ref(), config, self.storage.current_term(), self.commit_index, self.log.get_ref().index_before_first_log_entry() + 1);
                    } else {
                        view.send_install_snapshot(src_node_id, &mut self.network, &mut self.storage, config, self.log.get_ref(), offset);
                    }
                } else {
                    view.outstanding_rpc = None;
                    view.next_beat = config.new_heartbeat_timeout();
                }
            }
        }
    }

    fn handle_client_command_request(&mut self, req: ClientRequest<S::Command>) {
        if let Role::Leader { views, .. } = &mut self.role {
            if try_append_new_client_command_to_log(&mut self.network, &mut self.storage, &mut self.log, self.commit_index, &self.state_machine, req) {
                let config = config(&self.log, &self.state_machine);

                for (other_id, view) in views {
                    if view.outstanding_rpc.is_none() {
                        view.try_send_append_entries(*other_id, &mut self.network, self.log.get_ref(), &config, self.storage.current_term(), self.commit_index, self.log.get_ref().last_log_index());
                    }
                }
            }
        } else if let Some(leader_id) = self.storage.voted_for() {
            self.network.redirect(leader_id, req.client_id, req.request_id);
        } else {
            self.command_request_queue.push(req);
        }
    }

    fn handle_client_read_request(&mut self, req: ClientRequest<N::ReadRequest>) {
        if let Role::Leader { noop_index, .. } = self.role {
            if self.commit_index >= noop_index {
                self.network.handle_read(req.client_id, req.request_id, req.data, &self.state_machine.inner());
            } else {
                self.read_request_queue.push(req);
            }
        } else if let Some(leader_id) = self.storage.voted_for() {
            self.network.redirect(leader_id, req.client_id, req.request_id);
        } else {
            self.read_request_queue.push(req);
        }
    }

    fn handle_election_timeout(&mut self) {
        self.storage.save_current_term(self.storage.current_term() + 1);
        self.role = Role::Candidate {
            election_timeout: self.config().new_election_timeout(),
            views: self.config().other_node_ids().map(|n| (n, CandidateView::Waiting(self.config().new_rpc_response_timeout()))).collect(),
        };
        for node_id in config(&self.log, &self.state_machine).other_node_ids() {
            send_request_vote(&mut self.network, self.log.get_ref(), self.storage.current_term(), node_id);
        }
    }

    fn handle_request_vote_timeout(&mut self, node_id: u32) {
        let timeout = self.config().new_rpc_response_timeout();
        if let Role::Candidate { views, .. } = &mut self.role {
            views.entry(node_id).and_modify(|view| *view = CandidateView::Waiting(timeout));
            send_request_vote(&mut self.network, self.log.get_ref(), self.storage.current_term(), node_id);
        }
    }

    fn handle_heartbeat_timeout(&mut self, node_id: u32) {
        if let Role::Leader { views, .. } = &mut self.role {
            let view = views.get_mut(&node_id).unwrap();
            let config = config(&self.log, &self.state_machine);

            if let Some(OutstandingRPC::InstallSnapshot { offset, .. }) = view.outstanding_rpc {
                view.send_install_snapshot(node_id, &mut self.network, &mut self.storage, config, self.log.get_ref(), offset);
            } else {
                let index = match view.outstanding_rpc {
                    Some(OutstandingRPC::AppendEntries { index }) => index,
                    _ => self.log.get_ref().last_log_index() + 1
                };
                view.try_send_append_entries(node_id, &mut self.network, self.log.get_ref(), config, self.storage.current_term(), self.commit_index, index);
            }
        }
    }

    fn config(&self) -> &Config {
        config(&self.log, &self.state_machine)
    }

    fn check_term_and_update_self(&mut self, new_term: u32) -> bool {
        if new_term < self.storage.current_term() {
            return false;
        } else if new_term > self.storage.current_term() {
            self.storage.save_current_term(new_term);
            self.storage.save_voted_for(None);

            self.role = Role::Follower { election_timeout: self.config().new_election_timeout() };
        }
        true
    }

    fn receive_message_from_leader(&mut self, leader_id: u32) {
        self.storage.save_voted_for(Some(leader_id));
        self.role = Role::Follower { election_timeout: self.config().new_election_timeout() };

        for req in self.read_request_queue.drain(..) {
            self.network.redirect(leader_id, req.client_id, req.request_id);
        }

        for req in self.command_request_queue.drain(..) {
            self.network.redirect(leader_id, req.client_id, req.request_id);
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
    fn try_send_append_entries<S: StateMachine, N: NetworkInterface<S>>(&mut self, node_id: u32, network: &mut N, log: &Log<S::Command>, config: &Config, term: u32, commit_index: u32, index: u32) -> bool {
        if try_send_append_entries(network, config, log, term, commit_index, node_id, index) {
            self.next_beat = config.new_rpc_response_timeout();
            self.outstanding_rpc = Some(OutstandingRPC::AppendEntries { index });
            true
        } else {
            false
        }
    }

    fn send_install_snapshot<S: StateMachine, P: PersistentStorage<S>, N: NetworkInterface<S>>(&mut self, node_id: u32, network: &mut N, storage: &mut P, config: &Config, log: &Log<S::Command>, offset: u32) {
        let total_bytes = storage.total_snapshot_bytes();

        let amt = (config.max_message_bytes - 64).min(total_bytes - offset); // TODO calculate this properly

        self.outstanding_rpc = Some(OutstandingRPC::InstallSnapshot { offset, amt });
        self.next_beat = config.new_rpc_response_timeout();

        network.send_raft_message(node_id, Message::InstallSnapshot {
            term: storage.current_term(),
            leader_id: 0,
            last_included_index: log.index_before_first_log_entry(),
            last_included_term: log.term_before_first_log_entry(),
            offset,
            data: storage.snapshot_chunk(offset, amt).unwrap(),
            done: offset + amt >= total_bytes,
        })
    }
}

fn config<'a, S: StateMachine, P: PersistentStorage<S>>(log: &'a PersistedLog<S, P>, state_machine: &'a RaftStateMachine<S>) -> &'a Config {
    log.get_ref().config().unwrap_or(state_machine.config())
}

fn try_send_append_entries<S: StateMachine, N: NetworkInterface<S>>
(network: &mut N, config: &Config, log: &Log<S::Command>, term: u32, commit_index: u32, to_node_id: u32, next_index: u32) -> bool {
    if let Some(entries) = log.entries(next_index) {
        network.send_raft_message(to_node_id, OutgoingAppendEntries {
            term,
            leader_id: config.id,
            prev_log_index: next_index - 1,
            prev_log_term: log.term(next_index - 1).unwrap_or(0),
            leader_commit: commit_index,
            entries,
        });
        true
    } else {
        false
    }
}

fn send_request_vote<S: StateMachine, N: NetworkInterface<S>>(network: &mut N, log: &Log<S::Command>, term: u32, to_node_id: u32) {
    network.send_raft_message(to_node_id, Message::RequestVote { term, last_log_index: log.last_log_index(), last_log_term: log.last_log_term() })
}

fn has_majority_votes(others: &HashMap<u32, CandidateView>) -> bool {
    let votes = others.iter().filter(|(_, v)| match v {
        CandidateView::VoteGranted => true,
        _ => false
    }).count() + 1; // include our own vote for ourself
    votes > ((others.len() + 1) / 2) // add 1 to include ourselves
}

fn try_append_new_client_command_to_log<S: StateMachine, N: NetworkInterface<S>, P: PersistentStorage<S>>
(network: &mut N, storage: &mut P, log: &mut PersistedLog<S, P>, commit_index: u32, state_machine: &RaftStateMachine<S>, req: ClientRequest<S::Command>) -> bool {
    // ignore request if we already have it but haven't committed it yet
    if log.get_ref().has_uncommitted_command_from_client(commit_index, req.client_id, req.request_id) {
        return false;
    }

    // reply immediately if request has already been committed
    if state_machine.is_last_client_command(req.client_id, req.request_id) {
        network.handle_command_executed(req.client_id, req.request_id, state_machine.inner());
        return false;
    }

    log.push_entry(storage, LogEntryType::Command { command: req.data, client_id: req.client_id, command_id: req.request_id });
    true
}

fn update_commit_index<'a, S: StateMachine>(new_commit_index: u32, old_commit: &mut u32, log: &'a Log<S::Command>, state_machine: &mut RaftStateMachine<S>) -> &'a [LogEntry<S::Command>] {
    if new_commit_index > *old_commit {
        let new_commits = log.last_log_index().min(new_commit_index) - *old_commit;
        let old_commit_copy = *old_commit;
        *old_commit = new_commit_index;

        // commit idx should always be greater than the first log index
        let committed_entries = &log.entries(old_commit_copy).unwrap()[..(new_commits as usize)];

        for entry in committed_entries {
            state_machine.apply_command(entry);
        }

        committed_entries
    } else {
        &[]
    }
}

fn max_commit<C>(views: &HashMap<u32, LeaderView>, log: &Log<C>, current_term: u32, commit_index: u32) -> u32 {
    views.iter()
        .map(|(_, v)| v.match_index)
        .filter(|match_index| *match_index > commit_index)
        .filter(|match_index| log.term(*match_index).map(|term| term == current_term).unwrap_or(false))
        .filter(|match_index| majority_matches(views, *match_index))
        .max()
        .unwrap_or(commit_index)
}

fn majority_matches(views: &HashMap<u32, LeaderView>, match_index: u32) -> bool {
    let match_count = views.iter().filter(|(_, v)| v.match_index >= match_index).count() + 1;
    match_count > ((views.len() + 1) / 2)
}