use crate::log::LogEntry;

pub enum Message<'a> {
    AppendEntries {
        term: u32,
        leader_id: u32,
        prev_log_index: u32,
        prev_log_term: u32,
        leader_commit: u32,
        entries: &'a [u8],
    },
    AppendEntriesResponse {
        term: u32,
        success: bool,
        match_index: u32,
    },
    RequestVote {
        term: u32,
        last_log_index: u32,
        last_log_term: u32,
    },
    RequestVoteResponse {
        term: u32,
        vote_granted: bool,
    },
    InstallSnapshot {
        term: u32,
        leader_id: u32,
        last_included_index: u32,
        last_included_term: u32,
        offset: u32,
        data: &'a [u8],
        done: bool,
    },
    InstallSnapshotResponse {
        term: u32,
        success: bool,
    },
}

pub struct OutgoingAppendEntries<'a, C> {
    pub term: u32,
    pub leader_id: u32,
    pub prev_log_index: u32,
    pub prev_log_term: u32,
    pub leader_commit: u32,
    pub entries: &'a [LogEntry<C>],
}