pub mod node {
    tonic::include_proto!("node");
}

use std::collections::HashMap;
use tonic::{Request, Response, Status};

use node::{acceptor_server, replica_server};

use super::{
    acceptor, common::*, patch::*, patch_decree::*, paxos_common, paxos_common::Balnum, proposer,
    replica::*,
};
use acceptor::{AcceptorFailure, ProposeResponse};

type Decree = PatchDecree;
type AcceptorInterface = acceptor::AcceptorInterface<Decree>;
type PromiseResponse = acceptor::PromiseResponse<Decree>;
type Proposal = paxos_common::Proposal<Decree>;

pub struct AcceptorService {
    iface: AcceptorInterface,
}

impl AcceptorService {
    fn new(iface: AcceptorInterface) -> Self {
        Self { iface }
    }
}

#[tonic::async_trait]
impl acceptor_server::Acceptor for AcceptorService {
    async fn promise(
        &self,
        req: Request<node::Balnum>,
    ) -> Result<Response<node::PromiseResponse>, Status> {
        let mut iface = self.iface.clone();
        let num = req.into_inner();
        let res = iface
            .promise(from_balnum(num))
            .await
            .map_err(from_acc_err)?;

        Ok(Response::new(to_promise_response(res)))
    }

    async fn propose(
        &self,
        req: Request<node::Proposal>,
    ) -> Result<Response<node::ProposeResponse>, Status> {
        let mut iface = self.iface.clone();
        let prop = match from_proposal(req.into_inner()) {
            Some(p) => p,
            None => {
                return Err(Status::invalid_argument("missing proposal data"));
            }
        };
        let res = iface.propose(prop).await.map_err(from_acc_err)?;

        Ok(Response::new(to_propose_response(res)))
    }

    async fn get_balnum(&self, _: Request<node::Empty>) -> Result<Response<node::Balnum>, Status> {
        let mut iface = self.iface.clone();
        let res = iface.get_balnum().await.map_err(from_acc_err)?;

        Ok(Response::new(to_balnum(res)))
    }
}

pub fn new_acceptor_server(
    iface: AcceptorInterface,
) -> acceptor_server::AcceptorServer<AcceptorService> {
    acceptor_server::AcceptorServer::new(AcceptorService::new(iface))
}

pub struct ReplicaService {
    iface: ReplicaInterface,
}

impl ReplicaService {
    fn new(iface: ReplicaInterface) -> Self {
        Self { iface }
    }
}

#[tonic::async_trait]
impl replica_server::Replica for ReplicaService {
    async fn commit_and_read(
        &self,
        req: Request<node::CommitAndReadRequest>,
    ) -> Result<Response<node::CommitAndReadResponse>, Status> {
        let mut iface = self.iface.clone();
        let req = match from_commit_and_read_request(req.into_inner()) {
            Some(r) => r,
            None => {
                return Err(Status::invalid_argument(
                    "missing CommitAndReadRequest data",
                ));
            }
        };
        let res = iface.commit_and_read(req).await.map_err(from_repl_err)?;

        Ok(Response::new(to_commit_and_read_response(res)))
    }
}

pub fn new_replica_server(
    iface: ReplicaInterface,
) -> replica_server::ReplicaServer<ReplicaService> {
    replica_server::ReplicaServer::new(ReplicaService::new(iface))
}

fn from_acc_err(e: AcceptorFailure) -> Status {
    match e {
        AcceptorFailure::ChannelDropped => Status::unavailable("server shutting down"),
    }
}

fn from_repl_err(e: ReplicaFailure) -> Status {
    match e {
        ReplicaFailure::ChannelDropped => Status::unavailable("server shutting down"),
    }
}

fn to_promise_response(r: PromiseResponse) -> node::PromiseResponse {
    use node::promise_response::Result::*;
    node::PromiseResponse {
        result: Some(match r {
            PromiseResponse::Promised(p) => Promised(to_promised(p)),
            PromiseResponse::Rejected(num) => Rejected(to_balnum(num)),
        }),
    }
}

fn to_propose_response(r: ProposeResponse) -> node::ProposeResponse {
    use node::propose_response::Result::*;
    node::ProposeResponse {
        result: Some(match r {
            ProposeResponse::Accepted => Accepted(node::Empty {}),
            ProposeResponse::Preempted(num, slot) => Preempted(to_preempted(num, slot)),
        }),
    }
}

fn to_commit_and_read_response(r: CommitAndReadResponse) -> node::CommitAndReadResponse {
    use node::commit_and_read_response::Result::*;
    node::CommitAndReadResponse {
        result: Some(match r {
            CommitAndReadResponse::Preempted => Preempted(node::Empty {}),
            CommitAndReadResponse::AlreadyHandled {
                read_patch,
                preempted,
            } => AlreadyHandled(to_handled(read_patch, preempted)),
            CommitAndReadResponse::Versioned(_, p) => Versioned(to_versioned_patch(p)),
        }),
    }
}

pub fn to_commit_and_read_request(r: CommitAndReadRequest) -> node::CommitAndReadRequest {
    node::CommitAndReadRequest {
        commit: Some(to_maybe_commit(r.commit)),
        read_set: Some(to_set_of_keys(r.read_set)),
        read_cr_id: r.read_cr_id.0,
        read_slot: r.read_slot,
    }
}

fn to_maybe_commit(c: Option<Commit>) -> node::MaybeCommit {
    use node::maybe_commit::C;
    node::MaybeCommit {
        c: Some(match c {
            Some(c) => C::Commit(to_commit(c)),
            None => C::NoCommit(node::Empty {}),
        }),
    }
}

fn to_set_of_keys(s: Vec<Key>) -> node::SetOfKeys {
    node::SetOfKeys { keys: s }
}

fn to_commit(c: Commit) -> node::Commit {
    node::Commit {
        dec: Some(to_decree(c.dec)),
        slot: to_slot(c.slot),
    }
}

fn to_promised(p: Option<Proposal>) -> node::Promised {
    use node::promised::Promise::*;
    if let Some(p) = p {
        return node::Promised {
            promise: Some(Proposal(to_proposal(p))),
        };
    }

    node::Promised {
        promise: Some(NoProposal(node::Empty {})),
    }
}

fn to_preempted(num: Balnum, slot: Slot) -> node::Preempted {
    node::Preempted {
        num: Some(to_balnum(num)),
        slot: to_slot(slot),
    }
}

pub fn to_proposal(p: Proposal) -> node::Proposal {
    node::Proposal {
        dec: Some(to_decree(p.dec)),
        num: Some(to_balnum(p.num)),
        slot: to_slot(p.slot),
    }
}

fn to_decree(d: Decree) -> node::Decree {
    node::Decree {
        cr_id: d.cr_id.0,
        read_patch: to_patch(d.read_patch),
        write_patch: to_patch(d.write_patch),
    }
}

fn to_patch(p: Patch) -> HashMap<Key, Value> {
    p
}

pub fn to_balnum(num: Balnum) -> node::Balnum {
    node::Balnum {
        fst: num.fst,
        node_id: num.id.0,
    }
}

fn to_slot(s: Slot) -> u64 {
    s
}

fn to_handled(read_patch: Patch, preempted: bool) -> node::Handled {
    node::Handled {
        read_patch: to_patch(read_patch),
        preempted,
    }
}

fn to_versioned_patch(p: VersionedPatch) -> node::VersionedPatch {
    node::VersionedPatch {
        patch: p
            .into_iter()
            .map(|(k, (v, s))| (k, node::ValueAndSlot { value: v, slot: s }))
            .collect(),
    }
}

pub fn from_promise_response(
    id: NodeId,
    r: node::PromiseResponse,
) -> Option<proposer::PromiseResponse> {
    use node::promise_response::Result as R;
    use proposer::PromiseResponse as PR;
    Some(match r.result? {
        R::Promised(p) => PR::Promised(id, from_promised(p)?),
        R::Rejected(num) => PR::Rejected(from_balnum(num)),
    })
}

pub fn from_propose_response(
    id: NodeId,
    r: node::ProposeResponse,
) -> Option<proposer::ProposeResponse> {
    use node::propose_response::Result as R;
    use proposer::ProposeResponse as PR;
    Some(match r.result? {
        R::Accepted(node::Empty {}) => PR::Accepted(id),
        R::Preempted(pre) => {
            let (num, slot) = from_preempted(pre)?;
            PR::Preempted(num, slot)
        }
    })
}

fn from_preempted(pre: node::Preempted) -> Option<(Balnum, Slot)> {
    Some((from_balnum(pre.num?), from_slot(pre.slot)))
}

fn from_promised(p: node::Promised) -> Option<Option<Proposal>> {
    use node::promised::Promise as P;
    Some(match p.promise? {
        P::NoProposal(node::Empty {}) => None,
        P::Proposal(p) => Some(from_proposal(p)?),
    })
}

fn from_commit_and_read_request(r: node::CommitAndReadRequest) -> Option<CommitAndReadRequest> {
    Some(CommitAndReadRequest {
        commit: from_maybe_commit(r.commit?)?,
        read_set: from_set_of_keys(r.read_set?),
        read_cr_id: ClientRequestId(r.read_cr_id),
        read_slot: r.read_slot,
    })
}

pub fn from_commit_and_read_response(
    id: NodeId,
    r: node::CommitAndReadResponse,
) -> Option<CommitAndReadResponse> {
    use node::commit_and_read_response::Result as R;
    use CommitAndReadResponse as CAR;
    Some(match r.result? {
        R::Preempted(node::Empty {}) => CAR::Preempted,
        R::AlreadyHandled(h) => {
            let (read_patch, preempted) = from_handled(h);
            CAR::AlreadyHandled {
                read_patch,
                preempted,
            }
        }
        R::Versioned(p) => CAR::Versioned(id, from_versioned_patch(p)),
    })
}

fn from_handled(h: node::Handled) -> (Patch, bool) {
    (h.read_patch, h.preempted)
}

fn from_versioned_patch(p: node::VersionedPatch) -> VersionedPatch {
    p.patch
        .into_iter()
        .map(|(k, vs)| (k, (vs.value, vs.slot)))
        .collect()
}

fn from_maybe_commit(c: node::MaybeCommit) -> Option<Option<Commit>> {
    use node::maybe_commit::C;
    Some(match c.c? {
        C::Commit(c) => Some(from_commit(c)?),
        C::NoCommit(_) => None,
    })
}

fn from_commit(c: node::Commit) -> Option<Commit> {
    Some(Commit {
        dec: from_decree(c.dec?),
        slot: c.slot,
    })
}

fn from_proposal(prop: node::Proposal) -> Option<Proposal> {
    Some(Proposal {
        dec: from_decree(prop.dec?),
        num: from_balnum(prop.num?),
        slot: from_slot(prop.slot),
    })
}

fn from_decree(dec: node::Decree) -> Decree {
    Decree {
        cr_id: ClientRequestId(dec.cr_id),
        read_patch: from_patch(dec.read_patch),
        write_patch: from_patch(dec.write_patch),
    }
}

fn from_set_of_keys(s: node::SetOfKeys) -> Vec<Key> {
    s.keys
}

fn from_slot(s: u64) -> Slot {
    s
}

pub fn from_balnum(num: node::Balnum) -> Balnum {
    Balnum {
        fst: num.fst,
        id: NodeId(num.node_id),
    }
}

fn from_patch(p: HashMap<Key, Value>) -> Patch {
    p
}
