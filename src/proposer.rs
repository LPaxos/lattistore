use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Notify;

use tokio::select;
use tokio::time;

use futures::future;
use futures::future::FutureExt;

// use rand;
// use rand::distributions::{Distribution, Uniform};

use std::cmp;
use std::collections::HashMap;
use std::collections::HashSet;

use log::info;

use super::common::{Failable::*, *};
use super::patch::*;
use super::patch_decree::*;
use super::paxos_common::*;

type Proposal = super::paxos_common::Proposal<PatchDecree>;

fn make_commit(p: Proposal) -> Commit {
    Commit {
        dec: p.dec,
        slot: p.slot,
    }
}

pub enum ProposerRequest {
    Promise(Balnum, mpsc::Sender<Failable<PromiseResponse>>),
    Propose(Proposal, mpsc::Sender<Failable<ProposeResponse>>),
    GetBalnum(mpsc::Sender<Failable<Balnum>>),
}

#[derive(Debug)]
pub enum PromiseResponse {
    Promised(NodeId, Option<Proposal>),
    Rejected(Balnum),
}

#[derive(Debug)]
pub enum ProposeResponse {
    Accepted(NodeId),
    Preempted(Balnum, Slot),
}

enum ProposeFailure {
    Preempted(Balnum),
    NodesUnreachable, // todo: which nodes
    ChannelDropped,
}

pub struct GetBalnumRequest(pub oneshot::Sender<Balnum>);

pub type ProposerReplicaRequest = (
    CommitAndReadRequest,
    mpsc::Sender<Failable<CommitAndReadResponse>>,
);

struct ClientRequest {
    id: ClientRequestId,
    read_set: Vec<Key>,
    fun: Box<dyn Fn(&Patch) -> Patch + Send>,
    resp_tx: oneshot::Sender<ClientResponse>,
    timeout: time::Duration,
}

pub enum ClientResponse {
    Success(Patch),
    Redirect(Option<NodeId>),

    NodesUnreachable,
    Timeout,

    // LostLeadership(_, b): could have handled this request => `b` = true
    // (but even if `b` is false, if it's a retry, the previous attempt might have succeeded
    // deducing that the request was not handled works only if there is no retrying
    LostLeadership(Option<NodeId>, bool),
}

#[derive(Clone)]
pub struct AcceptorConfig {
    size: usize,
}

impl AcceptorConfig {
    pub fn new(size: usize) -> AcceptorConfig {
        AcceptorConfig { size }
    }

    fn size(&self) -> usize {
        self.size
    }
}

pub struct ReplicaConfig {
    size: usize,
}

impl ReplicaConfig {
    pub fn new(size: usize) -> ReplicaConfig {
        ReplicaConfig { size }
    }

    fn size(&self) -> usize {
        self.size
    }
}

pub struct Proposer {
    id: NodeId,
    name: String,

    notify: Arc<Notify>,
    client_request_rx: mpsc::Receiver<ClientRequest>,

    acceptor_bcast_tx: mpsc::Sender<Timed<ProposerRequest>>,
    local_acceptor_tx: mpsc::Sender<GetBalnumRequest>,

    replica_bcast_tx: mpsc::Sender<Timed<ProposerReplicaRequest>>,

    ufd_query_tx: mpsc::Sender<oneshot::Sender<Option<NodeId>>>,

    acceptor_config: AcceptorConfig,
    replica_config: ReplicaConfig,
}

#[derive(Clone)]
pub struct ProposerClientInterface {
    notifier: Arc<Notify>,
    client_request_tx: mpsc::Sender<ClientRequest>,
}

pub struct ProposerAcceptorInterface {
    pub acceptor_bcast_rx: mpsc::Receiver<Timed<ProposerRequest>>,
    pub local_acceptor_rx: mpsc::Receiver<GetBalnumRequest>,
}

pub struct ProposerReplicaInterface {
    pub replica_bcast_rx: mpsc::Receiver<Timed<ProposerReplicaRequest>>,
}

pub struct ProposerUFDInterface {
    pub ufd_query_rx: mpsc::Receiver<oneshot::Sender<Option<NodeId>>>,
}

#[derive(Debug)]
pub enum RequestFailure {
    ChannelDropped,
    TooManyRequests,
}

impl ProposerClientInterface {
    pub async fn request(
        &mut self,
        id: ClientRequestId,
        read_set: Vec<Key>,
        fun: Box<dyn Fn(&Patch) -> Patch + Send>,
        timeout: time::Duration,
    ) -> Result<ClientResponse, RequestFailure> {
        info!("pro-cl: sending request {:?}", id);
        let (resp_tx, resp_rx) = oneshot::channel();

        use mpsc::error::TrySendError::*;
        match self.client_request_tx.try_send(ClientRequest {
            id,
            read_set,
            fun,
            resp_tx,
            timeout,
        }) {
            Err(Full(_)) => {
                info!("pro-cl: channel full, returning TooManyRequests {:?}", id);
                return Err(RequestFailure::TooManyRequests);
            }
            Err(Closed(_)) => {
                info!("pro-cl: channel dropped when handling {:?}", id);
                return Err(RequestFailure::ChannelDropped);
            }
            Ok(()) => (),
        };
        self.notifier.notify();

        info!("pro-cl: request {:?} sent", id);

        match time::timeout(timeout, resp_rx).await {
            Ok(r) => r.map_err(|_| {
                info!("pro-cl: proposer response channel dropped");
                RequestFailure::ChannelDropped
            }),
            Err(time::Elapsed { .. }) => Ok(ClientResponse::Timeout),
        }
    }
}

/*
 * After picking a ballot, getting a quorum of promises from acceptors and performing Repropose if
 * necessary, the proposer enters a state in which it is ready to propose a new decree in the next
 * free slot. We say that the proposer became a leader of the picked ballot. An instance of the
 * `Leader' struct is a proof for being a leader.
 */
struct Leader<'a> {
    //id: NodeId,
    name: &'a str,

    num: Balnum,

    client_request_rx: &'a mut mpsc::Receiver<ClientRequest>,

    acceptor_bcast_tx: &'a mut mpsc::Sender<Timed<ProposerRequest>>,
    acceptor_config: &'a AcceptorConfig,

    replica_bcast_tx: &'a mut mpsc::Sender<Timed<ProposerReplicaRequest>>,
    replica_config: &'a ReplicaConfig,

    // Even if we don't handle client requests during some period,
    // a background task will regularly check in with acceptors in case
    // we've got preempted by a higher balnum (see `start_leadership_checker`).
    // The RemoteHandle is stored here to automatically Drop the background task
    // when we leave the Leader state.
    _leadership_checker: future::RemoteHandle<()>,
    leadership_lost: oneshot::Receiver<Balnum>,
}

/*
 * The result of performing a recovery, i.e. getting a quorum of promises and performing Repropose
 * (if necessary).
 */
struct Leadership<'a> {
    leader: Leader<'a>,

    // Contains the greatest proposal accepted by some quorum of
    // acceptors in a lower ballot; we need to ensure that it's applied by a quorum of replicas before
    // proposing a new decree in the next free slot, as per the specification.
    outstanding_commit: Option<Commit>,

    // The next free slot.
    next_slot: Slot,
}

pub enum ProposerFailure {
    ChannelDropped,
}

enum LeaderFailure {
    LostLeadership(Option<Balnum>),
    NodesUnreachable,
    ChannelDropped,
}

enum ObtainLeadershipFailure {
    Rejected(Balnum),
    NodesUnreachable,
    ChannelDropped,
}

enum CommitAndReadResult {
    Preempted,
    AlreadyHandled { read_patch: Patch, preempted: bool },
    Reconciled(Patch),

    NodesUnreachable,
    ChannelDropped,
}

/*
 * Return some balnum higher than `num` which the proposer identified by `id` owns.
 */
fn next_balnum(num: Balnum, id: NodeId) -> Balnum {
    Balnum {
        fst: num.fst + 1,
        id,
    }
}

/*
 * Create a new proposal and ask acceptors to accept it.
 * Used to perform ProposeNew and Repropose actions from the specification.
 */
async fn propose(
    name: &str,
    acceptor_config: &AcceptorConfig,
    acceptor_bcast_tx: &mut mpsc::Sender<Timed<ProposerRequest>>,
    prop: Proposal,
    timeout: time::Duration,
) -> Result<(), ProposeFailure> {
    let (acceptor_resp_tx, mut acceptor_resp_rx) = mpsc::channel(32); // todo: adjust the size?
    let req = Timed {
        timeout,
        request: ProposerRequest::Propose(prop, acceptor_resp_tx),
    };
    acceptor_bcast_tx
        .send(req)
        .await
        .map_err(|mpsc::error::SendError(_)| ProposeFailure::ChannelDropped)?;

    let majority = acceptor_config.size() / 2 + 1;

    let mut accepts = 0;
    let mut responded_acceptors = HashSet::new();

    // todo: the requests are timed, so the other end of the channel will
    // return a timeout message in case acceptors don't respond in time,
    // but to be safe we could timeout on our own here as well
    while let Some(msg) = acceptor_resp_rx.recv().await {
        match msg {
            Response(ProposeResponse::Accepted(id)) => {
                if !responded_acceptors.insert(id) {
                    continue;
                }

                accepts += 1;
                if accepts >= majority {
                    break;
                }
            }
            Response(ProposeResponse::Preempted(num, _)) => {
                return Err(ProposeFailure::Preempted(num))
            }
            Timeout(id) => {
                info!("{}: node {:?} request timeout (propose)", name, id);
            }
            NetworkError(id, m) => {
                info!("{}: node {:?} network error {} (propose)", name, id, m);
            }
        }
    }

    if accepts < majority {
        return Err(ProposeFailure::NodesUnreachable);
    }

    Ok(())
}

enum RedirectRequestsFailure {
    ChannelDropped,
}

/*
 * If a client request arrives but the failure detector tells us some other proposer should be the
 * leader, we don't perform recovery, but return a redirect message telling the client to contact
 * the other proposer.
 */
fn redirect_requests(
    name: &str,
    client_request_rx: &mut mpsc::Receiver<ClientRequest>,
    to: Option<NodeId>,
) -> Result<(), RedirectRequestsFailure> {
    use mpsc::error::TryRecvError::*;

    let mut cnt = 0;
    for _ in 0..30 {
        // todo: do something smarter?
        let ClientRequest {
            id,
            read_set: _,
            fun: _,
            resp_tx,
            timeout: _,
        } = match client_request_rx.try_recv() {
            Ok(cr) => cr,
            Err(Empty) => break,
            Err(Closed) => return Err(RedirectRequestsFailure::ChannelDropped),
        };

        info!("{}: redirecting request {:?} to {:?}", name, id, to);

        if let Err(_) = resp_tx.send(ClientResponse::Redirect(to)) {
            info!("{}: client request {:?} response channel dropped", name, id);
        } else {
            cnt += 1;
        }
    }

    if cnt > 0 {
        info!("{}: redirected {} requests to {:?}", name, cnt, to);
    }

    Ok(())
}

/*
 * Periodically queries the acceptors in order to check if another proposer didn't start a higher
 * ballot.
 */
fn start_leadership_checker(
    name: String,
    leader_balnum: Balnum,
    mut acceptor_bcast_tx: mpsc::Sender<Timed<ProposerRequest>>,
) -> (future::RemoteHandle<()>, oneshot::Receiver<Balnum>) {
    let (tx, rx) = oneshot::channel();
    let (t, h) = async move {
        loop {
            // todo: should be configurable or adjust dynamically
            let duration = time::Duration::from_secs(5);

            let (resp_tx, mut resp_rx) = mpsc::channel(32); // todo: adjust the size?
            let req = Timed {
                timeout: duration,
                request: ProposerRequest::GetBalnum(resp_tx)
            };
            if let Err(_) = acceptor_bcast_tx.send(req).await {
                info!("{}: leadership checker: acceptor bcast channel dropped", name);
                return;
            }

            //info!("balnum {:?}: checking leadership...", leader_balnum);

            let mut delay = time::delay_for(duration);
            loop {
                select! {
                    msg = resp_rx.recv() => match msg {
                        Some(Response(num)) if num > leader_balnum => {
                            info!("{}: leadership checker: leader of {:?} lost to {:?}", name, leader_balnum, num);
                            if let Err(_) = tx.send(num) {
                                info!("{}: leadership checker: leader channel dropped", name);
                            }

                            return;
                        },
                        Some(Timeout(id)) => {
                            info!("{}: leadership checker: node {:?} request timeout", name, id);
                        }
                        None => {
                            // Channel dropped -- no other responses to be received,
                            // sleep until the next check
                            delay.await;
                            break;
                        }
                        _ => continue,
                    },
                    _ = &mut delay => {
                        break;
                    },
                }
            }
        }
    }.remote_handle();
    tokio::spawn(t);
    (h, rx)
}

impl<'a> Proposer {
    pub fn new(
        id: NodeId,
        name: String,
        acceptor_config: AcceptorConfig,
        replica_config: ReplicaConfig,
    ) -> (
        Proposer,
        ProposerClientInterface,
        ProposerAcceptorInterface,
        ProposerReplicaInterface,
        ProposerUFDInterface,
    ) {
        let notify = Arc::new(Notify::new());
        let notifier = notify.clone();

        let (client_request_tx, client_request_rx) = mpsc::channel(20);
        let (acceptor_bcast_tx, acceptor_bcast_rx) = mpsc::channel(2);
        let (local_acceptor_tx, local_acceptor_rx) = mpsc::channel(1);
        let (replica_bcast_tx, replica_bcast_rx) = mpsc::channel(1);
        let (ufd_query_tx, ufd_query_rx) = mpsc::channel(1);

        (
            Proposer {
                id,
                name,
                notify,
                client_request_rx,
                acceptor_bcast_tx,
                local_acceptor_tx,
                replica_bcast_tx,
                ufd_query_tx,
                acceptor_config,
                replica_config,
            },
            ProposerClientInterface {
                notifier,
                client_request_tx,
            },
            ProposerAcceptorInterface {
                acceptor_bcast_rx,
                local_acceptor_rx,
            },
            ProposerReplicaInterface { replica_bcast_rx },
            ProposerUFDInterface { ufd_query_rx },
        )
    }

    /*
     * Outer proposer loop:
     * 1. query failure detector
     * 2. if it suggests another proposer, redirect any outstanding client requests and continue;
     *    otherwise start recovery (`obtain_leadership`)
     * 3. if recovery successful, enter normal operation (`leader.run`)
     */
    pub async fn run(mut self) -> ProposerFailure {
        let mut highest_known_balnum = MIN_BALNUM;

        let notifier = self.notify.clone();
        let (notifier_task, _h) = async move {
            loop {
                notifier.notify();
                time::delay_for(time::Duration::from_millis(100)).await;
            }
        }
        .remote_handle();

        tokio::spawn(notifier_task);

        loop {
            self.notify.notified().await;

            match self.get_suggested_leader().await {
                Ok(me) if me == Some(self.id) => (),
                Ok(suggested_leader) => {
                    // if there are any client requests, reject them
                    if let Err(RedirectRequestsFailure::ChannelDropped) =
                        redirect_requests(&self.name, &mut self.client_request_rx, suggested_leader)
                    {
                        info!("{}: redirect requests channel dropped", self.name);
                        return ProposerFailure::ChannelDropped;
                    }

                    continue;
                }
                Err(()) => {
                    return ProposerFailure::ChannelDropped;
                }
            }

            let local_acc_num = match self.get_local_acceptor_balnum().await {
                Ok(num) => num,
                Err(_) => {
                    info!("{}: local acc channel dropped", self.name);
                    return ProposerFailure::ChannelDropped;
                }
            };
            highest_known_balnum = cmp::max(highest_known_balnum, local_acc_num);
            let attempted_num = next_balnum(highest_known_balnum, self.id);
            highest_known_balnum = attempted_num;

            // todo: this timeout should be configurable or somehow adjust.
            // We could also use the timeout specified by the next incoming client request (if
            // there is any).
            let timeout = time::Duration::from_secs(5);
            let leadership = match time::timeout(
                timeout,
                self.obtain_leadership(attempted_num, timeout),
            )
            .await
            {
                Ok(Ok(l)) => l,
                Ok(Err(ObtainLeadershipFailure::Rejected(num))) => {
                    assert!(num >= highest_known_balnum);
                    info!("{}: obtain leadership: rejected by {:?}", self.name, num);
                    highest_known_balnum = num;
                    continue;
                }
                Ok(Err(ObtainLeadershipFailure::NodesUnreachable)) => {
                    info!("{}: obtain leadership nodes unreachable", self.name);
                    continue;
                }
                Ok(Err(ObtainLeadershipFailure::ChannelDropped)) => {
                    info!("{}: obtain leadership channel dropped", self.name);
                    return ProposerFailure::ChannelDropped;
                }
                Err(time::Elapsed { .. }) => {
                    info!("{}: obtain leadership timeout", self.name);
                    continue;
                }
            };

            let Leadership {
                leader,
                outstanding_commit,
                next_slot,
            } = leadership;

            let num = match leader.run(outstanding_commit, next_slot).await {
                LeaderFailure::LostLeadership(num) => num,
                LeaderFailure::NodesUnreachable => {
                    info!(
                        "{}: gave up leadership due to nodes being unreachable",
                        self.name
                    );
                    None
                }
                LeaderFailure::ChannelDropped => {
                    info!("{}: leader channel dropped", self.name);
                    return ProposerFailure::ChannelDropped;
                }
            };

            if let Some(num) = num {
                assert!(num >= highest_known_balnum);
                highest_known_balnum = num;
            }
        }
    }

    async fn get_suggested_leader(&mut self) -> Result<Option<NodeId>, ()> {
        let (resp_tx, resp_rx) = oneshot::channel();

        if let Err(_) = self.ufd_query_tx.send(resp_tx).await {
            info!("{}: ufd send channel dropped", self.name);
            return Err(());
        }

        resp_rx.await.map_err(|_| {
            info!("{}: ufd response channel dropped", self.name);
        })
    }

    /*
     * Proposer recovery procedure:
     * 1. ask acceptors to perform Promise with the picked ballot (`attempted_num`)
     * 2. if a majority answers, calculate the greatest accepted proposal
     * 3. if every member of the majority returned the same proposal, recovery is finished.
     *    Otherwise, repropose it with out ballot (Repropose from the specification)
     *    and wait until a quorum of acceptors accept the new proposal.
     */
    async fn obtain_leadership(
        &'a mut self,
        attempted_num: Balnum,
        timeout: time::Duration,
    ) -> Result<Leadership<'a>, ObtainLeadershipFailure> {
        info!(
            "{}: attempting to obtain leadership: {:?}",
            self.name, attempted_num
        );
        let (acceptor_resp_tx, mut acceptor_resp_rx) = mpsc::channel(32);
        let req = Timed {
            timeout,
            request: ProposerRequest::Promise(attempted_num, acceptor_resp_tx),
        };
        self.acceptor_bcast_tx
            .send(req)
            .await
            .map_err(|mpsc::error::SendError(_)| ObtainLeadershipFailure::ChannelDropped)?;

        let majority = self.acceptor_config.size() / 2 + 1;

        let mut promises = 0;
        let mut responded_acceptors = HashSet::new();

        let mut highest_proposal: Option<Proposal> = None;
        let mut equal_proposals = true;

        while let Some(msg) = acceptor_resp_rx.recv().await {
            match msg {
                Response(PromiseResponse::Promised(id, proposal)) => {
                    if !responded_acceptors.insert(id) {
                        continue;
                    }

                    if promises > 0 && proposal != highest_proposal {
                        equal_proposals = false;
                    }

                    if proposal > highest_proposal {
                        highest_proposal = proposal;
                    }

                    promises += 1;
                    if promises >= majority {
                        break;
                    }
                }
                Response(PromiseResponse::Rejected(num)) => {
                    assert!(num >= attempted_num);
                    return Err(ObtainLeadershipFailure::Rejected(num));
                }
                Timeout(id) => {
                    info!("{}: node {:?} timeout (promise)", self.name, id);
                }
                NetworkError(id, m) => {
                    info!("{}: node {:?} network error {} (promise)", self.name, id, m);
                }
            }
        }

        if promises < majority {
            return Err(ObtainLeadershipFailure::NodesUnreachable);
        }

        info!(
            "{}: got majority of promises for {:?}",
            self.name, attempted_num
        );
        let (outstanding_commit, next_slot) = if let Some(highest_prop) = highest_proposal {
            let prop = if equal_proposals {
                highest_prop
            } else {
                // we don't have a proof that this proposal's decree was chosen;
                // we must finish choosing it before we start sending our own proposals
                let refreshed_prop = Proposal {
                    dec: highest_prop.dec.clone(),
                    num: attempted_num,
                    slot: highest_prop.slot,
                };

                info!("{}: refreshing proposal: {:?}", self.name, refreshed_prop);
                propose(
                    &self.name,
                    &self.acceptor_config,
                    &mut self.acceptor_bcast_tx,
                    refreshed_prop.clone(),
                    timeout,
                )
                .await
                .map_err(|e| match e {
                    ProposeFailure::ChannelDropped => ObtainLeadershipFailure::ChannelDropped,
                    ProposeFailure::NodesUnreachable => ObtainLeadershipFailure::NodesUnreachable,
                    ProposeFailure::Preempted(num) => ObtainLeadershipFailure::Rejected(num),
                })?;

                refreshed_prop
            };
            let slot = prop.slot + 1;
            (Some(make_commit(prop)), slot)
        } else {
            (None, 1)
        };

        let (leadership_checker, leadership_lost) = start_leadership_checker(
            self.name.clone(),
            attempted_num,
            self.acceptor_bcast_tx.clone(),
        );

        Ok(Leadership {
            leader: Leader {
                //id: self.id,
                name: &self.name,

                num: attempted_num,

                client_request_rx: &mut self.client_request_rx,

                acceptor_bcast_tx: &mut self.acceptor_bcast_tx,
                acceptor_config: &self.acceptor_config,

                replica_bcast_tx: &mut self.replica_bcast_tx,
                replica_config: &self.replica_config,

                _leadership_checker: leadership_checker,
                leadership_lost,
            },
            outstanding_commit,
            next_slot,
        })
    }

    async fn get_local_acceptor_balnum(&mut self) -> Result<Balnum, ()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = GetBalnumRequest(resp_tx);
        self.local_acceptor_tx
            .send(req)
            .await
            .map_err(|mpsc::error::SendError(_)| ())?;
        resp_rx.await.map_err(|_| ())
    }
}

impl<'a> Leader<'a> {
    async fn commit_and_read(
        &mut self,
        req: CommitAndReadRequest,
        timeout: time::Duration,
    ) -> CommitAndReadResult {
        let (replica_resp_tx, mut replica_resp_rx) = mpsc::channel(32);
        let req = Timed {
            timeout: timeout,
            request: (req, replica_resp_tx),
        };
        if let Err(_) = self.replica_bcast_tx.send(req).await {
            return CommitAndReadResult::ChannelDropped;
        }

        let majority = self.replica_config.size() / 2 + 1;

        let mut responses = 0;
        let mut responded_replicas = HashSet::new();

        let mut merged_patch: VersionedPatch = HashMap::new();

        // todo: the requests are timed, so the other end of the channel will
        // return a timeout message in case replicas don't respond in time,
        // but to be safe we could timeout on our own here as well
        use CommitAndReadResponse::*;
        while let Some(msg) = replica_resp_rx.recv().await {
            match msg {
                Response(Preempted) => return CommitAndReadResult::Preempted,
                Response(AlreadyHandled {
                    read_patch,
                    preempted,
                }) => {
                    return CommitAndReadResult::AlreadyHandled {
                        read_patch,
                        preempted,
                    }
                }
                Response(Versioned(id, p)) => {
                    if !responded_replicas.insert(id) {
                        continue;
                    }

                    merge(&mut merged_patch, p);

                    responses += 1;
                    if responses >= majority {
                        break;
                    }
                }
                Timeout(id) => {
                    info!("{}: node {:?} timeout (commit/read)", self.name, id);
                }
                NetworkError(id, m) => {
                    info!(
                        "{}: node {:?} network error {} (commit/read)",
                        self.name, id, m
                    );
                }
            }
        }

        if responses < majority {
            return CommitAndReadResult::NodesUnreachable;
        }

        CommitAndReadResult::Reconciled(unversion(merged_patch))
    }

    /*
     * The ``normal operation'' loop:
     * 1. wait for a client request
     * 2. apply the proposal chosen in the previous operation and read the state required to handle
     *    the client request, by asking replicas to perform Apply + Read
     * 3. calculate a patch using the obtained state and the requested command
     * 4. propose a decree with this patch (ProposeNew action) and ask acceptors
     *    to Accept this proposal
     * 5. if a quorum of acceptors Accept, continue to the next iteration.
     */
    async fn run(
        mut self,
        mut outstanding_commit: Option<Commit>,
        mut next_slot: Slot,
    ) -> LeaderFailure {
        info!(
            "{}: successfully obtained leadership for {:?}",
            self.name, self.num
        );
        loop {
            assert_eq!(
                outstanding_commit.as_ref().map_or(1, |p| p.slot + 1),
                next_slot
            );

            let cr = select! {
                msg = self.client_request_rx.recv() => match msg {
                    Some(cr) => cr,
                    None => {
                        info!("{}: client request channel dropped", self.name);
                        return LeaderFailure::ChannelDropped;
                    }
                },
                // FIXME: if we're informed about losing leadership we should leave the leader
                // loop immediately (there is no point in trying to handle the next request, we
                // will to choose a proposal). However, select! doesn't prefer any branch over the other,
                // it picks the branch randomly.
                // Should try_recv on self.leadership_lost before entering the select!.
                msg = &mut self.leadership_lost => match msg {
                    Ok(num) => {
                        info!("{}: from leadership checker: lost leadership to {:?}", self.name, num);
                        return LeaderFailure::LostLeadership(Some(num));
                    }
                    Err(_) => {
                        info!("{}: leadership checker channel dropped", self.name);
                        return LeaderFailure::ChannelDropped;
                    }
                }
            };

            let ClientRequest {
                id: cr_id,
                read_set: cr_read_set,
                fun: cr_fun,
                resp_tx: cr_resp_tx,
                timeout: cr_timeout,
            } = cr;

            let req = CommitAndReadRequest {
                commit: outstanding_commit.clone(),
                read_set: cr_read_set,
                read_cr_id: cr_id,
                read_slot: next_slot - 1,
            };

            info!(
                "{}: sending car {:?}, cr_timeout: {:?}",
                self.name, req, cr_timeout
            );
            let read_patch = match self.commit_and_read(req, cr_timeout).await {
                CommitAndReadResult::Preempted => {
                    info!("{}: preempted (commit/read)", self.name);
                    if let Err(_) = cr_resp_tx.send(ClientResponse::LostLeadership(None, false)) {
                        info!("{}: client request {:?} response channel dropped when sending LostLeadership (commit/read)", self.name, cr_id);
                    }
                    return LeaderFailure::LostLeadership(None);
                }
                CommitAndReadResult::AlreadyHandled {
                    read_patch,
                    preempted,
                } => {
                    // A replica told us that this request was already handled and it already had
                    // the output. We can immediately return it to the client and proceed to the
                    // next request.
                    // Note that this means that the replica did not apply the outstanding commit
                    // (see replica's code) even though it could. But that's fine: it will be
                    // resent when handling the next request.
                    if let Err(_) = cr_resp_tx.send(ClientResponse::Success(read_patch)) {
                        info!("{}: client request {:?} response channel dropped when sending Success (commit/read, already handled)", self.name, cr_id);
                    }

                    if preempted {
                        // A replica told us that it saw a higher slot than the one we've sent,
                        // meaning that there's a higher-ballot proposer running already.
                        return LeaderFailure::LostLeadership(None);
                    }

                    continue;
                }
                CommitAndReadResult::Reconciled(p) => p,

                CommitAndReadResult::NodesUnreachable => {
                    info!(
                        "{}: nodes unreachable while handling {:?} (commit/read)",
                        self.name, cr_id
                    );
                    if let Err(_) = cr_resp_tx.send(ClientResponse::NodesUnreachable) {
                        info!("{}: client request {:?} response channel dropped when sending NodesUnreachable (commit/read)", self.name, cr_id);
                    }
                    return LeaderFailure::NodesUnreachable;
                }
                CommitAndReadResult::ChannelDropped => {
                    info!("{}: channel dropped (commit/read)", self.name);
                    return LeaderFailure::ChannelDropped;
                }
            };

            let proposal = Proposal {
                dec: PatchDecree {
                    cr_id,
                    read_patch: read_patch.clone(),
                    write_patch: cr_fun(&read_patch),
                },
                num: self.num,
                slot: next_slot,
            };

            info!("{}: sending proposal {:?}", self.name, proposal);
            match propose(
                &self.name,
                &self.acceptor_config,
                &mut self.acceptor_bcast_tx,
                proposal.clone(),
                cr_timeout,
            )
            .await
            {
                Ok(()) => {
                    outstanding_commit = Some(make_commit(proposal));
                    next_slot += 1;

                    if let Err(_) = cr_resp_tx.send(ClientResponse::Success(read_patch)) {
                        info!("{}: client request {:?} response channel dropped when sending Success (propose)", self.name, cr_id);
                    }
                }
                Err(ProposeFailure::NodesUnreachable) => {
                    if let Err(_) = cr_resp_tx.send(ClientResponse::NodesUnreachable) {
                        info!("{}: client request {:?} response channel dropped when sending NodesUnreachable (propose)", self.name, cr_id);
                    }
                    return LeaderFailure::NodesUnreachable;
                }
                Err(ProposeFailure::ChannelDropped) => {
                    info!("{}: propose channel dropped", self.name);
                    return LeaderFailure::ChannelDropped;
                }
                Err(ProposeFailure::Preempted(num)) => {
                    info!("{}: preempted by {:?}", self.name, num);
                    if let Err(_) =
                        cr_resp_tx.send(ClientResponse::LostLeadership(Some(num.id), true))
                    {
                        info!("{}: client request {:?} response channel dropped when sending LostLeadership (propose)", self.name, cr_id);
                    }
                    return LeaderFailure::LostLeadership(Some(num));
                }
            }
        }
    }
}
