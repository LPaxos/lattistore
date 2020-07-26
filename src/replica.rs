use std::collections::hash_map::Entry;
use std::collections::HashMap;

use log::info;

use tokio::sync::mpsc;
use tokio::sync::oneshot;

use super::common::*;
use super::patch::*;
use super::patch_decree::*;

#[derive(Debug)]
pub enum ReplicaFailure {
    ChannelDropped,
}

impl<T> From<mpsc::error::SendError<T>> for ReplicaFailure {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        Self::ChannelDropped
    }
}

impl From<oneshot::error::RecvError> for ReplicaFailure {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::ChannelDropped
    }
}

#[derive(Clone)]
pub struct ReplicaInterface {
    tx: mpsc::Sender<(CommitAndReadRequest, oneshot::Sender<CommitAndReadResponse>)>,
}

impl ReplicaInterface {
    pub async fn commit_and_read(
        &mut self,
        req: CommitAndReadRequest,
    ) -> Result<CommitAndReadResponse, ReplicaFailure> {
        let (tx, rx) = oneshot::channel();
        self.tx.send((req, tx)).await?;
        Ok(rx.await?)
    }
}

pub struct Replica {
    name: String,
    id: NodeId,
    rx: mpsc::Receiver<(CommitAndReadRequest, oneshot::Sender<CommitAndReadResponse>)>,

    last: Slot,
    state: VersionedPatch,
    reads: HashMap<ClientRequestId, Patch>,
}

impl Replica {
    pub fn new(id: NodeId, name: String) -> (Replica, ReplicaInterface) {
        let (tx, rx) = mpsc::channel(32);

        (
            Replica {
                id,
                name,
                rx,
                last: 0,
                state: HashMap::new(),
                reads: HashMap::new(),
            },
            ReplicaInterface { tx },
        )
    }

    pub async fn run(mut self) {
        info!("{}: run", self.name);

        while let Some(msg) = self.rx.recv().await {
            let (cr, resp_tx) = msg;
            resp_tx
                .send(self.commit_and_read(cr).await)
                .unwrap_or_else(|_| {
                    info!(
                        "{}: send CommitAndRead response: channel dropped",
                        self.name
                    );
                });
        }

        info!("{}: closing", self.name);
    }

    async fn commit_and_read(&mut self, req: CommitAndReadRequest) -> CommitAndReadResponse {
        let CommitAndReadRequest {
            commit,
            read_set,
            read_cr_id,
            read_slot,
        } = req;
        info!(
            "{}: request: commit {:?} read {:?} read_cr_id {:?} read_slot {:?}",
            self.name, commit, read_set, read_cr_id, read_slot
        );

        assert_eq!(commit.as_ref().map_or(0, |c| c.slot), read_slot);

        // we really want to compare self.last with c.slot, but see the assert above
        let preempted = self.last > read_slot;
        if preempted {
            info!(
                "{}: preempted; last: {:?}, read_slot: {:?}",
                self.name, self.last, read_slot
            );
        }

        if let Entry::Occupied(o) = self.reads.entry(read_cr_id) {
            info!("{}: already handled, rp: {:?}", self.name, o.get());
            // We could still apply the patch in `commit` but there's no need to.
            return CommitAndReadResponse::AlreadyHandled {
                read_patch: o.get().clone(),
                preempted,
            };
        }

        if preempted {
            // We could still apply the patch in `commit` but there's no need to.
            return CommitAndReadResponse::Preempted;
        }

        if let Some(c) = commit {
            self.last = c.slot; // self.last = max(self.last, c.slot)
            merge(&mut self.state, version(c.dec.write_patch, c.slot));
            let already_handled = self.reads.insert(c.dec.cr_id, c.dec.read_patch.clone()); // todo: cloned only for assert
            if let Some(p) = already_handled {
                assert_eq!(p, c.dec.read_patch);
                info!(
                    "{}: decree {:?} already committed, patch: {:?}",
                    self.name, c.dec.cr_id, p
                );
            }
        }

        let vp = restrict(&self.state, read_set);
        info!("{}: returning versioned patch {:?}", self.name, vp);
        CommitAndReadResponse::Versioned(self.id, vp)
    }
}
