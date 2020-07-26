use tokio::sync::mpsc;
use tokio::sync::oneshot;

use log::info;

use super::common::*;
use super::paxos_common::*;

#[derive(Debug)]
enum ProposerRequest<Decree> {
    Promise(Balnum, oneshot::Sender<PromiseResponse<Decree>>),
    Propose(Proposal<Decree>, oneshot::Sender<ProposeResponse>),
    GetBalnum(oneshot::Sender<Balnum>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum PromiseResponse<Decree> {
    Promised(Option<Proposal<Decree>>),
    Rejected(Balnum),
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProposeResponse {
    Accepted,
    Preempted(Balnum, Slot),
}

#[derive(Debug)]
pub enum AcceptorFailure {
    ChannelDropped,
}

impl<T> From<mpsc::error::SendError<T>> for AcceptorFailure {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        Self::ChannelDropped
    }
}

impl From<oneshot::error::RecvError> for AcceptorFailure {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::ChannelDropped
    }
}

#[derive(Clone)]
pub struct AcceptorInterface<Decree> {
    to_acc_tx: mpsc::Sender<ProposerRequest<Decree>>,
}

impl<Decree> AcceptorInterface<Decree> {
    pub async fn promise(
        &mut self,
        num: Balnum,
    ) -> Result<PromiseResponse<Decree>, AcceptorFailure> {
        let (tx, rx) = oneshot::channel();
        self.to_acc_tx
            .send(ProposerRequest::Promise(num, tx))
            .await?;
        Ok(rx.await?)
    }

    pub async fn propose(
        &mut self,
        p: Proposal<Decree>,
    ) -> Result<ProposeResponse, AcceptorFailure> {
        let (tx, rx) = oneshot::channel();
        self.to_acc_tx.send(ProposerRequest::Propose(p, tx)).await?;
        Ok(rx.await?)
    }

    pub async fn get_balnum(&mut self) -> Result<Balnum, AcceptorFailure> {
        let (tx, rx) = oneshot::channel();
        self.to_acc_tx.send(ProposerRequest::GetBalnum(tx)).await?;
        Ok(rx.await?)
    }
}

pub struct Acceptor<Decree> {
    name: String,
    to_acc_rx: mpsc::Receiver<ProposerRequest<Decree>>,

    max_prop: Option<Proposal<Decree>>,
    max_num: Balnum,
}

impl<Decree: Clone + std::fmt::Debug> Acceptor<Decree> {
    pub fn new(name: String) -> (Acceptor<Decree>, AcceptorInterface<Decree>) {
        let (to_acc_tx, to_acc_rx) = mpsc::channel(32);

        (
            Acceptor {
                name,
                to_acc_rx,
                max_prop: None,
                max_num: MIN_BALNUM,
            },
            AcceptorInterface { to_acc_tx },
        )
    }

    pub async fn run(mut self) {
        info!("{}: run", self.name);
        while let Some(msg) = self.to_acc_rx.recv().await {
            match msg {
                ProposerRequest::Promise(num, resp_tx) => {
                    resp_tx.send(self.promise(num).await).unwrap_or_else(|_| {
                        info!("{}: send promise response: channel dropped", self.name);
                    })
                }
                ProposerRequest::Propose(prop, resp_tx) => {
                    resp_tx.send(self.propose(prop).await).unwrap_or_else(|_| {
                        info!("{}: send propose response: channel dropped", self.name);
                    })
                }
                ProposerRequest::GetBalnum(resp_tx) => {
                    resp_tx.send(self.get_balnum().await).unwrap_or_else(|_| {
                        info!("{}: send getbalnum response: channel dropped", self.name);
                    })
                }
            }
        }

        info!("{}: closing", self.name);
    }

    async fn promise(&mut self, num: Balnum) -> PromiseResponse<Decree> {
        info!("{}: promise request {:?}", self.name, num);

        if self.max_num >= num {
            info!(
                "{}: rejected {:?}, max_num: {:?}",
                self.name, num, self.max_num
            );
            return PromiseResponse::Rejected(self.max_num);
        }

        info!(
            "{}: promising {:?}, max_prop: {:?}, max_num: {:?}",
            self.name, num, self.max_prop, self.max_num
        );
        self.max_num = num;
        PromiseResponse::Promised(self.max_prop.clone())
    }

    async fn propose(&mut self, prop: Proposal<Decree>) -> ProposeResponse {
        info!("{}: propose request {:?}", self.name, prop);
        let last_slot = (&self.max_prop.as_ref()).map_or(0, |p| p.slot);

        if self.max_num > prop.num || last_slot > prop.slot {
            info!(
                "{}: preempted {:?}, max_num: {:?}, last_slot: {:?}",
                self.name, prop.num, self.max_num, last_slot
            );
            return ProposeResponse::Preempted(self.max_num, last_slot);
        }

        info!(
            "{}: accepting {:?}, max_num: {:?}, last_slot: {:?}",
            self.name, prop, self.max_num, last_slot
        );
        self.max_num = prop.num;
        self.max_prop = Some(prop);
        ProposeResponse::Accepted
    }

    async fn get_balnum(&self) -> Balnum {
        self.max_num
    }
}
