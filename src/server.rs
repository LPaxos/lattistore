extern crate lattistore;
mod client_service;
mod id_provider_service;
mod node_services;

use id_provider::id_provider_client::IdProviderClient;
use id_provider::IdRequest;

use node_services::node::acceptor_client::AcceptorClient;
use node_services::node::replica_client::ReplicaClient;

use tonic::transport::channel::Channel;
use tonic::{transport::Server, Request};

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::future::join_all;
use futures::future::FutureExt;
use tokio::select;
use tokio::sync::oneshot;
use tokio::time;

use client_service::*;
use id_provider_service::*;
use lattistore::{
    acceptor::AcceptorFailure, common::*, patch_decree::*, proposer::*, replica::*, *,
};
use node_services::*;

use std::net::{IpAddr, SocketAddr};

use clap::Clap;

use rand::Rng;

use log::info;

type Decree = PatchDecree;
type Acceptor = acceptor::Acceptor<Decree>;
type AcceptorInterface = acceptor::AcceptorInterface<Decree>;

use acceptor::ProposeResponse;
type PromiseResponse = acceptor::PromiseResponse<Decree>;

type AcceptorChannel = AcceptorClient<Channel>;
type ReplicaChannel = ReplicaClient<Channel>;

#[derive(Clap)]
struct Opts {
    #[clap(short, long)]
    my_ip: IpAddr,
    #[clap(short, long, required = true)]
    node_ips: Vec<IpAddr>,
}

#[tokio::main]
async fn main() {
    use simplelog::*;
    SimpleLogger::init(LevelFilter::Info, Config::default()).unwrap();

    let opts = Opts::parse();
    if !opts.node_ips.contains(&opts.my_ip) {
        println!("This node's IP must be present in the list of node IPs");
        std::process::exit(1);
    }

    info!("node ips: {:?}", opts.node_ips);
    info!("my ip: {:?}", opts.my_ip);

    let mut rng = rand::thread_rng();
    let my_id = NodeId(rng.gen());

    let addr = SocketAddr::new(opts.my_ip, 50051);

    info!("serving ID...");
    let (id_provider_shutdown, shutdown_rx) = oneshot::channel();
    let id_provider_handle = tokio::spawn(
        Server::builder()
            .add_service(new_id_provider_server(my_id))
            .serve_with_shutdown(
                addr,
                (|| async move {
                    shutdown_rx.await.unwrap();
                })(),
            ),
    );

    let other_ips: Vec<IpAddr> = opts
        .node_ips
        .iter()
        .filter(|&ip| &opts.my_ip != ip)
        .cloned()
        .collect();

    info!("Retrieving others' IDs...");
    let id_to_ip: HashMap<NodeId, IpAddr> = join_all(
        other_ips
            .into_iter()
            .map(|ip| tokio::spawn((|| async move { (retrieve(ip).await, ip) })())),
    )
    .await
    .into_iter()
    .map(|r| r.unwrap())
    .collect();
    //id_to_ip.insert(my_id, opts.my_ip);

    id_provider_shutdown.send(()).unwrap();
    id_provider_handle.await.unwrap().unwrap();

    info!("IDs retrieved: {:?}", id_to_ip);
    let num_nodes = id_to_ip.len() + 1;

    let (acc, acc_iface) = Acceptor::new(format!("acc"));
    let (rep, rep_iface) = Replica::new(my_id, format!("rep"));
    let (prop, prop_cl_iface, prop_acc_iface, prop_repl_iface, prop_ufd_iface) = Proposer::new(
        my_id,
        format!("pro"),
        AcceptorConfig::new(num_nodes),
        ReplicaConfig::new(num_nodes),
    );

    let acc_chans: HashMap<NodeId, AcceptorChannel> = join_all(id_to_ip.iter().map(|(&id, ip)| {
        AcceptorClient::connect(format!("http://{}:{}", ip, 50051)).map(move |c| (id, c.unwrap()))
    }))
    .await
    .into_iter()
    .collect();
    let rep_chans: HashMap<NodeId, ReplicaChannel> = join_all(id_to_ip.iter().map(|(&id, ip)| {
        ReplicaClient::connect(format!("http://{}:{}", ip, 50051)).map(move |c| (id, c.unwrap()))
    }))
    .await
    .into_iter()
    .collect();

    tokio::spawn(relay_acc(
        prop_acc_iface,
        my_id,
        acc_iface.clone(),
        acc_chans,
    ));
    tokio::spawn(relay_rep(
        prop_repl_iface,
        my_id,
        rep_iface.clone(),
        rep_chans,
    ));
    tokio::spawn(relay_ufd(
        prop_ufd_iface,
        my_id,
        id_to_ip.keys().cloned().collect(),
    ));

    tokio::spawn(acc.run());
    tokio::spawn(rep.run());
    tokio::spawn(prop.run());

    Server::builder()
        .add_service(new_id_provider_server(my_id))
        .add_service(new_kv_server(prop_cl_iface))
        .add_service(new_acceptor_server(acc_iface))
        .add_service(new_replica_server(rep_iface))
        .serve(addr)
        .await
        .unwrap();
}

/*
 * A task that forwards messages from the local proposer to the acceptors in the cluster.
 */
async fn relay_acc(
    mut iface: ProposerAcceptorInterface,
    my_id: NodeId,
    my_acc: AcceptorInterface,
    chans: HashMap<NodeId, AcceptorChannel>,
) {
    loop {
        // todo: don't clone, pass references and keep the handles?
        // hard to maintaining lifetime without cloning though
        select! {
            msg = iface.acceptor_bcast_rx.recv() => match msg {
                Some(m) => { tokio::spawn(acc_bcast(chans.clone(), my_id, my_acc.clone(), m)); }
                None => { break; }
            },
            msg = iface.local_acceptor_rx.recv() => match msg {
                Some(m) => { tokio::spawn(acc_local(my_acc.clone(), m)); }
                None => { break; }
            },
        }
    }
    info!("proposer closing");
}

/*
 * A task that forwards messages from the local proposer to the replicas in the cluster.
 */
async fn relay_rep(
    mut iface: ProposerReplicaInterface,
    my_id: NodeId,
    my_rep: ReplicaInterface,
    chans: HashMap<NodeId, ReplicaChannel>,
) {
    loop {
        match iface.replica_bcast_rx.recv().await {
            Some(m) => {
                tokio::spawn(rep_bcast(chans.clone(), my_id, my_rep.clone(), m));
            }
            None => {
                break;
            }
        }
    }
    info!("proposer closing");
}

/*
 * A task that forwards queries from the local proposer to the local oracle of the Omega
 * unreliable failure detector.
 *
 * We're using a placeholder implementation that picks the suggested leader based on the current
 * time. Definitely do not do this in production.
 */
async fn relay_ufd(mut iface: ProposerUFDInterface, my_id: NodeId, other_ids: Vec<NodeId>) {
    let mut all_ids = other_ids;
    all_ids.push(my_id);
    all_ids.sort();

    loop {
        match iface.ufd_query_rx.recv().await {
            Some(resp_tx) => {
                let secs_since_epoch = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                // rotate about every 5 secs
                let node_ix = ((secs_since_epoch / 5) % all_ids.len() as u64) as usize;
                let id = all_ids[node_ix];

                if let Err(e) = resp_tx.send(Some(id)) {
                    info!("relay_ufd: response channel dropped {:?}", e);
                }
            }
            None => {
                break;
            }
        }
    }
    info!("proposer closing");
}

/*
 * Broadcast a message from the local proposer to the acceptors in the cluster. Wait for responses
 * and return them back to the proposer.
 */
// todo: there's a lot of similar code below. Use some trait or macro to deduplicate?
async fn acc_bcast(
    chans: HashMap<NodeId, AcceptorChannel>,
    my_id: NodeId,
    mut my_acc: AcceptorInterface,
    m: Timed<ProposerRequest>,
) {
    use ProposerRequest::*;
    match m {
        Timed {
            request: Promise(num, resp_tx),
            timeout,
        } => {
            let iter = chans
                .into_iter()
                .map(|(id, ch)| (resp_tx.clone(), id, ch))
                .map(|(resp_tx, id, mut ch)| async move {
                    match time::timeout(timeout, ch.promise(to_balnum(num))).await {
                        Ok(Ok(r)) => {
                            // FIXME: don't `unwrap`?
                            // Although failure here happens only when some fields are missing in
                            // the promise response message, which should not happen; if it
                            // happens, it's a byzantine failure, so maybe crashing is the best
                            // option.
                            let resp = from_promise_response(id, r.into_inner()).unwrap();
                            respond(resp_tx, Failable::Response(resp)).await;
                        }
                        Ok(Err(e)) => {
                            respond(resp_tx, from_grpc_error(id, e)).await;
                        }
                        Err(time::Elapsed { .. }) => {
                            respond(resp_tx, Failable::Timeout(id)).await;
                        }
                    }
                });
            tokio::join!(
                join_all(iter),
                (|| async move {
                    match time::timeout(timeout, my_acc.promise(num)).await {
                        Ok(Ok(r)) => {
                            use proposer::PromiseResponse::*;
                            let resp = match r {
                                PromiseResponse::Promised(p) => Promised(my_id, p),
                                PromiseResponse::Rejected(num) => Rejected(num),
                            };
                            respond(resp_tx, Failable::Response(resp)).await;
                        }
                        Ok(Err(AcceptorFailure::ChannelDropped)) => {
                            info!("acc_bcast: acceptor channel dropped");
                            return;
                        }
                        Err(time::Elapsed { .. }) => {
                            respond(resp_tx, Failable::Timeout(my_id)).await;
                        }
                    }
                })()
            );
        }
        // todo: find a way to do this without cloning, using references
        // We can keep the proposal alive and somehow prove it to the inner lambdas
        // using the fact that we join the futures.
        Timed {
            request: Propose(p, resp_tx),
            timeout,
        } => {
            let iter = chans
                .into_iter()
                .map(|(id, ch)| (resp_tx.clone(), p.clone(), id, ch))
                .map(|(resp_tx, p, id, mut ch)| async move {
                    match time::timeout(timeout, ch.propose(to_proposal(p))).await {
                        Ok(Ok(r)) => {
                            let resp = from_propose_response(id, r.into_inner()).unwrap();
                            respond(resp_tx, Failable::Response(resp)).await;
                        }
                        Ok(Err(e)) => {
                            respond(resp_tx, from_grpc_error(id, e)).await;
                        }
                        Err(time::Elapsed { .. }) => {
                            respond(resp_tx, Failable::Timeout(id)).await;
                        }
                    }
                });
            tokio::join!(
                join_all(iter),
                (|| async move {
                    match time::timeout(timeout, my_acc.propose(p)).await {
                        Ok(Ok(r)) => {
                            use proposer::ProposeResponse::*;
                            let resp = match r {
                                ProposeResponse::Accepted => Accepted(my_id),
                                ProposeResponse::Preempted(num, slot) => Preempted(num, slot),
                            };
                            respond(resp_tx, Failable::Response(resp)).await;
                        }
                        Ok(Err(AcceptorFailure::ChannelDropped)) => {
                            info!("acc_bcast: acceptor channel dropped");
                            return;
                        }
                        Err(time::Elapsed { .. }) => {
                            respond(resp_tx, Failable::Timeout(my_id)).await;
                        }
                    }
                })()
            );
        }
        Timed {
            request: GetBalnum(resp_tx),
            timeout,
        } => {
            let iter = chans
                .into_iter()
                .map(|(id, ch)| (resp_tx.clone(), id, ch))
                .map(|(resp_tx, id, mut ch)| async move {
                    match time::timeout(timeout, ch.get_balnum(node::Empty {})).await {
                        Ok(Ok(r)) => {
                            let resp = from_balnum(r.into_inner());
                            respond(resp_tx, Failable::Response(resp)).await;
                        }
                        Ok(Err(e)) => {
                            respond(resp_tx, from_grpc_error(id, e)).await;
                        }
                        Err(time::Elapsed { .. }) => {
                            respond(resp_tx, Failable::Timeout(id)).await;
                        }
                    }
                });
            tokio::join!(
                join_all(iter),
                (|| async move {
                    match time::timeout(timeout, my_acc.get_balnum()).await {
                        Ok(Ok(r)) => {
                            respond(resp_tx, Failable::Response(r)).await;
                        }
                        Ok(Err(AcceptorFailure::ChannelDropped)) => {
                            info!("acc_bcast: acceptor channel dropped");
                            return;
                        }
                        Err(time::Elapsed { .. }) => {
                            respond(resp_tx, Failable::Timeout(my_id)).await;
                        }
                    }
                })()
            );
        }
    }
}

/*
 * Forward a message from the local proposer to the local acceptor.
 */
async fn acc_local(mut acc: AcceptorInterface, m: proposer::GetBalnumRequest) {
    let proposer::GetBalnumRequest(resp_tx) = m;
    match acc.get_balnum().await {
        Ok(r) => {
            if let Err(e) = resp_tx.send(r) {
                info!("acc_local (get_balnum): response channel dropped {:?}", e);
            }
        }
        Err(AcceptorFailure::ChannelDropped) => {
            info!("acc_bcast: acceptor channel dropped");
            return;
        }
    }
}

/*
 * Broadcast a message from the local proposer to the replicas in the cluster. Wait for responses
 * and return them back to the proposer.
 */
async fn rep_bcast(
    chans: HashMap<NodeId, ReplicaChannel>,
    my_id: NodeId,
    mut my_rep: ReplicaInterface,
    m: Timed<ProposerReplicaRequest>,
) {
    let Timed {
        request: (car, resp_tx),
        timeout,
    } = m;
    let iter = chans
        .into_iter()
        .map(|(id, ch)| (resp_tx.clone(), car.clone(), id, ch))
        .map(|(resp_tx, car, id, mut ch)| async move {
            match time::timeout(timeout, ch.commit_and_read(to_commit_and_read_request(car))).await
            {
                Ok(Ok(r)) => {
                    let resp = from_commit_and_read_response(id, r.into_inner()).unwrap();
                    respond(resp_tx, Failable::Response(resp)).await;
                }
                Ok(Err(e)) => {
                    respond(resp_tx, from_grpc_error(id, e)).await;
                }
                Err(time::Elapsed { .. }) => {
                    respond(resp_tx, Failable::Timeout(id)).await;
                }
            }
        });
    tokio::join!(
        join_all(iter),
        (|| async move {
            match time::timeout(timeout, my_rep.commit_and_read(car)).await {
                Ok(Ok(r)) => {
                    respond(resp_tx, Failable::Response(r)).await;
                }
                Ok(Err(ReplicaFailure::ChannelDropped)) => {
                    info!("rep_bcast: replica channel dropped");
                    return;
                }
                Err(time::Elapsed { .. }) => {
                    respond(resp_tx, Failable::Timeout(my_id)).await;
                }
            }
        })()
    );
}

async fn retrieve(from: IpAddr) -> NodeId {
    info!("Retrieving ID from {}", from);
    let mut client = IdProviderClient::connect(format!("http://{}:{}", from, 50051))
        .await
        .unwrap();
    loop {
        let resp = client.get_id(Request::new(IdRequest {})).await;
        match resp {
            Ok(r) => return NodeId(r.into_inner().node_id),
            Err(e) => {
                info!(
                    "Couldn't retrieve ID from {}: {}.\nRetrying in 1s...",
                    from, e
                );
                time::delay_for(time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn respond<T: std::fmt::Debug>(mut chan: tokio::sync::mpsc::Sender<T>, m: T) {
    if let Err(e) = chan.send(m).await {
        info!("respond: response channel dropped {:?}", e);
    }
}

fn from_grpc_error<T>(id: NodeId, e: tonic::Status) -> Failable<T> {
    Failable::NetworkError(
        id,
        format!("grpc code: {}, message: {}", e.code(), e.message()),
    )
}
