pub mod client {
    tonic::include_proto!("client");
}

use tokio::time;
use tonic::{Request, Response, Status};

use client::kv_server::{Kv, KvServer};
use client::response;

use super::{front::*, patch_decree::*, proposer, proposer::*};

type ClientRequest = client::Request;
type ClientResponse = client::Response;

pub struct ClientService {
    iface: ProposerClientInterface,
}

impl ClientService {
    pub fn new(iface: ProposerClientInterface) -> Self {
        Self { iface }
    }
}

#[tonic::async_trait]
impl Kv for ClientService {
    async fn execute(
        &self,
        req: Request<ClientRequest>,
    ) -> Result<Response<ClientResponse>, Status> {
        let ClientRequest { id, timeout, body } = req.into_inner();
        let (tx, rs) = match parse_and_compile(&body) {
            Ok(r) => r,
            Err(e) => {
                return Ok(Response::new(ClientResponse {
                    result: Some(response::Result::CompileError(format!("{}", e))),
                }))
            }
        };

        let mut iface = self.iface.clone();
        let req = iface
            .request(
                ClientRequestId(id),
                rs,
                tx,
                time::Duration::from_millis(timeout),
            )
            .await;

        use proposer::ClientResponse::*;
        use response::Result as R;
        Ok(Response::new(ClientResponse {
            result: Some(match req {
                Ok(Success(rp)) => R::Read(client::ReadResult { value: rp }),
                Ok(Redirect(to)) => R::RedirectTo(client::RedirectTo {
                    maybe_id: to.map(|to| client::redirect_to::MaybeId::Id(to.0)),
                }),
                Ok(NodesUnreachable) => R::NodesUnreachable(client::Empty {}),
                Ok(LostLeadership(to, could_handle)) => R::LostLeadership(client::LostLeadership {
                    maybe_id: to.map(|to| client::lost_leadership::MaybeId::Id(to.0)),
                    could_handle,
                }),
                Ok(Timeout) => R::Timeout(client::Empty {}),
                Err(RequestFailure::TooManyRequests) => R::TooManyRequests(client::Empty {}),
                Err(RequestFailure::ChannelDropped) => R::Closing(client::Empty {}),
            }),
        }))
    }
}

pub fn new_kv_server(iface: ProposerClientInterface) -> KvServer<ClientService> {
    KvServer::new(ClientService::new(iface))
}
