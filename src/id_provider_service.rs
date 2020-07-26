pub mod id_provider {
    tonic::include_proto!("id_provider");
}

use super::common::*;

use tonic::{Request, Response, Status};

use id_provider::{id_provider_server as id_server, IdRequest, IdResponse};
use id_server::*;

pub struct IdProviderService {
    my_id: NodeId,
}

impl IdProviderService {
    pub fn new(my_id: NodeId) -> Self {
        Self { my_id }
    }
}

#[tonic::async_trait]
impl id_server::IdProvider for IdProviderService {
    async fn get_id(&self, _: Request<IdRequest>) -> Result<Response<IdResponse>, Status> {
        Ok(Response::new(IdResponse {
            node_id: self.my_id.0,
        }))
    }
}

pub fn new_id_provider_server(id: NodeId) -> IdProviderServer<IdProviderService> {
    IdProviderServer::new(IdProviderService::new(id))
}
