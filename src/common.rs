use tokio::time;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct NodeId(pub i64);

pub type Slot = u64;

pub struct Timed<Request> {
    pub timeout: time::Duration,
    pub request: Request,
}

#[derive(Debug)]
pub enum Failable<Response> {
    Response(Response),
    Timeout(NodeId),
    NetworkError(NodeId, String), //FIXME: refine error types further
}

pub const MIN_NODE_ID: NodeId = NodeId(std::i64::MIN);
