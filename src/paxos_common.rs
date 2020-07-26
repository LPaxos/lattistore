use std::cmp::Ordering;

use super::common::*;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Balnum {
    pub fst: u64,
    pub id: NodeId,
}

#[derive(Debug, Clone)]
pub struct Proposal<Decree> {
    pub dec: Decree,
    pub num: Balnum,
    pub slot: Slot,
}

impl Ord for Balnum {
    fn cmp(&self, other: &Self) -> Ordering {
        // compare number first, then id
        (self.fst, self.id).cmp(&(other.fst, other.id))
    }
}

impl PartialOrd for Balnum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq for Proposal<T> {
    fn eq(&self, other: &Self) -> bool {
        (self.slot, self.num).eq(&(other.slot, other.num))
    }
}

impl<T> Eq for Proposal<T> {}

impl<T> PartialOrd for Proposal<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Proposal<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.slot, self.num).cmp(&(other.slot, other.num))
    }
}

pub const MIN_BALNUM: Balnum = Balnum {
    fst: 0,
    id: MIN_NODE_ID,
};
