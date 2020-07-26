use std::collections::hash_map::Entry;
use std::collections::HashMap;

use super::common::*;
use super::patch::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientRequestId(pub i64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchDecree {
    pub cr_id: ClientRequestId,
    pub read_patch: Patch,

    // `Patch` does not store versions of the keys.
    // This is an optimization. When applying a patch, replicas assume that the version of each
    // key present in the patch is equal to the slot of the apply request.
    pub write_patch: Patch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub dec: PatchDecree,
    pub slot: Slot,
}

// Asks a replica to perform `Apply` and then `Read` actions.
#[derive(Debug, Clone)]
pub struct CommitAndReadRequest {
    pub commit: Option<Commit>,
    pub read_set: Vec<Key>,
    pub read_cr_id: ClientRequestId,
    pub read_slot: Slot,
}

#[derive(Debug)]
pub enum CommitAndReadResponse {
    Preempted,
    AlreadyHandled { read_patch: Patch, preempted: bool },
    Versioned(NodeId, VersionedPatch),
}

pub type VersionedPatch = HashMap<Key, (Value, Slot)>;

pub fn merge(p1: &mut VersionedPatch, p2: VersionedPatch) {
    p1.reserve(p2.len());
    for (k, (val2, ver2)) in p2 {
        match p1.entry(k) {
            Entry::Vacant(v) => {
                v.insert((val2, ver2));
            }
            Entry::Occupied(mut o) => {
                let (ref mut val1, ref mut ver1) = o.get_mut();
                if ver2 > *ver1 {
                    *ver1 = ver2;
                    *val1 = val2;
                }
            }
        }
    }
}

pub fn unversion(p: VersionedPatch) -> Patch {
    p.into_iter().map(|(k, (val, _))| (k, val)).collect()
}

pub fn version(p: Patch, v: Slot) -> VersionedPatch {
    p.into_iter().map(|(k, val)| (k, (val, v))).collect()
}

pub fn restrict(p: &VersionedPatch, dom: Vec<Key>) -> VersionedPatch {
    let mut res = HashMap::new();
    for k in dom {
        if let Some(e) = p.get(&k) {
            res.insert(k, e.clone());
        }
    }
    res
}
