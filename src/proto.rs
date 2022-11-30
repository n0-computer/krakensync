use ahash::AHashSet;
use bitvec::prelude::*;
use bytes::Bytes;
use cid::Cid;
use multihash::MultihashDigest;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Want(Query),
    Have(Query),
}

/// compact representation of a set of Cids
///
/// This could also be a probabilitic data structure like a bloom filter,
/// with a moderate false positive rate.
type CidSet = AHashSet<Cid>;

/// bitmap of blocks to get
type Bitmap = BitVec;

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub enum Traversal {
    /// depth-first traversal
    #[default]
    DepthFirst = 0,
    /// breadth-first traversal
    BreadthFirst = 1,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    /// left to right
    #[default]
    LeftToRight = 0,
    // right to left
    RightToLeft = 1,
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Query {
    pub root: Cid,

    /// cids to stop the traversal at (exclusive)
    pub stop: CidSet,

    /// depth of traversal
    pub depth: u64,

    /// traversal order
    pub traversal: Traversal,

    /// direction of traversal
    pub direction: Direction,

    /// bitmap of cids for which to send blocks
    pub bits: BitVec,
}

impl Debug for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Query")
            .field("root", &Dis(&self.root))
            .field("stop", &self.stop)
            .field("depth", &self.depth)
            .field("traversal", &self.traversal)
            .field("direction", &self.direction)
            .field("bits", &Dis(&fmt(&self.bits)))
            .finish()
    }
}

fn fmt(bits: &BitVec) -> String {
    bits.iter().map(|b| if *b { '1' } else { '0' }).collect()
}

struct Dis<T>(T);
impl<T: Display> Debug for Dis<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Query {
    pub fn depth(self, depth: u64) -> Self {
        Self { depth, ..self }
    }

    pub fn bits(self, bits: Bitmap) -> Self {
        Self { bits, ..self }
    }

    pub fn stop(self, stop: AHashSet<Cid>) -> Self {
        Self { stop, ..self }
    }

    pub fn direction(self, direction: Direction) -> Self {
        Self { direction, ..self }
    }

    pub fn traversal(self, traversal: Traversal) -> Self {
        Self { traversal, ..self }
    }

    pub fn new(root: Cid) -> Self {
        Self {
            root,
            depth: u64::MAX,
            traversal: Traversal::DepthFirst,
            direction: Direction::LeftToRight,
            bits: BitVec::repeat(true, 1024 * 64),
            stop: AHashSet::new(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Block {
    /// codec to use to reconstruct the Cid
    pub codec: u64,
    /// hash algo to use to reconstruct the Cid
    pub hash: u64,
    /// data of the block
    pub data: Bytes,
}

impl Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("codec", &self.codec)
            .field("hash", &self.hash)
            .field("data", &self.data.len())
            .finish()
    }
}

impl Block {
    pub fn new(cid: Cid, data: Bytes) -> Self {
        Self {
            codec: cid.codec(),
            hash: cid.hash().code(),
            data,
        }
    }

    pub fn cid(&self) -> Cid {
        let code = multihash::Code::try_from(self.hash).unwrap();
        let hash = code.digest(&self.data);
        Cid::new_v1(self.codec, hash)
    }
}

/// Update of an ongoing request
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WantRequestUpdate {
    /// Cancel values, e.g. if we got them already from another node
    Cancel(Bitmap),
    /// Request additional values, e.g. if another node has not delivered them
    Add(Bitmap),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Want response
pub enum WantResponse {
    /// Got a block.
    Block(usize, Block),
    /// Did not find a cid.
    NotFound(usize, Cid),
    /// We did not find a block and were unable to keep track of the index. Stream ends.
    StopNotFound(usize, Cid),
    /// Max blocks exceeded. Stream ends.
    MaxBlocksSent,
    /// Max bytes exceeded. Stream ends.
    MaxBytesSent,
    /// Max blocks exceeded. Stream ends.
    MaxBlocksRead,
    /// Max bytes exceeded. Stream ends.
    MaxBytesRead,
    /// Internal error in the store. Stream ends.
    InternalError(String),
}

impl WantResponse {
    pub fn internal_error(error: anyhow::Error) -> Self {
        Self::InternalError(error.to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HaveResponse {
    pub bitmap: BitVec,
    pub stop: Option<Cid>,
}

impl HaveResponse {
    pub fn is_complete(&self) -> bool {
        self.stop.is_none() && self.bitmap.all() && !self.bitmap.is_empty()
    }
}
