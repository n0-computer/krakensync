#![allow(clippy::type_complexity)]
use ahash::{AHashMap, AHashSet};
use bitvec::vec::BitVec;
use bytes::Bytes;
use cid::Cid;
use multihash::{Multihash, MultihashDigest};
use parking_lot::Mutex;
use std::{iter::FusedIterator, marker::PhantomData, sync::Arc};

trait StoreRead {
    fn has(&self, cid: &Cid) -> anyhow::Result<bool>;
    fn get(&self, cid: &Cid) -> anyhow::Result<Option<(Bytes, Arc<[Cid]>)>>;
}

trait StoreWrite {
    fn put(&self, cid: &Cid, data: &[u8], links: &[Cid]) -> anyhow::Result<()>;
    fn delete(&self, cid: &Cid) -> anyhow::Result<()>;
}

#[derive(Debug, Clone, Default)]
pub struct Store {
    blocks: Arc<Mutex<AHashMap<Cid, (Bytes, Arc<[Cid]>)>>>,
}

impl StoreRead for Store {
    fn has(&self, cid: &Cid) -> anyhow::Result<bool> {
        Ok(self.blocks.lock().get(cid).is_some())
    }

    fn get(&self, cid: &Cid) -> anyhow::Result<Option<(Bytes, Arc<[Cid]>)>> {
        Ok(self.blocks.lock().get(cid).cloned())
    }
}

impl StoreWrite for Store {
    fn put(&self, cid: &Cid, data: &[u8], links: &[Cid]) -> anyhow::Result<()> {
        self.blocks
            .lock()
            .insert(*cid, (data.to_vec().into(), links.to_vec().into()));
        Ok(())
    }

    fn delete(&self, cid: &Cid) -> anyhow::Result<()> {
        self.blocks.lock().remove(cid);
        Ok(())
    }
}

impl Store {
    pub fn get(&self, cid: &Cid) -> anyhow::Result<Option<(Bytes, Arc<[Cid]>)>> {
        Ok(self.blocks.lock().get(cid).cloned())
    }

    pub fn has(&self, cid: &Cid) -> anyhow::Result<bool> {
        Ok(self.blocks.lock().get(cid).is_some())
    }

    pub fn put(&self, cid: Cid, data: Bytes, links: Arc<[Cid]>) -> anyhow::Result<bool> {
        Ok(self.blocks.lock().insert(cid, (data, links)).is_some())
    }
}

trait StoreReadExt: StoreRead + Clone + Sized + 'static {
    fn have(&self, query: Query) -> anyhow::Result<BitVec> {
        match query.traversal {
            Traversal::DepthFirst => make_bitmap(
                self.clone(),
                DepthFirstTraversal::new(
                    self.clone(),
                    query.root,
                    query.depth as usize,
                    query.direction == Direction::LeftToRight,
                    query.stop,
                ),
            )
            .collect(),
            Traversal::BreadthFirst => make_bitmap(
                self.clone(),
                BreadthFirstTraversal::new(
                    self.clone(),
                    query.root,
                    query.depth as usize,
                    query.direction == Direction::LeftToRight,
                    query.stop,
                ),
            )
            .collect(),
        }
    }

    fn want(self, query: Query) -> Box<dyn Iterator<Item = WantResponse>> {
        #[allow(clippy::unnecessary_to_owned)]
        let bits = query.bits.into_iter();
        match query.traversal {
            Traversal::DepthFirst => Box::new(make_want(
                self.clone(),
                DepthFirstTraversal::new(
                    self,
                    query.root,
                    query.depth as usize,
                    query.direction == Direction::LeftToRight,
                    query.stop,
                ),
                bits,
                Default::default(),
            )),
            Traversal::BreadthFirst => Box::new(make_want(
                self.clone(),
                BreadthFirstTraversal::new(
                    self,
                    query.root,
                    query.depth as usize,
                    query.direction == Direction::LeftToRight,
                    query.stop,
                ),
                bits,
                Default::default(),
            )),
        }
    }
}

impl<S: StoreRead + Clone + Sized + 'static> StoreReadExt for S {}

fn make_bitmap(
    store: impl StoreRead,
    iter: impl Iterator<Item = anyhow::Result<(Cid, Extra)>>,
) -> impl Iterator<Item = anyhow::Result<bool>> {
    // todo: we could in theory do all has queries in one operation
    iter.take_while(|x| !matches!(x, Ok((_, Extra::StopNotFound))))
        .map(move |item| {
            let (cid, data) = item?;
            Ok(match data {
                Extra::Branch(_) => true,
                Extra::NotFound => false,
                Extra::Leaf => store.has(&cid)?,
                Extra::StopNotFound => unreachable!(),
            })
        })
}

fn make_want(
    store: impl StoreRead + 'static,
    iter: impl Iterator<Item = anyhow::Result<(Cid, Extra)>> + 'static,
    bitmap: impl Iterator<Item = bool> + 'static,
    limits: Limits,
) -> impl Iterator<Item = WantResponse> + 'static {
    let mut remaining_send_blocks = limits.send_blocks;
    let mut remaining_send_bytes = limits.send_bytes;
    let mut remaining_read_blocks = limits.read_blocks;
    let mut remaining_read_bytes = limits.read_bytes;
    let mut track_read_limits = move |data: &[u8]| -> std::result::Result<(), WantResponse> {
        if remaining_read_blocks == 0 {
            return Err(WantResponse::MaxBlocksRead);
        } else {
            remaining_read_blocks -= 1;
        }
        let data_len = data.len() as u64;
        if remaining_read_bytes < data_len {
            Err(WantResponse::MaxBytesRead)
        } else {
            remaining_read_bytes -= data_len;
            Ok(())
        }
    };
    // send a block, applying and decrementing the limits
    let mut send_block_limited =
        move |index: usize,
              cid: Cid,
              data: Bytes|
              -> std::result::Result<Option<WantResponse>, WantResponse> {
            if remaining_send_blocks == 0 {
                return Err(WantResponse::MaxBlocksSent);
            } else {
                remaining_send_blocks -= 1;
            }
            let data_len = data.len() as u64;
            if remaining_send_bytes < data_len {
                return Err(WantResponse::MaxBytesSent);
            } else {
                remaining_send_bytes -= data_len;
            }
            Ok(Some(WantResponse::Block(index, Block::new(cid, data))))
        };
    // zip all that is needed, and flatten it for convenience
    let flat = iter.enumerate().zip(bitmap).map(|((index, item), take)| {
        let (cid, data) = item?;
        anyhow::Ok((index, cid, data, take))
    });
    // iterate over the result, applying limits and skipping 0 bits in the bitmap
    limit(flat, move |item| {
        // terminate on internal error
        let (index, cid, data, take) = item.map_err(WantResponse::internal_error)?;
        match data {
            Extra::Branch(data) if take => {
                // it was a branch, so we already have the data
                track_read_limits(&data)?;
                send_block_limited(index, cid, data)
            }
            Extra::Branch(data) => {
                // it was a branch, but we don't want it
                // we still have to keep track of how much we read from the store
                track_read_limits(&data)?;
                Ok(None)
            }
            Extra::Leaf if take => {
                // it was a leaf, so we have to try to get the data
                if let Some((data, _)) = store.get(&cid).map_err(WantResponse::internal_error)? {
                    // we have to keep track of how much we read from the store
                    track_read_limits(&data)?;
                    send_block_limited(index, cid, data)
                } else {
                    Ok(Some(WantResponse::NotFound(index, cid)))
                }
            }
            Extra::Leaf => {
                // skipping a leaf does not require any bookkeeping
                Ok(None)
            }
            Extra::NotFound if take => {
                // terminate the iteration with a NotFound
                Ok(Some(WantResponse::NotFound(index, cid)))
            }
            Extra::NotFound => {
                // we got a NotFound, but can continue since take is false and order is not affected yet
                Ok(None)
            }
            Extra::StopNotFound => {
                // terminate the iteration with a NotFound
                Err(WantResponse::StopNotFound(index, cid))
            }
        }
    })
}

pub struct Limits {
    /// maximum number of blocks to send
    send_blocks: u64,
    /// maximum number of bytes to send
    send_bytes: u64,
    /// maximum number of blocks to read from the store
    read_blocks: u64,
    /// maximum number of bytes to read from the store
    read_bytes: u64,
}

impl Limits {
    fn min(&mut self, that: &Limits) {
        self.send_blocks = self.send_blocks.min(that.send_blocks);
        self.send_bytes = self.send_bytes.min(that.send_bytes);
        self.read_blocks = self.read_blocks.min(that.read_blocks);
        self.read_bytes = self.read_bytes.min(that.read_bytes);
    }
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            send_blocks: 1000,
            send_bytes: 1000 * 1024 * 1024,
            read_blocks: 1000,
            read_bytes: 1000 * 1024 * 1024,
        }
    }
}

/// Transform an iterator like filter_map, but additionally perform limit calculations.
///
/// Limit is called on each item and can either skip items `Ok(None)`, emit items `Ok(Some(item))`, or
/// terminate the iteration with an error value `Err(cause)` that will be the last element of the resulting
/// iterator.
///
/// The function is a `FnMut` so it can decrement some limits and terminate once they are exceeded
pub fn limit<I, R, F>(iter: I, f: F) -> Limit<I, F, R>
where
    I: Iterator,
    F: FnMut(I::Item) -> std::result::Result<Option<R>, R>,
{
    Limit {
        iter: Some(iter),
        f,
        _r: PhantomData,
    }
}

pub struct Limit<I, F, R> {
    iter: Option<I>,
    f: F,
    _r: PhantomData<R>,
}

impl<I, F, R> Iterator for Limit<I, F, R>
where
    I: Iterator,
    F: FnMut(I::Item) -> std::result::Result<Option<R>, R>,
{
    type Item = R;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.iter.as_mut()?.next()?;
            match (self.f)(item) {
                Ok(Some(r)) => {
                    // limit not reached, return the result
                    break Some(r);
                }
                Ok(None) => {
                    // limit not reached, skip the item
                    continue;
                }
                Err(r) => {
                    // limit reached, stop iterating and return limit marker
                    self.iter = None;
                    break Some(r);
                }
            }
        }
    }
}

impl<I, F, R> FusedIterator for Limit<I, F, R>
where
    I: Iterator,
    F: FnMut(I::Item) -> std::result::Result<Option<R>, R>,
{
}

macro_rules! unwrap_or {
    ($e:expr, $n:expr) => {
        match $e {
            Some(v) => v,
            None => $n,
        }
    };
}

/// A depth first iterator over a dag, limited by a maximum depth and a set of already visited nodes.
///
/// Nodes will be reported as leafs if they are at the maximum depth, even though they might have links.
struct DepthFirstTraversal<S> {
    store: S,
    visited: AHashSet<Cid>,
    stack: Vec<std::vec::IntoIter<Cid>>,
    max_depth: usize,
    left_to_right: bool,
}

impl<S: StoreRead> DepthFirstTraversal<S> {
    fn new(store: S, root: Cid, depth: usize, left_to_right: bool, visited: AHashSet<Cid>) -> Self {
        Self {
            store,
            visited,
            stack: vec![vec![root].into_iter()],
            max_depth: depth,
            left_to_right,
        }
    }

    fn next0(&mut self) -> anyhow::Result<Option<(Cid, Extra)>> {
        Ok(loop {
            // end the stream normally if there is nothing left to do
            let stack = unwrap_or!(self.stack.last_mut(), break None);
            // get the next cid from the stack for this level, or go up one level
            let cid = unwrap_or!(stack.next(), {
                self.stack.pop();
                continue;
            });
            // completely ignore visited cids, not even increase index
            if self.visited.contains(&cid) {
                continue;
            }
            self.visited.insert(cid);
            // we are at the max depth, so we don't need the links
            if self.stack.len() - 1 == self.max_depth {
                break Some((cid, Extra::Leaf));
            }
            // get the data for the cid, if we can't get it, abort
            let (data, links) = unwrap_or!(self.store.get(&cid)?, {
                break Some((cid, Extra::StopNotFound));
            });
            // push the links if there are any
            if !links.is_empty() {
                let mut links = links.to_vec();
                if !self.left_to_right {
                    // reverse to get right to left traversal
                    links.reverse();
                }
                self.stack.push(links.into_iter());
            }
            // return the cid and data
            break Some((cid, Extra::Branch(data)));
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Extra {
    // a branch for which we have the data
    Branch(Bytes),
    // a leaf for which we did not even try to get the data
    Leaf,
    // not found that does not cause the traversal to abort
    NotFound,
    // not found that causes the traversal to abort
    StopNotFound,
}

impl<S: StoreRead> Iterator for DepthFirstTraversal<S> {
    type Item = anyhow::Result<(Cid, Extra)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next0().transpose()
    }
}

/// A breath first traversal of a dag, limited by depth and a set of already visited nodes.
///
/// Nodes will be reported as leafs if they are at the maximum depth, even though they might have links.
struct BreadthFirstTraversal<S> {
    store: S,
    visited: AHashSet<Cid>,
    current: (std::vec::IntoIter<Cid>, std::option::IntoIter<Cid>),
    next: (Vec<Cid>, Option<Cid>),
    depth: usize,
    left_to_right: bool,
}

impl<S: StoreRead> BreadthFirstTraversal<S> {
    fn new(store: S, root: Cid, depth: usize, left_to_right: bool, visited: AHashSet<Cid>) -> Self {
        Self {
            store,
            depth,
            left_to_right,
            visited,
            current: (vec![root].into_iter(), None.into_iter()),
            next: Default::default(),
        }
    }

    fn next0(&mut self) -> anyhow::Result<Option<(Cid, Extra)>> {
        Ok(loop {
            let cid = unwrap_or!(self.current.0.next(), {
                let (next, stop) = std::mem::take(&mut self.next);
                if let Some(cid) = self.current.1.next() {
                    break Some((cid, Extra::StopNotFound));
                } else if self.depth > 0 && (!next.is_empty() || stop.is_some()) {
                    self.current = (next.into_iter(), stop.into_iter());
                    self.depth -= 1;
                    continue;
                } else {
                    break None;
                }
            });
            // completely ignore visited cids, not even increase index
            if self.visited.contains(&cid) {
                continue;
            }
            self.visited.insert(cid);
            // we are at the max depth, so we don't need the links
            if self.depth == 0 {
                break Some((cid, Extra::Leaf));
            }
            // get the data for the cid, if we can't get it, abort
            if let Some((data, links)) = self.store.get(&cid)? {
                // push the links if there are any
                if !links.is_empty() && self.next.1.is_none() {
                    let mut links = links.to_vec();
                    if !self.left_to_right {
                        // reverse to get right to left traversal
                        links.reverse();
                    }
                    self.next.0.extend(links);
                }
                // return the cid and data
                break Some((cid, Extra::Branch(data)));
            } else {
                // ensure we stop adding links for the next level, since the order is now non deterministic
                if self.next.1.is_none() {
                    self.next.1 = Some(cid);
                }
                break Some((cid, Extra::NotFound));
            }
        })
    }
}

impl<S: StoreRead> Iterator for BreadthFirstTraversal<S> {
    type Item = anyhow::Result<(Cid, Extra)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next0().transpose()
    }
}

/// compact representation of a set of Cids
///
/// This could also be a probabilitic data structure like a bloom filter,
/// with a moderate false positive rate.
type CidSet = AHashSet<Cid>;

/// bitmap of blocks to get
type Bitmap = BitVec;

#[derive(Debug, Default, Clone, Copy)]
enum Traversal {
    /// depth-first traversal
    #[default]
    DepthFirst = 0,
    /// breadth-first traversal
    BreadthFirst = 1,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Direction {
    /// left to right
    #[default]
    LeftToRight = 0,
    // right to left
    RightToLeft = 1,
}

#[derive(Debug, Default, Clone)]
pub struct Query {
    root: Cid,

    /// cids to stop the traversal at (exclusive)
    stop: CidSet,

    /// depth of traversal
    depth: u64,

    /// traversal order
    traversal: Traversal,

    /// direction of traversal
    direction: Direction,

    /// bitmap of cids for which to send blocks
    bits: Bitmap,
}

impl Query {
    fn depth(self, depth: u64) -> Self {
        Self { depth, ..self }
    }

    fn bits(self, bits: Bitmap) -> Self {
        Self { bits, ..self }
    }

    fn stop(self, stop: AHashSet<Cid>) -> Self {
        Self { stop, ..self }
    }

    fn direction(self, direction: Direction) -> Self {
        Self { direction, ..self }
    }

    fn traversal(self, traversal: Traversal) -> Self {
        Self { traversal, ..self }
    }

    fn new(root: Cid) -> Self {
        Self {
            root,
            depth: u64::MAX,
            traversal: Traversal::DepthFirst,
            direction: Direction::LeftToRight,
            bits: BitVec::repeat(true, 1024),
            stop: AHashSet::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    /// codec to use to reconstruct the Cid
    codec: u64,
    /// hash algo to use to reconstruct the Cid
    hash: u64,
    /// data of the block
    data: Bytes,
}

impl Block {
    fn new(cid: Cid, data: Bytes) -> Self {
        Self {
            codec: cid.codec(),
            hash: cid.hash().code(),
            data,
        }
    }

    fn cid(&self) -> Cid {
        let code = multihash::Code::try_from(self.hash).unwrap();
        let hash = code.digest(&self.data);
        Cid::new_v1(self.codec, hash)
    }
}

/// Update of an ongoing request
pub enum WantRequestUpdate {
    /// Cancel values, e.g. if we got them already from another node
    Cancel(Bitmap),
    /// Request additional values, e.g. if another node has not delivered them
    Add(Bitmap),
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Want response
pub enum WantResponse {
    /// Got a block
    Block(usize, Block),
    /// Did not find a cid.
    NotFound(usize, Cid),
    /// Did not find a cid. Stream ends.
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
    fn internal_error(error: anyhow::Error) -> Self {
        Self::InternalError(error.to_string())
    }
}

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
#[allow(clippy::redundant_clone)]
mod tests {
    use super::*;
    use cid::Cid;
    use libipld::{cbor::DagCborCodec, ipld, prelude::Codec, Ipld};
    use multihash::MultihashDigest;

    fn parse_bits(bits: &str) -> anyhow::Result<BitVec> {
        let bits = bits
            .chars()
            .map(|c| match c {
                '0' => Ok(false),
                '1' => Ok(true),
                _ => Err(anyhow::anyhow!("invalid bit")),
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(bits)
    }

    trait StoreWriteExt: StoreWrite {
        fn write_raw(&self, data: &[u8]) -> anyhow::Result<Cid> {
            let hash = multihash::Code::Sha2_256.digest(&data);
            let cid = Cid::new_v1(0x55, hash);
            self.put(&cid, data, &[])?;
            Ok(cid)
        }

        fn write_ipld(&self, ipld: Ipld) -> anyhow::Result<Cid> {
            let bytes = DagCborCodec.encode(&ipld).unwrap();
            let hash = multihash::Code::Sha2_256.digest(&bytes);
            let cid = Cid::new_v1(0x55, hash);
            let mut links = Vec::new();
            ipld.references(&mut links);
            self.put(&cid, &bytes, &links)?;
            Ok(cid)
        }
    }

    impl<S: StoreWrite> StoreWriteExt for S {}

    /// basically like a want response, but cid instead of block
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum WantResponseShort {
        Block(Cid),
        NotFound(Cid),
        StopNotFound(Cid),
        MaxBlocks,
        MaxBytes,
        InternalError(String),
    }

    fn short(response: WantResponse) -> WantResponseShort {
        match response {
            WantResponse::Block(_, block) => WantResponseShort::Block(block.cid()),
            WantResponse::NotFound(_, cid) => WantResponseShort::NotFound(cid),
            WantResponse::StopNotFound(_, cid) => WantResponseShort::StopNotFound(cid),
            WantResponse::MaxBlocksRead => WantResponseShort::MaxBlocks,
            WantResponse::MaxBytesRead => WantResponseShort::MaxBytes,
            WantResponse::MaxBlocksSent => WantResponseShort::MaxBlocks,
            WantResponse::MaxBytesSent => WantResponseShort::MaxBytes,
            WantResponse::InternalError(x) => WantResponseShort::InternalError(x),
        }
    }

    #[test]
    #[rustfmt::skip]
    fn smoke() -> anyhow::Result<()> {
        use Direction::*;
        use Traversal::*;
        let store = Store::default();
        use WantResponseShort::*;

        // small helper to reduce test boilerplate
        let have = |q: &Query| store.have(q.clone());
        let want = |q: &Query| store.clone().want(q.clone()).map(short).collect::<Vec<_>>();

        let a = store.write_raw(b"abcd")?;
        let b = store.write_raw(b"efgh")?;
        let c = store.write_raw(b"ijkl")?;
        let d = store.write_raw(b"mnop")?;
        let b0 = store.write_ipld(ipld! {{
            "aaaaaaaa": a,
            "bbbbbbbb": b,
        }})?;
        let b1 = store.write_ipld(ipld! {{
            "cccccccc": c,
            "dddddddd": d,
        }})?;
        let r = store.write_ipld(ipld! {{
            "b0": b0,
            "b1": b1,
        }})?;
    
        // create all the different queries
        let df = Query::new(r).traversal(DepthFirst);
        let dflr = df.clone().direction(LeftToRight);
        let dfrl = df.clone().direction(RightToLeft);
        let bf = Query::new(r).traversal(BreadthFirst);
        let bflr = bf.clone().direction(LeftToRight);
        let bfrl = bf.clone().direction(RightToLeft);

        let dflr = dflr.depth(0);
        let dfrl = dfrl.depth(0);
        let bflr = bflr.depth(0);
        let bfrl = bfrl.depth(0);

        assert_eq!(have(&dflr)?, parse_bits("1")?);
        assert_eq!(have(&dfrl)?, parse_bits("1")?);
        assert_eq!(have(&bflr)?, parse_bits("1")?);
        assert_eq!(have(&bfrl)?, parse_bits("1")?);

        assert_eq!(want(&dflr), vec![Block(r)]);
        assert_eq!(want(&dfrl), vec![Block(r)]);
        assert_eq!(want(&bflr), vec![Block(r)]);
        assert_eq!(want(&bfrl), vec![Block(r)]);

        let dflr = dflr.depth(1);
        let dfrl = dfrl.depth(1);
        let bflr = bflr.depth(1);
        let bfrl = bfrl.depth(1);

        assert_eq!(have(&dflr)?, parse_bits("111")?);
        assert_eq!(have(&dfrl)?, parse_bits("111")?);
        assert_eq!(have(&bflr)?, parse_bits("111")?);
        assert_eq!(have(&bfrl)?, parse_bits("111")?);

        assert_eq!(want(&dflr), vec![Block(r), Block(b0), Block(b1)]);
        assert_eq!(want(&dfrl), vec![Block(r), Block(b1), Block(b0)]);
        assert_eq!(want(&bflr), vec![Block(r), Block(b0), Block(b1)]);
        assert_eq!(want(&bfrl), vec![Block(r), Block(b1), Block(b0)]);

        let dflr = dflr.depth(2);
        let dfrl = dfrl.depth(2);
        let bflr = bflr.depth(2);
        let bfrl = bfrl.depth(2);

        assert_eq!(have(&dflr)?, parse_bits("1111111")?);
        assert_eq!(have(&dfrl)?, parse_bits("1111111")?);
        assert_eq!(have(&bflr)?, parse_bits("1111111")?);
        assert_eq!(have(&bfrl)?, parse_bits("1111111")?);

        assert_eq!(want(&dflr), vec![Block(r), Block(b0), Block(a), Block(b), Block(b1), Block(c), Block(d)]);
        assert_eq!(want(&dfrl), vec![Block(r), Block(b1), Block(d), Block(c), Block(b0), Block(b), Block(a)]);
        assert_eq!(want(&bflr), vec![Block(r), Block(b0), Block(b1), Block(a), Block(b), Block(c), Block(d)]);
        assert_eq!(want(&bfrl), vec![Block(r), Block(b1), Block(b0), Block(d), Block(c), Block(b), Block(a)]);

        store.delete(&d)?;
        assert_eq!(have(&dflr)?, parse_bits("1111110")?);
        assert_eq!(have(&dfrl)?, parse_bits("1101111")?);
        assert_eq!(have(&bflr)?, parse_bits("1111110")?);
        assert_eq!(have(&bfrl)?, parse_bits("1110111")?);

        assert_eq!(want(&dflr), vec![Block(r), Block(b0), Block(a), Block(b), Block(b1), Block(c), NotFound(d)]);
        assert_eq!(want(&dfrl), vec![Block(r), Block(b1), NotFound(d), Block(c), Block(b0), Block(b), Block(a)]);
        assert_eq!(want(&bflr), vec![Block(r), Block(b0), Block(b1), Block(a), Block(b), Block(c), NotFound(d)]);
        assert_eq!(want(&bfrl), vec![Block(r), Block(b1), Block(b0), NotFound(d), Block(c), Block(b), Block(a)]);

        store.delete(&b)?;
        assert_eq!(have(&dflr)?, parse_bits("1110110")?);
        assert_eq!(have(&dfrl)?, parse_bits("1101101")?);
        assert_eq!(have(&bflr)?, parse_bits("1111010")?);
        assert_eq!(have(&bfrl)?, parse_bits("1110101")?);

        assert_eq!(want(&dflr), vec![Block(r), Block(b0), Block(a), NotFound(b), Block(b1), Block(c), NotFound(d)]);
        assert_eq!(want(&dfrl), vec![Block(r), Block(b1), NotFound(d), Block(c), Block(b0), NotFound(b), Block(a)]);
        assert_eq!(want(&bflr), vec![Block(r), Block(b0), Block(b1), Block(a), NotFound(b), Block(c), NotFound(d)]);
        assert_eq!(want(&bfrl), vec![Block(r), Block(b1), Block(b0), NotFound(d), Block(c), NotFound(b), Block(a)]);

        store.delete(&c)?;
        assert_eq!(have(&dflr)?, parse_bits("1110100")?);
        assert_eq!(have(&dfrl)?, parse_bits("1100101")?);
        assert_eq!(have(&bflr)?, parse_bits("1111000")?);
        assert_eq!(have(&bfrl)?, parse_bits("1110001")?);

        assert_eq!(want(&dflr), vec![Block(r), Block(b0), Block(a), NotFound(b), Block(b1), NotFound(c), NotFound(d)]);
        assert_eq!(want(&dfrl), vec![Block(r), Block(b1), NotFound(d), NotFound(c), Block(b0), NotFound(b), Block(a)]);
        assert_eq!(want(&bflr), vec![Block(r), Block(b0), Block(b1), Block(a), NotFound(b), NotFound(c), NotFound(d)]);
        assert_eq!(want(&bfrl), vec![Block(r), Block(b1), Block(b0), NotFound(d), NotFound(c), NotFound(b), Block(a)]);

        store.delete(&a)?;
        assert_eq!(have(&dflr)?, parse_bits("1100100")?);
        assert_eq!(have(&dfrl)?, parse_bits("1100100")?);
        assert_eq!(have(&bflr)?, parse_bits("1110000")?);
        assert_eq!(have(&bfrl)?, parse_bits("1110000")?);

        assert_eq!(want(&dflr), vec![Block(r), Block(b0), NotFound(a), NotFound(b), Block(b1), NotFound(c), NotFound(d)]);
        assert_eq!(want(&dfrl), vec![Block(r), Block(b1), NotFound(d), NotFound(c), Block(b0), NotFound(b), NotFound(a)]);
        assert_eq!(want(&bflr), vec![Block(r), Block(b0), Block(b1), NotFound(a), NotFound(b), NotFound(c), NotFound(d)]);
        assert_eq!(want(&bfrl), vec![Block(r), Block(b1), Block(b0), NotFound(d), NotFound(c), NotFound(b), NotFound(a)]);

        store.delete(&b1)?;
        // b1 is gone, so with a left to right traversal, we have to give up after b0's children
        assert_eq!(have(&dflr)?, parse_bits("1100")?);
        // b1 is gone, so with a right to left traversal, we have to give up early
        assert_eq!(have(&dfrl)?, parse_bits("1")?);
        // r and b0 are there. b1 is gone. But we can still enumerate the children of b0 before breaking order
        assert_eq!(have(&bflr)?, parse_bits("11000")?);
        // r and b0 are there. b1 is gone. Because of rtl we can't enumerate the children of b0 before breaking order
        assert_eq!(have(&bfrl)?, parse_bits("101")?);

        assert_eq!(want(&dflr), vec![Block(r), Block(b0), NotFound(a), NotFound(b), StopNotFound(b1)]);
        assert_eq!(want(&dfrl), vec![Block(r), StopNotFound(b1)]);
        assert_eq!(want(&bflr), vec![Block(r), Block(b0), NotFound(b1), NotFound(a), NotFound(b), StopNotFound(b1)]);
        assert_eq!(want(&bfrl), vec![Block(r), NotFound(b1), Block(b0), StopNotFound(b1)]);
        Ok(())
    }
}
