#![allow(clippy::type_complexity)]
use ahash::{AHashMap, AHashSet};
use bitvec::vec::BitVec;
use bytes::Bytes;
use cid::Cid;
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
    iter.take_while(|x| !matches!(x, Ok((_, Extra::NotFound))))
        .map(move |item| {
            let (cid, data) = item?;
            Ok(match data {
                Extra::Branch(_) => true,
                Extra::Leaf => store.has(&cid)?,
                Extra::NotFound => unreachable!(),
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
            return Err(WantResponse::MaxBlocks);
        } else {
            remaining_read_blocks -= 1;
        }
        let data_len = data.len() as u64;
        if remaining_read_bytes < data_len {
            Err(WantResponse::MaxBytes)
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
                return Err(WantResponse::MaxBlocks);
            } else {
                remaining_send_blocks -= 1;
            }
            let data_len = data.len() as u64;
            if remaining_send_bytes < data_len {
                return Err(WantResponse::MaxBytes);
            } else {
                remaining_send_bytes -= data_len;
            }
            Ok(Some(WantResponse::Block(Block::new(index, cid, data))))
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
                    Err(WantResponse::NotFound(cid))
                }
            }
            Extra::Leaf => {
                // skipping a leaf does not require any bookkeeping
                Ok(None)
            }
            Extra::NotFound => {
                // terminate the iteration with a NotFound
                Err(WantResponse::NotFound(cid))
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
                break Some((cid, Extra::NotFound));
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
    Branch(Bytes),
    Leaf,
    NotFound,
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
    current: std::vec::IntoIter<Cid>,
    next: Vec<Cid>,
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
            current: vec![root].into_iter(),
            next: Vec::new(),
        }
    }

    fn next0(&mut self) -> anyhow::Result<Option<(Cid, Extra)>> {
        Ok(loop {
            let cid = unwrap_or!(self.current.next(), {
                let t = std::mem::take(&mut self.next);
                if self.depth > 0 && !t.is_empty() {
                    self.current = t.into_iter();
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
            let (data, links) = unwrap_or!(self.store.get(&cid)?, {
                break Some((cid, Extra::NotFound));
            });
            // push the links if there are any
            if !links.is_empty() {
                let mut links = links.to_vec();
                if !self.left_to_right {
                    links.reverse();
                }
                self.next.extend(links);
            }
            // return the cid and data
            break Some((cid, Extra::Branch(data)));
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
    /// index of the block in the traversal
    ///
    /// We could omit this, since the nth block corresponds to the nth 1 bit in the bitmap.
    /// But that gets complicated if you have block updates.
    index: usize,
    /// codec to use to reconstruct the Cid
    codec: u64,
    /// hash algo to use to reconstruct the Cid
    hash: u64,
    /// data of the block
    data: Bytes,
}

impl Block {
    fn new(index: usize, cid: Cid, data: Bytes) -> Self {
        Self {
            index,
            codec: cid.codec(),
            hash: cid.hash().code(),
            data,
        }
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
    Block(Block),
    /// Did not find a cid. Stream ends.
    NotFound(Cid),
    /// Max blocks exceeded. Stream ends.
    MaxBlocks,
    /// Max bytes exceeded. Stream ends.
    MaxBytes,
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

    #[test]
    fn smoke() -> anyhow::Result<()> {
        use Direction::*;
        use Traversal::*;
        let store = Store::default();
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
        let t = store.have(Query::new(r).depth(0))?;
        assert_eq!(t, parse_bits("1")?);
        let t = store.have(Query::new(r).depth(1))?;
        assert_eq!(t, parse_bits("111")?);
        let t = store.have(Query::new(r).depth(2))?;
        assert_eq!(t, parse_bits("1111111")?);

        let responses = store
            .clone()
            .want(Query::new(r).depth(2).traversal(DepthFirst))
            .collect::<Vec<_>>();
        println!("{:#?}\n\n", responses);

        let responses = store
            .clone()
            .want(Query::new(r).depth(2).traversal(BreadthFirst))
            .collect::<Vec<_>>();
        println!("{:#?}\n\n", responses);

        let df = Query::new(r).traversal(DepthFirst);
        let dflr = df.clone().direction(LeftToRight);
        let dfrl = df.clone().direction(RightToLeft);
        let bf = Query::new(r).traversal(BreadthFirst);
        let bflr = bf.clone().direction(LeftToRight);
        let bfrl = bf.clone().direction(RightToLeft);

        let dflr = dflr.depth(1);
        let dfrl = dfrl.depth(1);
        let bflr = bflr.depth(1);
        let bfrl = bfrl.depth(1);

        assert_eq!(store.have(dflr.clone())?, parse_bits("111")?);
        assert_eq!(store.have(dfrl.clone())?, parse_bits("111")?);
        assert_eq!(store.have(bflr.clone())?, parse_bits("111")?);
        assert_eq!(store.have(bfrl.clone())?, parse_bits("1111111")?);

        let dflr = dflr.depth(2);
        let dfrl = dfrl.depth(2);
        let bflr = bflr.depth(2);
        let bfrl = bfrl.depth(2);

        assert_eq!(store.have(dflr.clone())?, parse_bits("1111111")?);
        assert_eq!(store.have(dfrl.clone())?, parse_bits("1111111")?);
        assert_eq!(store.have(bflr.clone())?, parse_bits("1111111")?);
        assert_eq!(store.have(bfrl.clone())?, parse_bits("1111111")?);

        store.delete(&d)?;
        assert_eq!(store.have(dflr.clone())?, parse_bits("1111110")?);
        assert_eq!(store.have(dfrl.clone())?, parse_bits("1101111")?);
        assert_eq!(store.have(bflr.clone())?, parse_bits("1111110")?);
        assert_eq!(store.have(bfrl.clone())?, parse_bits("1110111")?);

        store.delete(&b)?;
        assert_eq!(store.have(dflr.clone())?, parse_bits("1110110")?);
        assert_eq!(store.have(dfrl.clone())?, parse_bits("1101101")?);
        assert_eq!(store.have(bflr.clone())?, parse_bits("1111010")?);
        assert_eq!(store.have(bfrl.clone())?, parse_bits("1110101")?);

        store.delete(&c)?;
        assert_eq!(store.have(dflr.clone())?, parse_bits("1110100")?);
        assert_eq!(store.have(dfrl.clone())?, parse_bits("1100101")?);
        assert_eq!(store.have(bflr.clone())?, parse_bits("1111000")?);
        assert_eq!(store.have(bfrl.clone())?, parse_bits("1110001")?);

        store.delete(&a)?;
        assert_eq!(store.have(dflr.clone())?, parse_bits("1100100")?);
        assert_eq!(store.have(dfrl.clone())?, parse_bits("1100100")?);
        assert_eq!(store.have(bflr.clone())?, parse_bits("1110000")?);
        assert_eq!(store.have(bfrl.clone())?, parse_bits("1110000")?);

        store.delete(&b1)?;
        assert_eq!(store.have(dflr.clone())?, parse_bits("1100")?);
        assert_eq!(store.have(dfrl.clone())?, parse_bits("1")?);
        assert_eq!(store.have(bflr.clone())?, parse_bits("11")?);
        assert_eq!(store.have(bfrl.clone())?, parse_bits("1")?);
        Ok(())
    }
}
