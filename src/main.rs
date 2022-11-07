use ahash::{AHashMap, AHashSet};
use cid::Cid;
use parking_lot::Mutex;
use std::{
    collections::{BTreeSet, VecDeque},
    iter::FusedIterator,
    marker::PhantomData,
    sync::Arc,
};

#[derive(Debug, Clone)]
struct Store {
    blocks: Arc<Mutex<AHashMap<Cid, (Arc<[u8]>, Arc<[Cid]>)>>>,
}

impl Store {
    fn get(&self, cid: &Cid) -> anyhow::Result<Option<(Arc<[u8]>, Arc<[Cid]>)>> {
        Ok(self.blocks.lock().get(cid).cloned())
    }

    fn has(&self, cid: &Cid) -> anyhow::Result<bool> {
        Ok(self.blocks.lock().get(cid).is_some())
    }

    fn have(self, query: Query) -> anyhow::Result<Vec<bool>> {
        match query.traversal {
            Traversal::DepthFirst => self
                .clone()
                .make_bitmap(DepthFirstTraversal2::new(
                    self,
                    query.root,
                    query.depth as usize,
                    query.direction == Direction::LeftToRight,
                    query.stop,
                ))
                .collect(),
            Traversal::BreadthFirst => self
                .clone()
                .make_bitmap(BreadthFirstTraversal2::new(
                    self,
                    query.root,
                    query.depth as usize,
                    query.direction == Direction::LeftToRight,
                    query.stop,
                ))
                .collect(),
        }
    }

    fn want(self, query: Query) -> Box<dyn Iterator<Item = Response>> {
        let bits = query.bits.to_vec().into_iter();
        match query.traversal {
            Traversal::DepthFirst => Box::new(self.clone().make_want(
                DepthFirstTraversal2::new(
                    self,
                    query.root,
                    query.depth as usize,
                    query.direction == Direction::LeftToRight,
                    query.stop,
                ),
                bits,
                Default::default(),
            )),
            Traversal::BreadthFirst => Box::new(self.clone().make_want(
                BreadthFirstTraversal2::new(
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

    fn make_bitmap(
        self,
        iter: impl Iterator<Item = anyhow::Result<(Cid, Extra)>> + 'static,
    ) -> impl Iterator<Item = anyhow::Result<bool>> + 'static {
        iter.take_while(|x| !matches!(x, Ok((_, Extra::NotFound))))
            .map(move |item| {
                let (cid, data) = item?;
                Ok(match data {
                    Extra::Branch(_) => true,
                    Extra::Leaf => self.has(&cid)?,
                    Extra::NotFound => unreachable!(),
                })
            })
    }

    fn make_want(
        self,
        iter: impl Iterator<Item = anyhow::Result<(Cid, Extra)>> + 'static,
        bitmap: impl Iterator<Item = bool> + 'static,
        limits: Limits,
    ) -> impl Iterator<Item = Response> + 'static {
        let mut remaining_send_blocks = limits.send_blocks;
        let mut remaining_send_bytes = limits.send_bytes;
        let mut remaining_read_blocks = limits.read_blocks;
        let mut remaining_read_bytes = limits.read_bytes;
        let mut track_read_limits = move |data: &[u8]| -> std::result::Result<(), Response> {
            if remaining_read_blocks == 0 {
                return Err(Response::MaxBlocks);
            } else {
                remaining_read_blocks -= 1;
            }
            let data_len = data.len() as u64;
            if remaining_read_bytes < data_len {
                Err(Response::MaxBytes)
            } else {
                remaining_read_bytes -= data_len;
                Ok(())
            }
        };
        // send a block, applying and decrementing the limits
        let mut send_block_limited =
            move |index: usize,
                  cid: Cid,
                  data: Arc<[u8]>|
                  -> std::result::Result<Option<Response>, Response> {
                if remaining_send_blocks == 0 {
                    return Err(Response::MaxBlocks);
                } else {
                    remaining_send_blocks -= 1;
                }
                let data_len = data.len() as u64;
                if remaining_send_bytes < data_len {
                    return Err(Response::MaxBytes);
                } else {
                    remaining_send_bytes -= data_len;
                }
                Ok(Some(Response::Block(Block::new(index, cid, data))))
            };
        // zip all that is needed, and flatten it for convenience
        let flat = iter.enumerate().zip(bitmap).map(|((index, item), take)| {
            let (cid, data) = item?;
            anyhow::Ok((index, cid, data, take))
        });
        // iterate over the result, applying limits and skipping 0 bits in the bitmap
        limit(flat, move |item| {
            // terminate on internal error
            let (index, cid, data, take) = item.map_err(Response::internal_error)?;
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
                    if let Some((data, _)) = self.get(&cid).map_err(Response::internal_error)? {
                        // we have to keep track of how much we read from the store
                        track_read_limits(&data)?;
                        send_block_limited(index, cid, data)
                    } else {
                        Err(Response::NotFound(cid))
                    }
                }
                Extra::Leaf => {
                    // skipping a leaf does not require any bookkeeping
                    Ok(None)
                }
                Extra::NotFound => {
                    // terminate the iteration with a NotFound
                    Err(Response::NotFound(cid))
                }
            }
        })
    }
}

struct Limits {
    /// maximum number of blocks to send
    send_blocks: u64,
    /// maximum number of bytes to send
    send_bytes: u64,
    /// maximum number of blocks to read from the store
    read_blocks: u64,
    /// maximum number of bytes to read from the store
    read_bytes: u64,
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

struct DepthFirstTraversal2 {
    store: Store,
    visited: AHashSet<Cid>,
    stack: Vec<Vec<Cid>>,
    max_depth: usize,
    left_to_right: bool,
}

impl DepthFirstTraversal2 {
    fn new(
        store: Store,
        root: Cid,
        depth: usize,
        left_to_right: bool,
        visited: AHashSet<Cid>,
    ) -> Self {
        Self {
            store,
            visited,
            stack: vec![vec![root]],
            max_depth: depth,
            left_to_right,
        }
    }

    fn next0(&mut self) -> anyhow::Result<Option<(Cid, Extra)>> {
        Ok(loop {
            // end the stream normally if there is nothing left to do
            let stack = unwrap_or!(self.stack.last_mut(), break None);
            // get the next cid from the stack for this level, or go up one level
            let cid = unwrap_or!(stack.pop(), {
                self.stack.pop();
                continue;
            });
            // completely ignore visited cids, not even increase index
            if self.visited.contains(&cid) {
                continue;
            }
            self.visited.insert(cid);
            // we are at the max depth, so we don't need the links
            if self.stack.len() == self.max_depth {
                break Some((cid, Extra::Leaf));
            }
            // get the data for the cid, if we can't get it, abort
            let (data, links) = unwrap_or!(self.store.get(&cid)?, {
                break Some((cid, Extra::NotFound));
            });
            // push the links if there are any
            if !links.is_empty() {
                let mut links = links.to_vec();
                if self.left_to_right {
                    // reverse to get left to right traversal
                    links.reverse();
                }
                self.stack.push(links);
            }
            // return the cid and data
            break Some((cid, Extra::Branch(data)));
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Extra {
    Branch(Arc<[u8]>),
    Leaf,
    NotFound,
}

impl Iterator for DepthFirstTraversal2 {
    type Item = anyhow::Result<(Cid, Extra)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next0().transpose()
    }
}

struct BreadthFirstTraversal2 {
    store: Store,
    visited: AHashSet<Cid>,
    current: Vec<Cid>,
    next: Vec<Cid>,
    depth: usize,
    left_to_right: bool,
}

impl BreadthFirstTraversal2 {
    fn new(
        store: Store,
        root: Cid,
        depth: usize,
        left_to_right: bool,
        visited: AHashSet<Cid>,
    ) -> Self {
        Self {
            store,
            depth,
            left_to_right,
            visited,
            current: vec![root],
            next: Vec::new(),
        }
    }

    fn next0(&mut self) -> anyhow::Result<Option<(Cid, Extra)>> {
        Ok(loop {
            let cid = unwrap_or!(self.current.pop(), {
                if self.depth > 0 {
                    self.current = std::mem::take(&mut self.next);
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
                if self.left_to_right {
                    // reverse to get left to right traversal
                    links.reverse();
                }
                self.next.extend(links);
            }
            // return the cid and data
            break Some((cid, Extra::Branch(data)));
        })
    }
}

impl Iterator for BreadthFirstTraversal2 {
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
type Bitmap = Arc<[bool]>;

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

struct Query {
    root: Cid,

    /// cids to stop the traversal at (exclusive)
    stop: CidSet,

    /// traversal order
    traversal: Traversal,

    /// direction of traversal
    direction: Direction,

    /// depth of traversal
    depth: u64,

    /// bitmap of cids for which to send blocks
    bits: Bitmap,

    /// maximum number of bytes to send
    ///
    /// will be min-combined with the server limit
    ///
    /// do we want the client to be able to specify this?
    max_bytes: u64,
}

struct Block {
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
    data: Arc<[u8]>,
}

impl Block {
    fn new(index: usize, cid: Cid, data: Arc<[u8]>) -> Self {
        Self {
            index,
            codec: cid.codec(),
            hash: cid.hash().code(),
            data,
        }
    }
}

struct BitmapUpdate {
    bits: Bitmap,
}

enum Response {
    Block(Block),
    NotFound(Cid),
    MaxBlocks,
    MaxBytes,
    InternalError(String),
}

impl Response {
    fn internal_error(error: anyhow::Error) -> Self {
        Self::InternalError(error.to_string())
    }
}

fn main() {
    println!("Hello, world!");
}
