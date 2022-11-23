#![allow(clippy::type_complexity)]
use std::{fmt::Debug, net::SocketAddr, sync::Arc};

use ahash::{AHashMap, AHashSet};
use async_stream::try_stream;
use bitvec::vec::BitVec;
use bytes::Bytes;
use cid::Cid;
use clap::Parser;
use futures::{future::BoxFuture, stream::BoxStream, FutureExt, StreamExt};
use libipld::{cbor::DagCborCodec, prelude::Codec, Ipld};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::proto::*;
use crate::util::limit;

macro_rules! unwrap_or {
    ($e:expr, $n:expr) => {
        match $e {
            Some(v) => v,
            None => $n,
        }
    };
}

pub trait StoreRead {
    fn has(&self, cid: &Cid) -> anyhow::Result<bool>;
    fn get(&self, cid: &Cid) -> anyhow::Result<Option<(Bytes, Arc<[Cid]>)>>;
}

pub trait StoreWrite {
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

pub(crate) trait StoreReadExt: StoreRead + Clone + Sized + Send + 'static {
    fn have(&self, query: Query) -> anyhow::Result<HaveResponse> {
        match query.traversal {
            Traversal::DepthFirst => make_have(
                self.clone(),
                DepthFirstTraversal::new(
                    self.clone(),
                    query.root,
                    query.depth as usize,
                    query.direction == Direction::LeftToRight,
                    query.stop,
                ),
            ),
            Traversal::BreadthFirst => make_have(
                self.clone(),
                BreadthFirstTraversal::new(
                    self.clone(),
                    query.root,
                    query.depth as usize,
                    query.direction == Direction::LeftToRight,
                    query.stop,
                ),
            ),
        }
    }

    fn want(self, query: Query) -> Box<dyn Iterator<Item = WantResponse> + Send> {
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

impl<S: StoreRead + Clone + Sized + Send + 'static> StoreReadExt for S {}

fn make_have(
    store: impl StoreRead,
    iter: impl Iterator<Item = anyhow::Result<(Cid, Extra)>>,
) -> anyhow::Result<HaveResponse> {
    // todo: we could in theory do all has queries in one operation
    let mut stop: Option<Cid> = None;
    let bitmap = iter
        .take_while(|x| match x {
            Ok((cid, Extra::StopNotFound)) => {
                stop = Some(*cid);
                false
            }
            _ => true,
        })
        .map(move |item| {
            let (cid, data) = item?;
            Ok(match data {
                Extra::Branch(_) => true,
                Extra::NotFound => false,
                Extra::Leaf => store.has(&cid)?,
                Extra::StopNotFound => unreachable!(),
            })
        })
        .collect::<anyhow::Result<BitVec>>()?;
    Ok(HaveResponse { bitmap, stop })
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
            send_blocks: 10000,
            send_bytes: 1000 * 1024 * 1024,
            read_blocks: 10000,
            read_bytes: 1000 * 1024 * 1024,
        }
    }
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

trait SyncApi: Debug + Send + Sync + 'static {
    fn have(&self, query: Query) -> BoxFuture<anyhow::Result<HaveResponse>>;
    fn want(
        &self,
        query: Query,
        changes: BoxStream<WantRequestUpdate>,
    ) -> BoxFuture<anyhow::Result<BoxStream<WantResponse>>>;
}

impl SyncApi for Box<dyn SyncApi> {
    fn have(&self, query: Query) -> BoxFuture<anyhow::Result<HaveResponse>> {
        (**self).have(query)
    }

    fn want(
        &self,
        query: Query,
        changes: BoxStream<WantRequestUpdate>,
    ) -> BoxFuture<anyhow::Result<BoxStream<WantResponse>>> {
        (**self).want(query, changes)
    }
}

#[derive(Debug)]
struct Node {
    peers: AHashMap<u64, Box<dyn SyncApi>>,
    store: Store,
}

impl Node {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            peers: Default::default(),
        }
    }
}

impl SyncApi for Node {
    fn have(&self, query: Query) -> BoxFuture<anyhow::Result<HaveResponse>> {
        let store = self.store.clone();
        async move { store.have(query) }.boxed()
    }

    fn want(
        &self,
        query: Query,
        changes: BoxStream<WantRequestUpdate>,
    ) -> BoxFuture<anyhow::Result<BoxStream<WantResponse>>> {
        let res = futures::stream::iter(self.store.clone().want(query));
        futures::future::ok(res.boxed()).boxed()
    }
}

impl Node {
    fn add_peer(&mut self, id: u64, peer: Box<dyn SyncApi>) {
        self.peers.insert(id, peer);
    }

    fn sync(&self, query: Query) -> BoxFuture<anyhow::Result<BoxStream<anyhow::Result<()>>>> {
        println!("syncing {}", query.root);
        let s = try_stream! {
            loop {
                let peers = self.peers.values().collect::<Vec<_>>();
                let mut mine = self.store.have(query.clone())?;
                if mine.is_complete() {
                    break;
                }
                if let Some(peer) = peers.first() {
                    let mut query = query.clone();
                    query.bits = !mine.bitmap.clone();
                    query.bits.extend((0..1024).map(|_| true));
                    let mut stream = peer.want(query, futures::stream::empty().boxed()).await?;
                    while let Some(response) = stream.next().await {
                        match response {
                            WantResponse::Block(index, block) => {
                                let cid = block.cid();
                                let mut links = Vec::new();
                                DagCborCodec.references::<Ipld, _>(&block.data, &mut links)?;
                                println!("got block {} {} {}", index, cid, links.len());
                                self.store.put(cid, block.data, links.into())?;
                                if index < mine.bitmap.len() {
                                    mine.bitmap.set(index, true);
                                }
                            }
                            WantResponse::NotFound(index, cid) => {
                            }
                            WantResponse::StopNotFound(index, cid) => {
                            }
                            WantResponse::MaxBlocksSent => {
                            }
                            WantResponse::MaxBytesSent => {
                            }
                            WantResponse::MaxBlocksRead => {
                            }
                            WantResponse::MaxBytesRead => {
                            }
                            WantResponse::InternalError(error) => {
                            }
                        }
                    }
                }
                yield ();
            }
        }
        .boxed();
        futures::future::ready(anyhow::Ok(s)).boxed()
    }
}

#[derive(Parser, Debug)]
#[clap(name = "kraken")]
pub struct Args {
    #[arg(short, long, help = "port to listen on")]
    pub(crate) port: Option<u16>,

    #[arg(short, long, help = "addresses to connect to")]
    pub(crate) connect: Vec<SocketAddr>,

    #[arg(long, help = "data sets to create")]
    pub(crate) create: Option<Vec<String>>,

    #[arg(long, help = "roots to sync")]
    #[arg(long, value_parser = clap::value_parser!(Cid))]
    pub(crate) sync: Option<Vec<Cid>>,
}
