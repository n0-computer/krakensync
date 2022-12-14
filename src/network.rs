use ahash::HashMapExt;
use anyhow::{bail, Context, Ok};
use async_stream::try_stream;
use cid::Cid;
use futures::{
    stream::{self, BoxStream},
    Sink, SinkExt, Stream, StreamExt, TryFutureExt,
};
use libipld::{cbor::DagCborCodec, prelude::Codec, Ipld, IpldCodec};
use quic_rpc::{
    client::BidiItemError,
    transport::{http2::ServerChannel, Http2ChannelTypes},
    RpcClient, RpcServer,
};
use std::{
    collections::BTreeMap,
    io::{self, stdout, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::Path,
    result,
    sync::Arc,
    time::Instant,
};
use tokio::task::JoinHandle;

use crate::{
    core::{Args, Store, StoreReadExt},
    proto::{KrakenService, Query, Request, Want, WantResponse, WantUpdate},
    test_util::make_tree,
};

#[derive(Debug, Clone)]
pub struct RpcStore(Store);

impl RpcStore {
    fn handle_want(
        self,
        req: Want,
        mut recv: impl Stream<Item = WantUpdate> + Send + Sync + Unpin + 'static,
    ) -> impl Stream<Item = WantResponse> {
        tracing::info!("handling want");
        tokio::spawn(async move {
            while let Some(recv) = recv.next().await {
                tracing::info!("got update: {:?}", recv);
            }
            tracing::info!("no more want updates!");
            Ok(())
        });
        stream::iter(self.0.want(req.query))
            .map(|item| {
                if let WantResponse::Block(o, b) = &item {
                    tracing::info!("sending block: {} {}", o, b.cid());
                } else {
                    tracing::info!("sending item: {:?}", item);
                }
                item
            })
            .filter(|_| async move {
                // this is so we get backpressure
                tokio::task::yield_now().await;
                true
            })
    }
}

pub struct KrakenServer2 {
    hyper_handle: JoinHandle<anyhow::Result<()>>,
    handler_handle: JoinHandle<anyhow::Result<()>>,
}

impl Drop for KrakenServer2 {
    fn drop(&mut self) {
        self.hyper_handle.abort();
        self.handler_handle.abort();
    }
}

impl KrakenServer2 {
    pub fn new(addr: SocketAddr, store: Store) -> anyhow::Result<Self> {
        let (channel, hyper) = ServerChannel::new(&addr)?;
        let hyper_handle = tokio::spawn(hyper.map_err(|x| anyhow::anyhow!(x)));
        let server = RpcServer::new(channel);
        let handler_handle = tokio::task::spawn(Self::run(server, store));
        Ok(Self {
            hyper_handle,
            handler_handle,
        })
    }

    async fn run(
        server: RpcServer<KrakenService, Http2ChannelTypes>,
        store: Store,
    ) -> anyhow::Result<()> {
        loop {
            let (req, c) = server.accept_one().await?;
            let target = RpcStore(store.clone());
            let s = server.clone();
            let x = match req {
                Request::Want(req) => {
                    s.bidi_streaming(req, c, target, RpcStore::handle_want)
                        .await
                }
                Request::WantUpdate(_) => {
                    bail!("unexpected request");
                }
                Request::Have(_) => {
                    bail!("not implemented");
                }
            };
            if let Err(cause) = x {
                tracing::error!("error handling request: {}", cause);
            }
        }
        Ok(())
    }
}

struct KrakenClient2 {
    client: RpcClient<KrakenService, Http2ChannelTypes>,
}

impl KrakenClient2 {
    pub fn new(uri: &str) -> anyhow::Result<Self> {
        let uri = uri.parse()?;
        let channel = quic_rpc::transport::http2::ClientChannel::new(uri);
        let client = RpcClient::new(channel);
        Ok(Self { client })
    }

    pub async fn want(
        &self,
        query: Query,
    ) -> anyhow::Result<(
        impl Sink<WantUpdate>,
        impl Stream<Item = result::Result<WantResponse, BidiItemError<Http2ChannelTypes>>>,
    )> {
        let (updates, responses) = self.client.bidi(Want { query }).await?;
        Ok((updates, responses))
    }
}

pub struct Node {
    port: u16,
    store: Store,
    peers: BTreeMap<SocketAddr, KrakenClient2>,
    server: KrakenServer2,
}

impl Node {
    pub fn new(store: Store, port: u16) -> anyhow::Result<Self> {
        let server_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
        let server = KrakenServer2::new(server_addr, store.clone())?;
        Ok(Self {
            port,
            store,
            peers: BTreeMap::new(),
            server,
        })
    }

    pub async fn connect_local(&mut self, port: u16) -> anyhow::Result<()> {
        anyhow::ensure!(port != self.port, "cannot connect to self");
        let server_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
        self.connect(server_addr).await
    }

    pub async fn connect(&mut self, remote_addr: SocketAddr) -> anyhow::Result<()> {
        let uri = format!("http://{}", remote_addr);
        let client = KrakenClient2::new(&uri)?;
        self.peers.insert(remote_addr, client);
        Ok(())
    }

    pub fn sync(&self, query: Query) -> impl Stream<Item = anyhow::Result<(usize, usize)>> + '_ {
        println!("syncing {}", query.root);
        try_stream! {
            loop {
                let peers = self.peers.values().collect::<Vec<_>>();
                let mut mine = self.store.have(query.clone())?;
                if mine.is_complete() {
                    break;
                }
                let total = mine.bitmap.len();
                let have = mine.bitmap.iter().filter(|b| **b).count();
                yield (have, total + 1);
                if let Some(peer) = peers.first() {
                    let mut query = query.clone();
                    query.bits = !mine.bitmap.clone();
                    query.bits.extend((0..1024).map(|_| true));
                    let (sink, mut stream) = peer.want(query).await?;
                    let mut n = 0;
                    let mut bytes = 0;
                    while let Some(response) = stream.next().await {
                        match response? {
                            WantResponse::Block(index, block) => {
                                n += 1;
                                bytes += block.data.len();
                                let cid = block.cid();
                                let links = parse_links(&cid, &block.data)?;
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
                    println!("want query terminated after {} blocks {} bytes", n, bytes);
                    drop(sink);
                }
            }
        }
        .boxed()
    }
}

/// Extract links from the given content.
///
/// Links will be returned as a sorted vec
pub fn parse_links(cid: &Cid, bytes: &[u8]) -> anyhow::Result<Vec<Cid>> {
    let codec = cid.codec();
    let codec = IpldCodec::try_from(codec)?;
    let mut links = Vec::new();
    codec.references::<Ipld, _>(bytes, &mut links)?;
    for link in links.iter_mut() {
        *link = to_v1(link);
    }
    Ok(links)
}

fn codec_name(codec: &IpldCodec) -> &str {
    match codec {
        IpldCodec::DagCbor => "dag-cbor",
        IpldCodec::DagPb => "dag-pb",
        IpldCodec::DagJson => "dag-json",
        IpldCodec::Raw => "raw",
    }
}

fn to_v1(cid: &Cid) -> Cid {
    Cid::new(cid::Version::V1, cid.codec(), *cid.hash()).unwrap()
}

pub async fn sync_peer(args: Args) -> anyhow::Result<()> {
    let store = Store::default();
    // create some data sets to sync

    // import some data sets
    if let Some(ss) = args.stats {
        for s in ss {
            let file = tokio::fs::File::open(&s).await?;
            let reader = iroh_car::CarReader::new(file).await?;
            let roots = reader.header().roots().to_vec();
            println!("computing stats for {}", s);
            for root in roots {
                println!("root {}", root);
            }
            let items = reader.stream().enumerate();
            tokio::pin!(items);
            let mut size_hist = BTreeMap::new();
            let mut links_hist = BTreeMap::new();
            let mut leaf_size = 0u64;
            let mut branch_size = 0u64;
            let mut leaf_count = 0u64;
            let mut branch_count = 0u64;
            let mut link_count = 0u64;
            while let Some((i, block)) = items.next().await {
                if i % 1000 == 0 {
                    print!("\r{}", i);
                    stdout().flush()?;
                }
                let (cid, data) = block?;
                let links = parse_links(&cid, &data)?;
                link_count += links.len() as u64;
                let c = size_hist.entry(data.len()).or_insert(0u64);
                *c += 1;
                let c = links_hist.entry(links.len()).or_insert(0u64);
                *c += 1;
                if links.is_empty() {
                    leaf_size += data.len() as u64;
                    leaf_count += 1;
                } else {
                    branch_size += data.len() as u64;
                    branch_count += 1;
                }
            }
            let stats = format!("{}.stats", s);
            let stats = Path::new(&stats);
            println!("removing {}", stats.display());
            let _ = std::fs::remove_dir_all(stats);
            println!("creating {}", stats.display());
            std::fs::create_dir_all(stats)?;
            println!("\rdone!");
            let size_hist = size_hist
                .into_iter()
                .map(|(k, v)| format!("{}\t{}", k, v))
                .collect::<Vec<_>>()
                .join("\n");
            std::fs::write(stats.join("size.hist"), size_hist)?;
            let links_hist = links_hist
                .into_iter()
                .map(|(k, v)| format!("{}\t{}", k, v))
                .collect::<Vec<_>>()
                .join("\n");
            std::fs::write(stats.join("links.hist"), links_hist)?;
            println!("branch size:\t{}", branch_size);
            println!("branch count:\t{}", branch_count);
            println!("leaf size:\t{}", leaf_size);
            println!("leaf count:\t{}", leaf_count);
            println!("total size:\t{}", branch_size + leaf_size);
            println!("total count:\t{}", branch_count + leaf_count);
            println!("link count:\t{}", link_count);
        }
        return Ok(());
    }
    if let Some(ss) = args.create {
        for s in ss {
            match s.as_str() {
                "tree" => {
                    let mut leafs = (1u64..).map(|i| {
                        // 8 kb data, unique for each leaf
                        i.to_be_bytes().repeat(1024)
                    });
                    let root = make_tree(&store, 10, 2, &mut leafs)?.unwrap();
                    println!("created dataset {}: {}", s, root);
                }
                _ => {
                    anyhow::bail!("unknown dataset {}", s);
                }
            }
        }
    }
    // import some data sets
    if let Some(ss) = args.import {
        for s in ss {
            let file = tokio::fs::File::open(&s).await?;
            let reader = iroh_car::CarReader::new(file).await?;
            let roots = reader.header().roots().to_vec();
            for root in roots {
                println!("importing root {} as {} from {}", root, to_v1(&root), s);
            }
            let items = reader.stream().enumerate();
            tokio::pin!(items);
            while let Some((i, block)) = items.next().await {
                if i % 1000 == 0 {
                    print!("\r{}", i);
                    stdout().flush()?;
                }
                let (cid, data) = block?;
                let links = parse_links(&cid, &data)?;
                let cid = to_v1(&cid);
                store.put(cid, data.into(), links.into())?;
            }
            println!("\rdone!");
        }
    }
    let port = args.port.unwrap_or(31337);
    println!("listening on port {}", port);
    let mut peer = Node::new(store, port)?;
    for addr in args.connect {
        println!("connecting to {:?}...", addr);
        peer.connect(addr).await?;
        println!("done");
    }
    if let Some(sync) = args.sync {
        for root in sync {
            println!("syncing {}...", root);
            let t0 = Instant::now();
            let mut updates = peer.sync(Query::new(root));
            while let Some(update) = updates.next().await {
                let (have, total) = update?;
                println!("syncing {} {}/{}", root, have, total);
            }
            println!("syncing {} done in {}s", root, t0.elapsed().as_secs_f64());
        }
    }
    futures::future::pending::<()>().await;
    Ok(())
}

pub(crate) async fn peer_sync_demo() -> anyhow::Result<()> {
    let mut peer1 = Node::new(Store::default(), 10001)?;
    let mut peer2 = Node::new(Store::default(), 10002)?;
    peer1.connect_local(10002).await?;
    peer2.connect_local(10001).await?;
    let mut leafs = (1u64..).map(|i| {
        // 8 kb unique data
        i.to_be_bytes().repeat(1024)
    });
    let root = make_tree(&peer1.store, 10, 2, &mut leafs)?.unwrap();
    let mut progress = peer2.sync(Query::new(root));
    while let Some(update) = progress.next().await {
        let update = update?;
        println!("{:?}", update);
    }
    Ok(())
}

pub async fn sync_demo() -> anyhow::Result<()> {
    let store = Store::default();
    let server_addr = "127.0.0.1:5000".parse()?;
    let server = KrakenServer2::new(server_addr, store.clone());

    let client = KrakenClient2::new("http://127.0.0.1:5000")?;
    let mut leafs = (1u64..).map(|i| {
        // 8 kb unique data
        i.to_be_bytes().repeat(1024)
    });
    let root = make_tree(&store, 10, 2, &mut leafs)?.unwrap();
    let (_send, mut recv) = client.want(Query::new(root)).await?;
    while let Some(item) = recv.next().await {
        let item = item?;
        println!("client got {:?}", item);
    }
    Ok(())
}
