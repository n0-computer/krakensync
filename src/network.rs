use anyhow::{bail, Context, Ok};
use async_stream::try_stream;
use cid::Cid;
use futures::{stream::BoxStream, Sink, SinkExt, Stream, StreamExt};
use libipld::{cbor::DagCborCodec, prelude::Codec, Ipld, IpldCodec};
use quinn::{ClientConfig, Connecting, Endpoint, ServerConfig};
use std::{
    collections::BTreeMap,
    io::{self, stdout, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
    time::Instant,
};
use tokio::task::JoinHandle;
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::{
    core::{Args, Store, StoreReadExt},
    proto::{Query, Request, WantRequestUpdate, WantResponse},
    test_util::make_tree,
};

/// Constructs a QUIC endpoint configured for use a client only.
///
/// ## Args
///
/// - server_certs: list of trusted certificates.
#[allow(unused)]
pub fn make_client_endpoint(
    bind_addr: SocketAddr,
    server_certs: &[&[u8]],
) -> anyhow::Result<Endpoint> {
    let client_cfg = configure_client(server_certs)?;
    let mut endpoint = Endpoint::client(bind_addr)?;
    endpoint.set_default_client_config(client_cfg);
    Ok(endpoint)
}

// insecure connection example from quinn-rs: https://github.com/quinn-rs/quinn/blob/369573482a1e716d3d7d47b2e74ab94cb1f88db5/quinn/examples/insecure_connection.rs
// insecure connection example from quic-rs: https://github.com/n0-computer/quic-rpc/blob/1b17e92fb0d39df2204f22b31f50b4bfb5b837f0/examples/split/client/src/main.rs#L65
pub fn make_insecure_client_endpoint(bind_addr: SocketAddr) -> anyhow::Result<Endpoint> {
    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();

    let client_cfg = ClientConfig::new(Arc::new(crypto));
    let mut endpoint = Endpoint::client(bind_addr)?;
    endpoint.set_default_client_config(client_cfg);
    Ok(endpoint)
}

struct SkipServerVerification;
impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        std::result::Result::Ok(rustls::client::ServerCertVerified::assertion())
    }
}

/// Constructs a QUIC endpoint configured to listen for incoming connections on a certain address
/// and port.
///
/// ## Returns
///
/// - a stream of incoming QUIC connections
/// - server certificate serialized into DER format
#[allow(unused)]
pub fn make_server_endpoint(bind_addr: SocketAddr) -> anyhow::Result<(Endpoint, Vec<u8>)> {
    let (server_config, server_cert) = configure_server()?;
    let endpoint = Endpoint::server(server_config, bind_addr)?;
    Ok((endpoint, server_cert))
}

/// Builds default quinn client config and trusts given certificates.
///
/// ## Args
///
/// - server_certs: a list of trusted certificates in DER format.
fn configure_client(server_certs: &[&[u8]]) -> anyhow::Result<ClientConfig> {
    let mut certs = rustls::RootCertStore::empty();
    for cert in server_certs {
        certs.add(&rustls::Certificate(cert.to_vec()))?;
    }

    Ok(ClientConfig::with_root_certificates(certs))
}

/// Returns default server configuration along with its certificate.
#[allow(clippy::field_reassign_with_default)] // https://github.com/rust-lang/rust-clippy/issues/6527
pub fn configure_server() -> anyhow::Result<(ServerConfig, Vec<u8>)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.serialize_der()?;
    let priv_key = cert.serialize_private_key_der();
    let priv_key = rustls::PrivateKey(priv_key);
    let cert_chain = vec![rustls::Certificate(cert_der.clone())];

    let mut server_config = ServerConfig::with_single_cert(cert_chain, priv_key)?;
    Arc::get_mut(&mut server_config.transport)
        .unwrap()
        .max_concurrent_uni_streams(0_u8.into());

    Ok((server_config, cert_der))
}

async fn handle_client(incoming_conn: Connecting) -> anyhow::Result<()> {
    let conn = incoming_conn.await.context("accept failed")?;
    println!(
        "[server] connection accepted: addr={}",
        conn.remote_address()
    );
    let (send, recv) = conn.accept_bi().await?;
    // turn chunks of bytes into a stream of messages using length delimited codec
    let send = FramedWrite::new(send, LengthDelimitedCodec::new());
    let recv = FramedRead::new(recv, LengthDelimitedCodec::new());
    // turn the stream of messages into a stream of strings
    let mut recv = SymmetricallyFramed::new(recv, SymmetricalBincode::<String>::default());
    let mut send = SymmetricallyFramed::new(send, SymmetricalBincode::<String>::default());
    while let Some(msg) = recv.next().await {
        let msg = msg?;
        send.send(msg).await?;
        println!("and sent it back!");
    }
    anyhow::Ok(())
    // Dropping all handles associated with a connection implicitly closes it
}

async fn make_server(endpoint: Endpoint) -> anyhow::Result<()> {
    // accept connections and process them on a separate task
    while let Some(conn) = endpoint.accept().await {
        tokio::spawn(handle_client(conn));
    }
    Ok(())
}

/// Server for the kraken sync protocol
pub struct KrakenServer {
    endpoint: Endpoint,
    store: Store,
}

impl KrakenServer {
    pub fn new(endpoint: Endpoint, store: Store) -> Self {
        Self { endpoint, store }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        // accept connections and process them on a separate task
        while let Some(conn) = self.endpoint.accept().await {
            tokio::spawn(Self::handle_client(conn, self.store.clone()));
        }
        Ok(())
    }

    async fn handle_client(conn: Connecting, store: Store) -> anyhow::Result<()> {
        let conn = conn.await.context("accept failed")?;
        tracing::info!(
            "[server] connection accepted: addr={}",
            conn.remote_address()
        );
        let (send, recv) = conn.accept_bi().await?;
        // turn chunks of bytes into a stream of messages using length delimited codec
        let send = FramedWrite::new(send, LengthDelimitedCodec::new());
        let recv = FramedRead::new(recv, LengthDelimitedCodec::new());
        let mut recv = SymmetricallyFramed::new(recv, SymmetricalBincode::<Request>::default());
        let request = recv.next().await.context("no msg")??;
        tracing::info!("got request: {:?}", request);
        match request {
            Request::Want(query) => {
                let recv = recv.into_inner();
                // now switch to streams of WantRequestUpdate and WantResponse
                let recv = SymmetricallyFramed::new(
                    recv,
                    SymmetricalBincode::<WantRequestUpdate>::default(),
                );
                let send =
                    SymmetricallyFramed::new(send, SymmetricalBincode::<WantResponse>::default());
                Self::handle_want(query, store, send, recv).await
            }
            Request::Have(_query) => {
                anyhow::bail!("not implemented");
            }
        }
    }

    async fn handle_want(
        query: Query,
        store: Store,
        mut send: impl Sink<WantResponse, Error = io::Error> + Unpin,
        mut recv: impl Stream<Item = io::Result<WantRequestUpdate>> + Send + Sync + Unpin + 'static,
    ) -> anyhow::Result<()> {
        tracing::info!("handling want");
        tokio::spawn(async move {
            while let Some(recv) = recv.next().await {
                let recv = recv?;
                tracing::info!("MESSAGE_RECEIVED - got update: {:?}", recv);
            }
            Ok(())
        });
        for item in store.want(query) {
            tracing::info!("MESSAGE_SENT - sending item: {:?}", item);
            send.send(item).await?;
        }
        Ok(())
    }
}

struct KrakenClient {
    conn: quinn::Connection,
}

impl KrakenClient {
    pub async fn new(
        endpoint: Endpoint,
        server_addr: SocketAddr,
        server_name: &str,
    ) -> anyhow::Result<Self> {
        let conn = endpoint.connect(server_addr, server_name)?.await?;
        tracing::info!("[client] connected: addr={}", conn.remote_address());
        Ok(Self { conn })
    }

    pub async fn want(
        &self,
        query: Query,
    ) -> anyhow::Result<(
        impl Sink<WantRequestUpdate>,
        BoxStream<io::Result<WantResponse>>,
    )> {
        let (send, recv) = self.conn.open_bi().await?;
        // turn chunks of bytes into a stream of messages using length delimited codec
        let send = FramedWrite::new(send, LengthDelimitedCodec::new());
        let recv = FramedRead::new(recv, LengthDelimitedCodec::new());
        let mut send = SymmetricallyFramed::new(send, SymmetricalBincode::<Request>::default());
        // send a want request
        tracing::info!("MESSAGE_SENT - sending want request {:#?}", query);
        send.send(Request::Want(query)).await?;
        let send = send.into_inner();
        // now switch to streams of WantRequestUpdate and WantResponse
        let recv = SymmetricallyFramed::new(recv, SymmetricalBincode::<WantResponse>::default());
        let send =
            SymmetricallyFramed::new(send, SymmetricalBincode::<WantRequestUpdate>::default());
        Ok((send, recv.boxed()))
    }
}

pub struct Node {
    port: u16,
    store: Store,
    peers: BTreeMap<SocketAddr, KrakenClient>,
    server: JoinHandle<anyhow::Result<()>>,
    cert: Vec<u8>,
}

impl Node {
    pub fn new(
        store: Store,
        server_addr: SocketAddr,
        server_config: ServerConfig,
        cert: Vec<u8>,
    ) -> anyhow::Result<Self> {
        let port = server_addr.port();
        let endpoint = Endpoint::server(server_config, server_addr)?;
        let server = KrakenServer::new(endpoint, store.clone());
        let server = tokio::task::spawn(server.run());
        Ok(Self {
            port,
            store,
            peers: BTreeMap::new(),
            server,
            cert,
        })
    }

    pub async fn connect_local(&mut self, port: u16) -> anyhow::Result<()> {
        anyhow::ensure!(port != self.port, "cannot connect to self");
        let server_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
        self.connect(server_addr).await
    }

    pub async fn connect(&mut self, remote_addr: SocketAddr) -> anyhow::Result<()> {
        let bind_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));
        let endpoint = make_client_endpoint(bind_addr, &[&self.cert])?;
        let client = KrakenClient::new(endpoint, remote_addr, "localhost").await?;
        self.peers.insert(remote_addr, client);
        Ok(())
    }

    pub async fn insecure_connect(&mut self, remote_addr: SocketAddr) -> anyhow::Result<()> {
        let bind_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));
        let endpoint = make_insecure_client_endpoint(bind_addr)?;
        let client = KrakenClient::new(endpoint, remote_addr, "localhost").await?;
        self.peers.insert(remote_addr, client);
        Ok(())
    }

    pub fn sync(&self, query: Query) -> impl Stream<Item = anyhow::Result<(usize, usize)>> + '_ {
        println!("syncing {}", query.root);
        let t0 = Instant::now();
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
                    let (_sink, mut stream) = peer.want(query).await?;
                    while let Some(response) = stream.next().await {
                        tracing::info!("MESSAGE_RECEIVED");
                        match response? {
                            WantResponse::Block(index, block) => {
                                if index == 0 {
                                    tracing::info!("REQUESTER_TTFB {:?}", t0.elapsed().as_secs_f64());
                                }
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
                    tracing::info!("REQUESTER_FETCH_DURATION {:?}", t0.elapsed().as_secs_f64());
                }
            }
        }
        .boxed()
    }
}

async fn make_client(endpoint: Endpoint, server_addr: SocketAddr) -> anyhow::Result<()> {
    let connection = endpoint.connect(server_addr, "localhost")?.await?;
    tracing::info!("[client] connected: addr={}", connection.remote_address());
    let (send, recv) = connection.open_bi().await?;
    // turn chunks of bytes into a stream of messages using length delimited codec
    let send = FramedWrite::new(send, LengthDelimitedCodec::new());
    let recv = FramedRead::new(recv, LengthDelimitedCodec::new());
    // turn the stream of messages into a stream of strings
    let mut recv = SymmetricallyFramed::new(recv, SymmetricalBincode::<String>::default());
    let mut send = SymmetricallyFramed::new(send, SymmetricalBincode::<String>::default());
    send.send("hello world".to_string()).await?;
    if let Some(msg) = recv.next().await {
        let msg = msg?;
        println!("client got {}", msg);
    }
    send.into_inner().into_inner().finish().await?;

    // Give the server has a chance to clean up
    endpoint.wait_idle().await;
    Ok(())
}

/// read hardcoded config for localhost
pub fn read_localhost_config() -> anyhow::Result<(ServerConfig, Vec<u8>)> {
    let cert_der = include_bytes!("../certs/cert.der").to_vec();
    let priv_key = include_bytes!("../certs/priv.key").to_vec();
    let priv_key = rustls::PrivateKey(priv_key);
    let cert_chain = vec![rustls::Certificate(cert_der.clone())];
    let mut server_config = ServerConfig::with_single_cert(cert_chain, priv_key)?;
    Arc::get_mut(&mut server_config.transport)
        .unwrap()
        .max_concurrent_uni_streams(0_u8.into());
    Ok((server_config, cert_der))
}

/// Extract links from the given content.
///
/// Links will be returned as a sorted vec
pub fn parse_links(cid: &Cid, bytes: &[u8]) -> anyhow::Result<Vec<Cid>> {
    let codec = cid.codec();
    let mut cids = Vec::new();
    let codec = match codec {
        0x71 => IpldCodec::DagCbor,
        0x70 => IpldCodec::DagPb,
        0x0129 => IpldCodec::DagJson,
        0x55 => IpldCodec::Raw,
        _ => bail!("unsupported codec {:?}", codec),
    };
    codec.references::<Ipld, _>(bytes, &mut cids)?;
    let links = cids.into_iter().collect();
    Ok(links)
}

#[derive(Debug, Default)]
pub struct CarImportStats {
    pub size_hist: BTreeMap<usize, u64>,
    pub links_hist: BTreeMap<usize, u64>,
    pub leaf_size: u64,
    pub branch_size: u64,
    pub leaf_count: u64,
    pub branch_count: u64,
}

pub async fn import_car_file(
    store: &Store,
    path: String,
) -> anyhow::Result<(Vec<Cid>, CarImportStats)> {
    let file = tokio::fs::File::open(&path).await?;
    let reader = iroh_car::CarReader::new(file).await?;
    let roots = reader.header().roots().to_vec();
    println!("computing stats for {}", path);
    for root in roots.clone() {
        println!("root {}", root);
    }
    let items = reader.stream().enumerate();
    tokio::pin!(items);
    let mut stats = CarImportStats::default();
    while let Some((i, block)) = items.next().await {
        if i % 1000 == 0 {
            print!("\r{}", i);
            stdout().flush()?;
        }
        let (cid, data) = block?;
        let links = parse_links(&cid, &data)?;
        let c = stats.size_hist.entry(data.len()).or_insert(0u64);
        *c += 1;
        let c = stats.links_hist.entry(links.len()).or_insert(0u64);
        *c += 1;
        if links.is_empty() {
            stats.leaf_size += data.len() as u64;
            stats.leaf_count += 1;
        } else {
            stats.branch_size += data.len() as u64;
            stats.branch_count += 1;
        }
        store.put(cid, data.into(), links.into())?;
    }
    Ok((roots, stats))
}

pub async fn sync_peer(args: Args) -> anyhow::Result<()> {
    let (server_config, server_cert) = read_localhost_config()?;
    let store = Store::default();
    // create some data sets to sync

    // import some data sets
    if let Some(ss) = args.stats {
        for s in ss {
            let (_, stats) = import_car_file(&store, s).await?;
            println!("\rdone!");
            println!("size histogram:");
            for (size, count) in stats.size_hist {
                println!("{}\t{}", size, count);
            }
            println!("links histogram:");
            for (links, count) in stats.links_hist {
                println!("{}\t{}", links, count);
            }
            println!("branch size:\t{}", stats.branch_size);
            println!("branch count:\t{}", stats.branch_count);
            println!("leaf size:\t{}", stats.leaf_size);
            println!("leaf count:\t{}", stats.leaf_count);
            println!("total size:\t{}", stats.branch_size + stats.leaf_size);
            println!("total count:\t{}", stats.branch_count + stats.leaf_count);
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
                println!("importing root {} from {}", root, s);
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
                store.put(cid, data.into(), links.into())?;
            }
            println!("\rdone!");
        }
    }
    let port = args.port.unwrap_or(31337);
    println!("listening on port {}", port);
    let server_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
    let mut peer = Node::new(
        store,
        server_addr,
        server_config.clone(),
        server_cert.clone(),
    )?;
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
    let (server_config, server_cert) = read_localhost_config()?;

    let server_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 10001));
    let mut peer1 = Node::new(
        Store::default(),
        server_addr,
        server_config.clone(),
        server_cert.clone(),
    )?;

    let server_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 10002));
    let mut peer2 = Node::new(
        Store::default(),
        server_addr,
        server_config.clone(),
        server_cert.clone(),
    )?;
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
    let (endpoint, server_cert) = make_server_endpoint(server_addr)?;
    let server = KrakenServer::new(endpoint, store.clone());
    tokio::spawn(server.run());

    let endpoint = make_client_endpoint("0.0.0.0:0".parse()?, &[&server_cert])?;
    let client = KrakenClient::new(endpoint, server_addr, "localhost").await?;
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

pub async fn main() -> anyhow::Result<()> {
    let server_addr = "127.0.0.1:5000".parse()?;
    let (endpoint, server_cert) = make_server_endpoint(server_addr)?;
    // accept a single connection
    tokio::spawn(async move { make_server(endpoint).await });

    let endpoint = make_client_endpoint("0.0.0.0:0".parse().unwrap(), &[&server_cert])?;
    // connect to server
    make_client(endpoint, server_addr).await?;

    let endpoint = make_client_endpoint("0.0.0.0:0".parse().unwrap(), &[&server_cert])?;
    make_client(endpoint, server_addr).await?;

    Ok(())
}
