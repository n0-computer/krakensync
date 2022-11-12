use anyhow::{Context, Ok};
use futures::{stream::BoxStream, Sink, SinkExt, Stream, StreamExt};
use quinn::{ClientConfig, Connecting, Endpoint, ServerConfig};
use std::{io, net::SocketAddr, sync::Arc};
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::info;

use crate::{
    proto::{Query, Request, WantRequestUpdate, WantResponse},
    test_util::make_tree,
    Store,
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
fn configure_server() -> anyhow::Result<(ServerConfig, Vec<u8>)> {
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
        mut recv: impl Stream<Item = io::Result<WantRequestUpdate>> + Unpin,
    ) -> anyhow::Result<()> {
        tracing::info!("handling want");
        send.send(WantResponse::InternalError("not implemented".into()))
            .await?;
        while let Some(recv) = recv.next().await {
            let recv = recv?;
            tracing::info!("got update: {:?}", recv);
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
        tracing::info!("sending want request {:#?}", query);
        send.send(Request::Want(query)).await?;
        let send = send.into_inner();
        // now switch to streams of WantRequestUpdate and WantResponse
        let recv = SymmetricallyFramed::new(recv, SymmetricalBincode::<WantResponse>::default());
        let send =
            SymmetricallyFramed::new(send, SymmetricalBincode::<WantRequestUpdate>::default());
        Ok((send, recv.boxed()))
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