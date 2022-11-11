use quinn::{ClientConfig, Endpoint, ServerConfig};
use std::{error::Error, net::SocketAddr, sync::Arc};

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

#[allow(unused)]
pub const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];

pub async fn main() -> anyhow::Result<()> {
    let server_addr = "127.0.0.1:5000".parse().unwrap();
    let (endpoint, server_cert) = make_server_endpoint(server_addr)?;
    // accept a single connection
    let endpoint2 = endpoint.clone();
    tokio::spawn(async move {
        let incoming_conn = endpoint2.accept().await.unwrap();
        let conn = incoming_conn.await.unwrap();
        println!(
            "[server] connection accepted: addr={}",
            conn.remote_address()
        );
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();
        let mut buf = [0u8; 1024];
        while let Some(n) = recv.read(&mut buf).await.unwrap() {
            println!("server got {}", String::from_utf8_lossy(&buf[..n]));
            send.write_all(buf[..n].as_ref()).await.unwrap();
        }
        // Dropping all handles associated with a connection implicitly closes it
    });

    let endpoint = make_client_endpoint("0.0.0.0:0".parse().unwrap(), &[&server_cert])?;
    // connect to server
    let connection = endpoint.connect(server_addr, "localhost")?.await?;
    println!("[client] connected: addr={}", connection.remote_address());
    let (mut send, mut recv) = connection.open_bi().await?;
    let mut buf = vec![0u8; 10];
    send.write_all(b"hello world").await?;
    while let Some(n) = recv.read(&mut buf).await? {
        println!("client got {}", String::from_utf8_lossy(&buf[..n]));
    }
    send.finish().await?;

    // Give the server has a chance to clean up
    endpoint.wait_idle().await;

    Ok(())
}
