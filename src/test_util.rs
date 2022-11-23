#![allow(clippy::redundant_clone)]
use bitvec::prelude::BitVec;
use cid::Cid;
use libipld::{cbor::DagCborCodec, prelude::Codec, Ipld};
use multihash::MultihashDigest;

use crate::core::{Store, StoreWrite};
use crate::proto::WantResponse;

pub fn parse_bits(bits: &str) -> anyhow::Result<BitVec> {
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

pub trait StoreWriteExt: StoreWrite {
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

// Make a tree given tree parameters and an iterator of leafs
pub fn make_tree(
    store: &Store,
    branches: usize,
    depth: usize,
    leaf: &mut impl Iterator<Item = Vec<u8>>,
) -> anyhow::Result<Option<Cid>> {
    if depth == 0 {
        leaf.next().map(|data| store.write_raw(&data)).transpose()
    } else {
        let mut links = Vec::new();
        for i in 0..branches {
            if let Some(cid) = make_tree(store, branches, depth - 1, leaf)? {
                links.push(Ipld::Link(cid));
            }
        }
        Ok(if !links.is_empty() {
            Some(store.write_ipld(Ipld::List(links))?)
        } else {
            None
        })
    }
}

/// basically like a want response, but cid instead of block
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WantResponseShort {
    Block(Cid),
    NotFound(Cid),
    StopNotFound(Cid),
    MaxBlocks,
    MaxBytes,
    InternalError(String),
}

pub fn short(response: WantResponse) -> WantResponseShort {
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
