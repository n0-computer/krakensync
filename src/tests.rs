#![allow(clippy::redundant_clone)]
use crate::{proto::{Direction, Traversal, Query}, core::{Store, StoreReadExt, StoreWrite, Node}};

use super::*;
use futures::StreamExt;
use libipld::ipld;
use test_util::*;

#[test]
    #[rustfmt::skip]
    fn smoke() -> anyhow::Result<()> {
        use Direction::*;
        use Traversal::*;
        use WantResponseShort::*;
        let store = Store::default();

        // small helper to reduce test boilerplate
        let have = |q: &Query| anyhow::Ok(store.have(q.clone())?.bitmap);
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

// #[test]
// fn deflate() {
//     let data = [
//         1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
//         1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
//         1, 1, 1, 1, 1, 1,
//     ];
//     let def = lz4_flex::compress(&data);
//     println!("deflated: {:?}", def.len());
// }

#[test]
fn mk_tree() -> anyhow::Result<()> {
    let store = Store::default();
    let mut leafs = (1u64..).map(|i| {
        // 8 kb unique data
        i.to_be_bytes().repeat(1024)
    });
    let root = make_tree(&store, 10, 4, &mut leafs)?.unwrap();
    let bitmap = store.have(Query::new(root))?;
    for r in store.want(Query::new(root)) {
        println!("{:?}", r);
    }
    Ok(())
}

#[tokio::test]
async fn two_peer_sync() -> anyhow::Result<()> {
    let store1 = Store::default();
    let store2 = Store::default();
    let mut node1 = Node::new(store1.clone());
    let mut node2 = Node::new(store2.clone());
    let mut leafs = (1u64..).map(|i| {
        // 8 kb unique data
        i.to_be_bytes().repeat(1024)
    });
    let root = make_tree(&store1, 10, 2, &mut leafs)?.unwrap();
    node2.add_peer(0, Box::new(node1));
    let mut stream = node2.sync(Query::new(root)).await?;
    while let Some(_) = stream.next().await {
        print!(".");
    }
    Ok(())
}
