# krakensync
Sync experiment: Can we use bitmaps with memesync?

## TLDR

Define deterministic traversal to turn DAG into tree
 - using only data available to the store - no "deep inspection"
Then use bitmaps/offsets for everything
 - bitmap to check what a peer has
 - bitmap to define what to get
 - indexes for blocks (get a block => set bit at offset to 0)

## Deterministic traversal

Given a query, the order in which blocks are traversed will always be deterministic for a store that has all the data.

So the blocks are in a sequence, and we can use the offset in that sequence to talk about a block.

E.g. the bit in a bitmap at an offset indicates if we want the block or not. Then when we get a block from a remote peer it comes with an offset, so we can a) validate its Cid and b) notify other peers that we don't need this offset anymore.

When a store has incomplete data, it *must* abort an iteration as soon as it can no longer determine the offset of a block.
E.g. for a depth first traversal with no depth limit: you have a node `x` with two children `a` and `b`. If you don't have
`a` you don't know if it is a leaf block or if it has children, so you can not assign an offset for `b` and have to abort
the iteration despite knowing that `b` will appear in the sequence somewhere.

In some cases you can continue iterating despite not having all data. E.g. in a breadth first traversal you can always
iterate over children of a node in sequence, even if you don't have some of them. For a depth first traversal with a depth
limit, you can also iterate over "leafs". (with a depth limit of 2 a node at level 2 is a child for the purpose of your
query, even if it is not a child in the real dag)

**Note: to make a traversal deterministic we have to have some defined order for the children of a block.**

The chosen order is the order in which the links appear in the block, which is better than e.g. sorting by hash for
the very common case of a unixfs file branch node. The order in which links appear in the block is exactly how the file
is laid out and will be consumed, whereas the hash sorted order is pretty random.

## Basic Demo
Quick demo to show transferring data using krakensync. Create a dataset named "tree" and listen on port 10000:
```
krakensync -p 10000 --create tree
```

Connect to port 10000 and sync the CID (from the output of the above command):
```
krakensync --connect 127.0.0.1:10000 --sync bafkreicjb44bshgq5agfkj5mcjcotzgkjgubjhpimlx2k3ymjfpqxjfjym
```
