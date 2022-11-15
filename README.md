# krakensync
Sync experiment: Can we use bitmaps with memesync?

## TLDR

Define deterministic traversal to turn DAG into tree
 - using only data available to the store - no "deep inspection"
Then use bitmaps/offsets for everything
 - bitmap to check what a peer has
 - bitmap to define what to get
 - indexes for blocks (get a block => set bit at offset to 0)

## Basic Demo
Quick demo to show transferring data using krakensync. Create a dataset named "tree" and listen on port 10000:
```
krakensync -p 10000 --create tree
```

Connect to port 10000 and sync the CID (from the output of the above command):
```
krakensync --connect 127.0.0.1:10000 --sync bafkreicjb44bshgq5agfkj5mcjcotzgkjgubjhpimlx2k3ymjfpqxjfjym
```
