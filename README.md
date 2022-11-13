# krakensync
Sync experiment: Can we use bitmaps with memesync?

## Basic Demo
Quick demo to show transferring data using krakensync. Create a dataset named "tree" and listen on port 10000:
```
krakensync -p 10000 --create tree
```

Connect to port 10000 and sync the CID (from the output of the above command):
```
krakensync --connect 127.0.0.1:10000 --sync bafkreicjb44bshgq5agfkj5mcjcotzgkjgubjhpimlx2k3ymjfpqxjfjym
```
