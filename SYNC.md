Sync challenges

- stop traversal where you already have something
- avoid traversing too deep
- avoid retransferring the roots
- deterministic traversal even for nodes who have incomplete data

Depth + Traversal order + Stop Set = deterministic order
Deterministic order => bitmap

E.g. large unixfs file

3 levels, empty stop set, bitmap per peer

ranges in bitmap -> locality on source disk
round robin in bitmap

Can I get a bitmap from a node that does not have everything?


Define set of CIDs
- depth
- stop cids
- traversal order => bitmap

Query
- cid set
- want bitmap

Response
- Blocks in set in defined order
- Abort for first cid in the 1 set! that you don't have

Query stream
- bitmap updates (xor?)

Switch bits on -> other node was a flake
Switch bits off -> got it from other node

Switching bits on after the current position is useless, but who cares

