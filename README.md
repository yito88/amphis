![](https://github.com/yito88/amphis/workflows/Amphis/badge.svg)

# Amphis
An embedded key-value store

# TODO
- API
  - [x] get()
  - [x] put()
    - including insert and update
  - [x] delete()

- Config
  - [ ] FPTree config
  - [ ] SSTable config

- FPTree
  - [x] Simplified FPTree
  - [x] Leaf file
  - [x] Concurrency
  - [x] Flush (converted to SSTable)
  - [x] Recovery (flush)
  - [ ] tail header (for durable write)
  - [x] Extended leaf page

- SSTable
  - [x] SSTable file
  - [x] Bloom filter/Sparse index
  - [ ] Compaction
  - [x] Recovery

- Others
  - [ ] Error handling
  - [ ] Backgound thread

# Threshold for flushing
Basically, the number of keys triggers flushing (Converting FPTree to SSTable) because the number of the root split times on the FPTree is the threshold.
Where you insert K keys and the N split happens, the relation between K and N would be:
```math
8 \times (3^{n+1} + 1)
```
If you set the `root_split_threshold` to 6, flush would happen every 17,504 insertions.
