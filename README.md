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
  - [ ] Recovery (flush)
  - [ ] tail header (for durable write)
  - [x] Reclaim a leaf page
  - [ ] Extended leaf page

- SSTable
  - [x] SSTable file
  - [x] Bloom filter/Sparse index
  - [ ] Compaction
  - [ ] Recovery
