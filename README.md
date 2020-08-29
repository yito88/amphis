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
  - [x] B+Tree
  - [x] Leaf file
  - [ ] Concurrency
  - [ ] Flush (converted to SSTable)
  - [ ] Recovery

- SSTable
  - [ ] SSTable file
  - [ ] Bloom filter
  - [ ] Compaction
  - [ ] Recovery
