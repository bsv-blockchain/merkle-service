This repository defines a Merkle Service server that supports returning merkle proofs for transactions in a world of teranode.

# High Level Overview
Merkle service allows for arcade instances to register transactions with this
server via a callback URL in which merkle service can return STUMPs. See
[swimlane diagram](./diagram.png) for more details.

# Terms
- [BUMP (Bitcoin Unified Merkle Path)](https://github.com/bitcoin-sv/BRCs/blob/master/transactions/0074.md#abstract) is for full block merkle path for a given transaction
- STUMP (Subtree Unified Merkle Path) follows the same format as BUMP but is a merkle path for a transaction in a given subtree

# Relevant Projects
- [Arcade](https://github.com/bsv-blockchain/arcade)
- [Teranode](https://github.com/bsv-blockchain/teranode)

# Arcade workflow
- arcade stores all blocks (using teranode block definition)
- arcade registers transaction with merkle service
- arcade receives callback when tx is seen in a subtree
- arcade receives callback with STUMP in block
- arcade needs to use BEEF of coinbase transaction to build the full bump

# Requirements
- Follow the same daemon/service pattern that [Teranode](https://github.com/bsv-blockchain/teranode) uses
- Build with a microservice architecture in mind, but support runing all-in-one like Teranode
- Support scale where blocks contain millions of transactions, subtrees will be thousands of transactions
- Leverage teranode learnings wherever possible to support high-throughput and massive blocks

# Services
- api-server
  - registers transactions/callbacks for businesses
    - stores in aerospike
- p2p client
  - listens for subtrees and blocks
  - publishes to kafka
  - teranode libp2p client
- kafka
  - topics:
    - subtree
    - block
    - stumps
- subtreeprocessor
  - subscribes to kafka subtree topic
  - storing subtrees to subtreestore with 1 block TTL
  - adds all transactions to a queue
  - checking if transaction is registered, if so hit callback with SEEN_ON_NETWORK
  - implements some mechanism to deduplicate requests to aerospike in future
    - txmetacache is useful for this (could use counter with threshold with new status SEEN_MULTIPLE_NODES etc)
- database
  - txid: callback_url
  - basic key-value store that scales really well
- blockprocessor
  - fire off multiple blocksubtreeprocessors in parallel to process each subtree
  - after all blocksubtreeprocessors are complete, clear out subtreestore (do this somehow, idk)
- blocksubtreeprocessor
  - partitions/parellizes subtree processing for all subtrees that come within a block
  - just as a note; this subtree processor receives subtree 0 with the coinbase placeholder; does not do coinbase transaction replacement
  - get all txids from subtree
  - check callback_urls for all txs that have been registered
  - Build stump per callback URL
  - Push stump to kafka
  - update TTL of all transactions to 30 minutes to allow for forks/orphans but not keep it forever
- callback
  - subscribes to stumps kafka topic
  - for each stump perform callback to appropriate business
  - if callback fails, put it back on kafka

# Assumptions:
- make behavior of subtree storing configurable within subtreeprocessor vs blockprocessor
- assume for now all subtrees are stored in real time as they get them
- teranode block definition should include BEEF of coinbase transaction
- all aerospike calls need to be batched
  - See [blockassembler](https://github.com/bsv-blockchain/teranode/blob/main/services/blockassembly/Client.go#L243) for reference.

