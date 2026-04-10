# MIT 6.5840 Distributed Systems Labs

My solutions to the [MIT 6.5840 (Spring 2024)](https://pdos.csail.mit.edu/6.824/) Distributed Systems lab assignments.

## Labs

| Lab | Description | Status |
|-----|-------------|--------|
| Lab 1: MapReduce | Distributed MapReduce framework with fault tolerance | ✅ All tests pass |
| Lab 2: Key/Value Server | Single-machine KV server with at-most-once semantics | ✅ All tests pass (10/10) |
| Lab 3: Raft | Full Raft consensus protocol (election, log replication, persistence, snapshots) | ✅ All tests pass (28/28) |
| Lab 4: Fault-tolerant KV Service | Replicated KV service built on Raft, with snapshots | ✅ All tests pass (23/23) |
| Lab 5: Sharded KV Service | Sharded KV with dynamic configuration changes and shard migration | ✅ All tests pass (5A+5B 11/11) |

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    Lab 5: Sharded KV                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │
│  │  ShardKV    │  │  ShardKV    │  │  ShardKV    │      │
│  │  Group 1    │  │  Group 2    │  │  Group 3    │      │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘      │
│         └────────────────┼────────────────┘              │
│                    ┌─────┴──────┐                        │
│                    │ShardCtrler │                        │
│                    └────────────┘                        │
├──────────────────────────────────────────────────────────┤
│              Lab 4: KV Raft Service                      │
│  ┌────────────────────────────────────────┐              │
│  │  KVServer + Raft (replicated state     │              │
│  │  machine with snapshots)               │              │
│  └────────────────────────────────────────┘              │
├──────────────────────────────────────────────────────────┤
│              Lab 3: Raft Consensus                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐               │
│  │  Leader  │──│ Follower │──│ Follower │               │
│  └──────────┘  └──────────┘  └──────────┘               │
│  Leader Election · Log Replication · Persistence         │
│  Snapshots · Fast Log Backtracking                       │
├──────────────────────────────────────────────────────────┤
│  Lab 2: KV Server          │  Lab 1: MapReduce          │
│  Single-node KV with       │  Distributed map/reduce    │
│  at-most-once semantics    │  with worker fault tolerance│
└──────────────────────────────────────────────────────────┘
```

## Running Tests

```bash
cd src

# Lab 1 - MapReduce
cd main && bash test-mr.sh && cd ..

# Lab 2 - KV Server
go test ./kvsrv/...

# Lab 3 - Raft
go test ./raft/...

# Lab 4 - KV Raft
go test ./kvraft/...

# Lab 5 - Sharded KV
go test ./shardctrler/...
go test ./shardkv/...
```

## Key Implementation Details

### Raft (Lab 3)
- Leader election with randomized timeouts
- Log replication with consistency checks
- Persistence of `currentTerm`, `votedFor`, and `log` via `labgob`
- Log compaction via snapshots with `InstallSnapshot` RPC
- Fast log backtracking optimization (XTerm/XIndex/XLen)

### KV Raft (Lab 4)
- Client-side duplicate detection using `(ClientId, SeqNum)` pairs
- Snapshot-based state transfer for crash recovery
- Leadership change detection to avoid stale responses

### Sharded KV (Lab 5)
- Shard controller with balanced shard assignment across groups
- Two-phase shard migration: config change first, then pull data
- Per-shard state tracking (`shardReady`) to prevent serving stale data
- Sequential config processing to maintain consistency during multi-step migrations
