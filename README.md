# ⚡ RaftLab

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Build Status](https://img.shields.io/badge/tests-passing-brightgreen?style=flat)]()
[![Deterministic Simulation](https://img.shields.io/badge/simulation-deterministic-blueviolet?style=flat)]()
[![Zero External Dependencies](https://img.shields.io/badge/dependencies-zero-success?style=flat)]()

> **Single-process, deterministic Raft consensus engine with discrete-event fault injection.**  
> Built strictly to the Figure 2 specification of Diego Ongaro's Raft paper (*"In Search of an Understandable Consensus Algorithm"*).

---

## 📖 Overview

Distributed consensus algorithms are notoriously difficult to test, debug, and reason about. In traditional networked clusters, edge cases depend on arbitrary thread scheduling, wall-clock timing variations, and unpredictable network latency — making flaky bugs nearly impossible to reproduce reliably.

**RaftLab** eliminates these headaches by implementing Raft inside a **single-process discrete-event simulator**:
- **Virtual Time**: Time advances instantaneously via an event priority queue (`container/heap`). A 10-second cluster trace simulates in milliseconds.
- **Zero Sockets / Zero Threads**: Peers communicate via synchronous message queues owned by the simulation scheduler.
- **100% Deterministic Fault Injection**: Message loss, asymmetric partitions, election timeouts, node crashes, and restarts are driven exclusively by a seeded pseudo-random generator (`math/rand`). The exact same seed replays the exact same event trace byte-for-byte.

```
       ┌────────────────────────────────────────────────────────┐
       │                 Discrete-Event Sim                     │
       │       [Virtual Clock] ─── [Event Priority Queue]       │
       │            │                       │                   │
       │     Deterministic Faults    Deterministic Delays       │
       │   (Crashes, Partitions)     (Heartbeats, Timeouts)     │
       └────────────┬───────────────────────┬───────────────────┘
                    │                       │
     ┌──────────────▼─────┐          ┌──────▼─────────────┐
     │  Raft Peer 0 (L)   │ ◄──────► │  Raft Peer 1..N    │
     │ ┌────────────────┐ │   RPC    │ ┌────────────────┐ │
     │ │   Raft State   │ │ Messages │ │   Raft State   │ │
     │ │ (Log, Term...) │ │          │ │ (Log, Term...) │ │
     │ └───────┬────────┘ │          │ └───────┬────────┘ │
     │         ▼          │          │         ▼          │
     │   [KV Machine]     │          │   [KV Machine]     │
     └────────────────────┘          └────────────────────┘
```

---

## ✨ Features & Scope

### Implemented (Ongaro Figure 2)
- **Leader Election**: Randomized election timeouts, term increment, candidate transition, majority quorum voting.
- **Log Replication**: `AppendEntries` RPC with consistency checks (`PrevLogIndex`, `PrevLogTerm`).
- **Election Restriction**: Candidates with less up-to-date logs are denied votes.
- **Current-Term Commit Rule**: Leaders only advance `commitIndex` for entries created in their current term.
- **Log Reconciliation**: Automatic truncation of conflicting uncommitted suffixes upon reconnecting to the leader.
- **State Machine Application**: Strict in-order application of committed entries to a replicated Key-Value store (`internal/machine/kv.go`).
- **Crash Durability**: Clear separation of volatile state vs. persistent state (`currentTerm`, `votedFor`, `Log`). A rebooted peer restores from persistent storage and clears volatile indices.

*(Note: In accordance with the lab's educational and verification goals, membership changes, log compaction/snapshots, and lease reads are intentionally excluded).*

---

## 🛡️ Formal Properties Verified

The test suite continuously verifies fundamental Raft safety invariants across multiple fault scenarios and random seeds:

| Property | Description | Verification Method |
| :--- | :--- | :--- |
| **Election Safety** | At most one leader can be elected in a given term ($|Leaders(T)| \le 1$). | Tested across all seeds, including asymmetric network partitions. |
| **Log Matching** | If two logs contain an entry with the same index and term, then the logs are identical up to that index. | Verified post-heal via `s.LogMatchProperty()`. |
| **Leader Completeness** | If a log entry is committed in a given term, it will be present in the logs of the leaders for all higher terms. | Verified across 20 randomized partition/conflict seeds. |
| **State Machine Safety** | If a peer has applied a log entry at an index to its state machine, no other peer will apply a different log entry for that index. | Checked across all applied KV key-value pairs. |
| **Trace Determinism** | Running identical scenarios with identical seeds produces identical SHA-256 event trace digests. | Validated in `TestDeterminism`. |

---

## ⚡ Quick Start

### Prerequisites
- [Go](https://go.dev/) 1.22 or higher.

### 1. Run the Test Suite
Execute the deterministic test suite:
```bash
go test -v ./...
```

Output:
```
=== RUN   TestElection
--- PASS: TestElection (0.00s)
=== RUN   TestLeaderCrash
--- PASS: TestLeaderCrash (0.00s)
=== RUN   TestPartitionMinorityCannotCommit
--- PASS: TestPartitionMinorityCannotCommit (0.00s)
=== RUN   TestAsymmetricNoTwoLeadersSameTerm
--- PASS: TestAsymmetricNoTwoLeadersSameTerm (0.00s)
=== RUN   TestConflictSuffixOverwritten
--- PASS: TestConflictSuffixOverwritten (0.01s)
=== RUN   TestLogMatching
--- PASS: TestLogMatching (0.00s)
=== RUN   TestDeterminism
--- PASS: TestDeterminism (0.00s)
PASS
ok  	raftlab/internal/sim	0.600s
```

### 2. Run a Fault Injection Scenario
Run a scenario simulation from the CLI:
```bash
go run ./cmd/raftlab partition -seed 7
```

CLI Output:
```
scenario=partition seed=7 leader=4 events=2008 trace=traces/partition.json hash=2e61129daa0a
```

---

## 🧪 Simulation Scenarios

| Scenario | Injected Fault Description | Expected Invariant / Outcome |
| :--- | :--- | :--- |
| `election` | Normal operations in a 5-node cluster. | Exactly 1 leader elected; continuous heartbeats prevent spurious elections. |
| `crash` | Active leader is killed mid-stream, then later restarted. | Cluster elects a new leader with a higher term; restarted node joins as follower; previously committed entries survive. |
| `partition` | 2/3 network split (Minority: peers {0,1}, Majority: peers {2,3,4}). | Minority cannot commit writes; majority continues committing; upon partition heal, logs converge and no committed entries are lost. |
| `asymmetric` | Directional packet drop (e.g. Peer A can reach B, but B cannot reach A). | At most one leader elected per term despite asymmetric reachability. |
| `conflict` | Isolated peer receives conflicting speculative client proposals. | Conflicting uncommitted suffix on the isolated peer is safely discarded and replaced with the authoritative log. |

### CLI Options
```bash
go run ./cmd/raftlab [scenario] [-seed N] [-out dir]
```
- `scenario`: `election`, `crash`, `partition`, `asymmetric`, or `conflict` (default: `partition`).
- `-seed`: Random seed (integer, default: `7`).
- `-out`: Directory to write JSON trace file (default: `traces`).

---

## 📊 Visual Trace Inspector

RaftLab includes a zero-dependency web visualizer to inspect the timeline of events, role changes, message deliveries, and partitions.

```
Peer 0 ──[■ Leader]───────────────[▼ Partition]─────────────────────[▲ Heal]──[■ Commit]──
Peer 1 ──[• Follower]─────────────[               ]───────────────────────────[■ Commit]──
Peer 2 ──[• Follower]──[■ Leader]─[               ]──[■ Commit]───────────────[■ Commit]──
Peer 3 ──[• Follower]─────────────[   Majority    ]──[■ Commit]───────────────[■ Commit]──
Peer 4 ──[• Follower]─────────────[               ]──[■ Commit]───────────────[■ Commit]──
```

### How to Use:
1. Open [`ui/index.html`](ui/index.html) in your browser:
   ```bash
   # On macOS
   open ui/index.html
   ```
2. Click **"Choose File"** and select a generated trace from `traces/` (e.g., `traces/partition.json`).
3. Hover over the event marks on each peer's lane to inspect:
   - 🟡 **Role / Election** transitions (`follower` → `candidate` → `leader`)
   - 🟢 **Commit** advancements and current `commitIndex`
   - 🔴 **Partition / Heal** injection markers
   - 🟠 **Crash / Restart** events

---

## 📂 Repository Structure

```
raftlab/
├── cmd/
│   └── raftlab/
│       └── main.go           # CLI entry point (scenario runner & trace generator)
├── internal/
│   ├── machine/
│   │   └── kv.go             # Replicated Key-Value state machine
│   ├── raft/
│   │   └── raft.go           # Core Raft state machine & Figure 2 RPC handling
│   └── sim/
│       ├── sim.go            # Discrete-event simulator, fault injector & trace recorder
│       └── sim_test.go       # Deterministic invariant & property-based tests
├── traces/                   # Pre-generated / output JSON scenario traces
├── ui/
│   └── index.html            # Timeline visualization dashboard
└── README.md                 # Technical specification & project documentation
```

---

## 📚 References

- **Ongaro, D., & Ousterhout, J. (2014).** *In Search of an Understandable Consensus Algorithm.* USENIX Annual Technical Conference (ATC). [Paper PDF](https://raft.github.io/raft.pdf).
- **Raft Consensus Website & Interactive Visualization:** [raft.github.io](https://raft.github.io/).
