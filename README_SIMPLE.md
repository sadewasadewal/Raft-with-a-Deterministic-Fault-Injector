# RaftLab Explained in Plain English 💡
> *A friendly guide to understanding distributed consensus, fault injection, and how this project works — no PhD required.*

---

## 1. What is this project in one sentence?

**RaftLab is a simulated cluster of 5 computers that must agree on a shared list of data, even when network cables are cut, computers crash, or messages get delayed — with 100% predictable, repeatable results.**

---

## 2. The Big Problem: Why is this hard?

Imagine you and 4 friends are managing a shared notebook of bank account balances. 

If you were all in the same room talking face-to-face, it's easy: one person speaks, everyone writes it down.

**In the real world of servers, things break constantly:**
- 💥 **A server crashes** in the middle of writing down an update.
- ✂️ **A network cable gets severed** (a *network partition*), splitting the 5 friends into two separate rooms (say, a group of 2 and a group of 3) where they can't talk to each other.
- 🐌 **Messages get delayed or arrive out of order.**

If the group of 2 people accepted a money deposit of $100, while the group of 3 accepted a deposit of $50, your notebook is now ruined. You have two different realities (called a **"split-brain"**).

**Raft is an algorithm designed by computer scientists in 2014 to solve this exact problem.**

---

## 3. The "5 Friends" Analogy: How Raft Works

Raft keeps everyone in sync by following a few strict rules:

### A. Only One Boss at a Time (The Leader)
- At any time, one server is the **Leader**, and the others are **Followers**.
- Only the Leader is allowed to accept new data from users.
- The Leader sends regular "heartbeats" (little pings saying *"I'm still alive!"*) to all followers.

### B. Democracy & Elections
- If followers stop hearing heartbeats from the Leader (maybe the Leader crashed or got disconnected), they get impatient.
- A follower will raise their hand and say: *"I want to be the new leader! Vote for me!"* (They become a **Candidate**).
- **Rule of Majority (Quorum):** To become the new Leader, a candidate must win votes from a **strict majority** (at least 3 out of 5 friends).

### C. Committing Data (Making it Permanent)
- When a user asks the Leader to save something (like `set balance = 100`):
  1. The Leader writes it down in its own log as a *draft*.
  2. The Leader sends a copy to all followers.
  3. Once a **majority** (at least 3 out of 5) confirms they saved the draft, the Leader marks it as **COMMITTED** (permanent).
  4. Once committed, that data will **never be lost or rewritten**, no matter what happens!

---

## 4. What Happens When Things Go Wrong? (The 5 Scenarios)

This project contains 5 built-in test experiments ("scenarios") that purposely break things to prove the algorithm holds up:

### Scenario 1: `election` (A Peaceful Day)
- **What happens:** 5 healthy nodes boot up. After a brief election timeout, they vote and pick 1 leader. The leader continuously sends heartbeats.
- **The Lesson:** Demonstrates basic leader election and stabilization without any faults.

### Scenario 2: `crash` (The Leader Suddenly Faints)
- **What happens:** Node 0 is the leader and commits key `k = v1`. Suddenly, Node 0 crashes (dies).
- **The Recovery:** The other 4 nodes notice the missing heartbeats. They elect a new leader (say, Node 1) for a higher "term" (election cycle). They commit `k = v2`. Later, the old leader (Node 0) wakes back up. It realizes it is no longer the boss, steps down to follower, and downloads the missing data.
- **The Lesson:** The system automatically survives server deaths, and no committed data is lost.

### Scenario 3: `partition` (The Room Gets Split in Half)
- **What happens:** A network failure isolates 2 nodes (minority: Nodes 0 and 1) from 3 nodes (majority: Nodes 2, 3, and 4).
- **The Magic:**
  - If someone tries to write data to the minority (2 nodes), they **CANNOT commit it** because 2 out of 5 is not a majority!
  - Meanwhile, the majority (3 nodes) can still elect a leader and commit data.
- **The Heal:** When the network cable is plugged back in, the 2 isolated nodes throw away their uncommitted drafts and adopt the majority's true log.
- **The Lesson:** **Split-brain is impossible.** A minority cannot commit rogue data.

### Scenario 4: `asymmetric` (One-Way Telephones)
- **What happens:** A weird network glitch where Node A can send messages to Node B, but Node B cannot send messages back to Node A.
- **The Lesson:** Even under confusing one-way network cuts, Raft guarantees there is **never more than one leader in the same term**.

### Scenario 5: `conflict` (Erasing False Drafts)
- **What happens:** A disconnected node creates uncommitted entries that conflict with the real leader.
- **The Lesson:** Raft safely overwrites and cleans up the conflicting suffix on the isolated peer once it reconnects.

---

## 5. Why is RaftLab Built This Way? (The Secret Sauce)

Normally, testing distributed systems is painful:
- You need Docker or 5 virtual machines.
- Tests use real network sockets and `time.Sleep()`, making tests slow (taking 30–60 seconds).
- Bugs happen randomly and cannot be reproduced ("it only happens once every 100 runs!").

### RaftLab's Smart Solution: Deterministic Simulation
1. **Single Process, Zero Real Sockets:** All 5 "peers" live in the same Go program.
2. **Virtual Time (Zero Waiting):** Instead of waiting 150 milliseconds of real clock time, the simulator jumps directly to the next event on a virtual timeline. A 10-second cluster simulation runs in **0.01 seconds**!
3. **Seeded Randomness:** Every random delay, timeout, and message delivery order is driven by a number called a **Seed** (e.g., `-seed 7`).
   - If you run with `-seed 7` today, tomorrow, or 10 years from now on any computer, **every single event will happen in the exact same microsecond order**.
   - If a bug happens, you can replay it 1,000 times until you fix it!

---

## 6. Project Tour: Which File Does What?

| File / Folder | Purpose in Plain English |
| :--- | :--- |
| [`internal/raft/raft.go`](file:///Users/sandew/Downloads/raftlab/internal/raft/raft.go) | **The Brain.** Implements the core Raft rules (voting, election timeouts, log entries, committing, followers/leaders). |
| [`internal/machine/kv.go`](file:///Users/sandew/Downloads/raftlab/internal/machine/kv.go) | **The Application.** A simple Key-Value dictionary (`set key = value`). When Raft commits an entry, it gets applied here. |
| [`internal/sim/sim.go`](file:///Users/sandew/Downloads/raftlab/internal/sim/sim.go) | **The Puppet Master.** Controls virtual time, delivers messages, cuts network links (partitions), kills nodes, and records every event into a JSON trace. |
| [`cmd/raftlab/main.go`](file:///Users/sandew/Downloads/raftlab/cmd/raftlab/main.go) | **The Command Line Entry Point.** Lets you choose a scenario and a seed, then runs the simulation and outputs the trace. |
| [`ui/index.html`](file:///Users/sandew/Downloads/raftlab/ui/index.html) | **The Visualizer.** An interactive web page where you can see the 5 peers as timeline lanes, complete with color-coded marks for elections, commits, and network partitions. |

---

## 7. How to Try It Yourself Right Now

### Step 1: Run the Automated Tests
Verify all safety rules (log matching, single leader, recovery) pass:
```bash
go test -v ./...
```

### Step 2: Run a Simulation Scenario
Run the famous network partition scenario:
```bash
go run ./cmd/raftlab partition -seed 7
```
*You will see an output like:*
```
scenario=partition seed=7 leader=4 events=2008 trace=traces/partition.json hash=2e61129daa0a
```
Try other scenarios:
```bash
go run ./cmd/raftlab election -seed 42
go run ./cmd/raftlab crash -seed 11
go run ./cmd/raftlab asymmetric -seed 21
go run ./cmd/raftlab conflict -seed 5
```

### Step 3: See It Visually in Your Browser
1. Open the file [`ui/index.html`](file:///Users/sandew/Downloads/raftlab/ui/index.html) in Chrome or Safari:
   ```bash
   open ui/index.html
   ```
2. Click **"Choose File"** and pick `traces/partition.json` (or any other trace file in the `traces/` folder).
3. Hover your mouse over the colored dots on each peer's lane:
   - 🟡 **Yellow-Green dots:** Leader election & role changes.
   - 🟢 **Green dots:** Log entries being committed.
   - 🔴 **Orange vertical lines:** When the network was partitioned and healed.
   - 🟠 **Tan dots:** Node crashes and restarts.

---

## 8. Key Takeaways to Remember
1. **Consensus requires a majority ($N/2 + 1$).** A 5-node cluster can tolerate 2 dead or isolated nodes and keep working smoothly.
2. **Deterministic simulation is a superpower.** By eliminating real time and sockets, distributed systems become easy to test, replay, and debug.
3. **Once committed, always committed.** Raft guarantees that if a majority agreed on a record, it survives any combination of future crashes or partitions.
