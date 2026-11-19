# RAFT-MESH: Distributed Raft Consensus, Consistent Hashing & Microservices Chaos Simulator

> **Lead Software Engineer & AI Architect:** **أ. طارق ابوعشي | Tareq Abuashi — Lead Software Engineer & AI Architect**

**RAFT-MESH** is a deterministic, 60fps real-time distributed systems telemetry & chaos engineering laboratory built in TypeScript and HTML5 Canvas 2D. It models Raft consensus invariants, MurmurHash3 consistent hashing with 300+ virtual nodes, a multi-stage microservices gateway pipeline protected by a Circuit Breaker FSM and Token Bucket rate limiter, and interactive CAP theorem fault injection.

---

## Core Architectural Pillars

### 1. Deterministic 5-Node / 7-Node Raft Consensus Engine (`src/engine/raftEngine.ts`)
- **Dynamic Cluster Topologies:** Switch seamlessly between **5-Node (`Q = 3`, $f = 2$)** and **7-Node (`Q = 4`, $f = 3$)** clusters.
- **True Raft State Machine:** Nodes transition deterministically across `FOLLOWER`, `CANDIDATE`, `LEADER`, `CRASHED`, and `PARTITIONED` states driven by randomized election timeouts ($1450\text{ ms} - 2700\text{ ms}$) and periodic `AppendEntries` heartbeats ($420\text{ ms}$).
- **Live Flying RPC Packets:** Real-time packet animation across mesh links for `RequestVote`, `VoteGranted`, `VoteDenied`, `Heartbeat`, `AppendEntries`, and `AppendAck`.
- **Direct Interactive Faults:** Click any node directly on the Canvas or log inspector to **Crash** or **Revive** it on the fly.

### 2. Replicated Log & Deterministic KV State Machine
- **Client Command Console:** Execute `SET key=val`, `INC key`, `CAS key expected newVal`, or engage **Auto-Write Burst**.
- **Commit vs. Uncommitted Visibility:** Per-node `log[]` and `commitIndex` clearly distinguish uncommitted dashed entries from quorum-committed solid entries, updating the converged **State Machine KV Store**.
- **Log Matching & Catch-Up:** Revived or healed nodes automatically reconcile `prevLogIndex`/`prevLogTerm` via `nextIndex`/`matchIndex`, truncating stale minority suffixes and catching up to the Leader's `commitIndex`.

### 3. Consistent Hashing Ring & Microservices Resilience (`src/engine/meshEngine.ts`)
- **MurmurHash3_32 Unit Ring:** Projects **320 virtual nodes** (5-node mode) or **448 virtual nodes** (7-node mode) onto $[0, 2^{32}-1)$ with $O(\log(N \cdot V))$ binary search routing and live clockwise shard selection.
- **Microservices Pipeline:** End-to-end animated request flow across **`API Gateway → Auth Service → Order Service → Raft Ledger`**.
- **Circuit Breaker FSM:** Authentic 3-state machine (`CLOSED → OPEN → HALF-OPEN → CLOSED`) monitoring sliding-window error rates ($\ge 42\%$), fail-fast `HTTP 503` shedding, cooldown recovery, and `3`-probe validation.
- **Token Bucket Rate Limiter:** Continuous admission control ($C = 28$ tokens, $r = 16\text{ req/s}$) shedding excess burst load with `HTTP 429`.
- **Live Telemetry Chart:** 60fps dual-trace Canvas graph plotting **`P99 Latency (ms)`** and **`Throughput (RPS)`**.

### 4. Chaos Engineering & CAP Theorem Lab
- **Kill Current Leader:** Crashes the active Leader immediately to observe election timeout expiration and majority vote re-election.
- **Split-Brain Partition (`2 vs 3` / `3 vs 4`):** Severs network links into Minority and Majority islands, proving **CP** semantics (`Minority` fails quorum writes while `Majority` elects a higher-term Leader and continues committing).
- **Inject 40% Packet Loss & Jitter:** Degrades network links, spiking P99 latency and naturally tripping the `Circuit Breaker` to `OPEN`.
- **Heal Cluster & Sync Logs:** Reconnects islands, clears packet loss, revives crashed nodes, forces stale minority leaders to step down on higher `Term`, and converges logs across all nodes.

### 5. Web Audio Telemetry & Distributed Systems Dossier
- Synthesized Web Audio cues for leader elections, quorum log commits, circuit breaker trips, and cluster recovery (with mute toggle).
- Interactive **Systems Dossier Modal** detailing Raft safety invariants, CAP/PACELC tradeoffs, Consistent Hashing math, and Resilience FSM thresholds.

---

## Build & Local Development

```bash
# Typecheck and build production bundle (outputs ./dist with ./assets/ relative paths)
npm run build

# Start Vite dev server
npm run dev
```

---

**Architected & Engineered by:** **أ. طارق ابوعشي | Tareq Abuashi — Lead Software Engineer & AI Architect**
