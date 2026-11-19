/**
 * RAFT-MESH: True Deterministic 5-Node / 7-Node Raft Consensus Engine
 * Lead Software Engineer & AI Architect: أ. طارق ابوعشي | Tareq Abuashi
 */

export type RaftRole = 'FOLLOWER' | 'CANDIDATE' | 'LEADER' | 'CRASHED';

export type RpcType =
  | 'RequestVote'
  | 'VoteGranted'
  | 'VoteDenied'
  | 'Heartbeat'
  | 'AppendEntries'
  | 'AppendAck';

export type CommandOp = 'SET' | 'INC' | 'CAS';

export interface LogEntry {
  index: number; // 1-based Raft log index
  term: number;
  command: string;
  op: CommandOp;
  key: string;
  value: string;
  expected?: string;
}

export interface RaftNode {
  id: string;
  numId: number;
  role: RaftRole;
  currentTerm: number;
  votedFor: string | null;
  votesReceived: string[];
  electionElapsedMs: number;
  electionTimeoutMs: number;
  heartbeatElapsedMs: number;
  log: LogEntry[];
  commitIndex: number;
  lastApplied: number;
  kvStore: Record<string, string>;
  nextIndex: Record<string, number>;
  matchIndex: Record<string, number>;
  partitionId: number; // 0 = unified, 1 = minority island, 2 = majority island
  x: number;
  y: number;
  pulsePhase: number;
  lastAckMs: number;
}

export interface RpcPacket {
  id: number;
  type: RpcType;
  fromId: string;
  toId: string;
  term: number;
  progress: number; // 0 -> 1
  durationMs: number;
  dropped: boolean;
  dropAt: number; // 0..1 if dropped
  dropReason?: 'PARTITION' | 'PACKET_LOSS' | 'CRASHED';
  label: string;
  // Payload metadata
  lastLogIndex?: number;
  lastLogTerm?: number;
  entries?: LogEntry[];
  prevLogIndex?: number;
  prevLogTerm?: number;
  leaderCommit?: number;
  matchIndex?: number;
  success?: boolean;
}

export interface TelemetryEvent {
  id: number;
  timestamp: string;
  level: 'INFO' | 'SUCCESS' | 'WARN' | 'ERROR';
  source: string;
  message: string;
}

export interface ClientWriteResult {
  accepted: boolean;
  committed: boolean;
  reason: string;
  leaderId: string | null;
  term: number;
  index?: number;
}

export interface RaftCallbacks {
  onLeaderElected?: (leaderId: string, term: number) => void;
  onLogCommitted?: (entry: LogEntry, term: number) => void;
  onNodeCrashed?: (nodeId: string, wasLeader: boolean) => void;
  onClusterHealed?: () => void;
  onEvent?: (event: TelemetryEvent) => void;
}

export class RaftEngine {
  public clusterSize: 5 | 7 = 5;
  public nodes: RaftNode[] = [];
  public packets: RpcPacket[] = [];
  public partitionActive = false;
  public packetLossRate = 0; // 0 or 0.40
  public jitterMs = 0; // 0 or up to 380ms
  public speedMultiplier = 1.0;
  public paused = false;

  private nextPacketId = 1;
  private nextEventId = 1;
  private callbacks: RaftCallbacks;
  private simTimeMs = 0;

  constructor(callbacks: RaftCallbacks = {}) {
    this.callbacks = callbacks;
    this.initCluster(5);
  }

  public get quorumSize(): number {
    return Math.floor(this.clusterSize / 2) + 1;
  }

  private randomElectionTimeout(): number {
    return 1450 + Math.random() * 1250; // 1.45s - 2.70s for clear visual observation
  }

  private formatTimestamp(): string {
    const totalSec = Math.floor(this.simTimeMs / 1000);
    const mins = String(Math.floor(totalSec / 60)).padStart(2, '0');
    const secs = String(totalSec % 60).padStart(2, '0');
    const ms = String(Math.floor(this.simTimeMs % 1000)).padStart(3, '0');
    return `${mins}:${secs}.${ms}`;
  }

  private emitEvent(level: TelemetryEvent['level'], source: string, message: string): void {
    this.callbacks.onEvent?.({
      id: this.nextEventId++,
      timestamp: this.formatTimestamp(),
      level,
      source,
      message,
    });
  }

  public initCluster(size: 5 | 7): void {
    this.clusterSize = size;
    this.nodes = [];
    this.packets = [];
    this.partitionActive = false;
    this.packetLossRate = 0;
    this.jitterMs = 0;
    this.simTimeMs = 0;

    for (let i = 1; i <= size; i++) {
      const id = `N${i}`;
      this.nodes.push({
        id,
        numId: i,
        role: 'FOLLOWER',
        currentTerm: 1,
        votedFor: null,
        votesReceived: [],
        electionElapsedMs: i === 1 ? 1100 : Math.random() * 550, // Encourage N1 or random fast first election
        electionTimeoutMs: this.randomElectionTimeout(),
        heartbeatElapsedMs: 0,
        log: [],
        commitIndex: 0,
        lastApplied: 0,
        kvStore: {},
        nextIndex: {},
        matchIndex: {},
        partitionId: 0,
        x: 0,
        y: 0,
        pulsePhase: Math.random() * Math.PI * 2,
        lastAckMs: 0,
      });
    }

    // Seed initial genesis entry committed by N1 in Term 1 so KV store isn't blank on boot
    this.seedInitialState();
    this.emitEvent('INFO', 'CLUSTER', `Initialized ${size}-node Raft cluster (Quorum=${this.quorumSize})`);
  }

  private seedInitialState(): void {
    const leader = this.nodes[0];
    leader.role = 'LEADER';
    leader.currentTerm = 1;
    leader.votedFor = leader.id;
    leader.votesReceived = this.nodes.map((n) => n.id);
    const genesis: LogEntry = {
      index: 1,
      term: 1,
      command: 'SET cluster=ready',
      op: 'SET',
      key: 'cluster',
      value: 'ready',
    };
    for (const node of this.nodes) {
      node.log.push({ ...genesis });
      node.commitIndex = 1;
      node.lastApplied = 1;
      node.kvStore['cluster'] = 'ready';
      leader.nextIndex[node.id] = 2;
      leader.matchIndex[node.id] = 1;
    }
  }

  public getActiveLeaders(): RaftNode[] {
    return this.nodes.filter((n) => n.role === 'LEADER');
  }

  public getMajorityLeader(): RaftNode | null {
    const leaders = this.getActiveLeaders();
    if (leaders.length === 0) return null;
    if (!this.partitionActive) {
      // Highest term leader
      return leaders.sort((a, b) => b.currentTerm - a.currentTerm)[0];
    }
    // In partition mode, Island 2 is majority
    return leaders.find((l) => l.partitionId === 2) || null;
  }

  public getMinorityLeader(): RaftNode | null {
    if (!this.partitionActive) return null;
    return this.nodes.find((n) => n.role === 'LEADER' && n.partitionId === 1) || null;
  }

  public getHighestTerm(): number {
    return this.nodes.reduce((max, n) => Math.max(max, n.currentTerm), 1);
  }

  public isNodePartitionedInMinority(node: RaftNode): boolean {
    return this.partitionActive && node.partitionId === 1;
  }

  public toggleCrashNode(nodeId: string): void {
    const node = this.nodes.find((n) => n.id === nodeId);
    if (!node) return;
    if (node.role === 'CRASHED') {
      this.reviveNode(node);
    } else {
      this.crashNode(node);
    }
  }

  public crashNode(node: RaftNode): void {
    if (node.role === 'CRASHED') return;
    const wasLeader = node.role === 'LEADER';
    node.role = 'CRASHED';
    node.votesReceived = [];
    this.emitEvent('ERROR', node.id, `${node.id} CRASHED (was ${wasLeader ? 'LEADER' : 'Follower/Candidate'} at T${node.currentTerm})`);
    this.callbacks.onNodeCrashed?.(node.id, wasLeader);
  }

  public reviveNode(node: RaftNode): void {
    if (node.role !== 'CRASHED') return;
    node.role = 'FOLLOWER';
    node.votedFor = null;
    node.votesReceived = [];
    node.electionElapsedMs = 0;
    node.electionTimeoutMs = this.randomElectionTimeout();
    this.emitEvent('SUCCESS', node.id, `${node.id} REVIVED as FOLLOWER (T${node.currentTerm}), awaiting AppendEntries sync`);
  }

  public killCurrentLeader(): boolean {
    const leader = this.getMajorityLeader() || this.getActiveLeaders()[0];
    if (!leader) {
      this.emitEvent('WARN', 'CHAOS', 'No active Leader to kill — election already in progress.');
      return false;
    }
    this.crashNode(leader);
    return true;
  }

  public toggleSplitBrainPartition(): boolean {
    if (this.partitionActive) {
      this.healCluster();
      return false;
    }
    this.partitionActive = true;
    const minorityCutoff = this.clusterSize === 5 ? 2 : 3; // 2 vs 3 (or 3 vs 4)
    for (const node of this.nodes) {
      node.partitionId = node.numId <= minorityCutoff ? 1 : 2;
    }
    const minIds = this.nodes.filter((n) => n.partitionId === 1).map((n) => n.id).join(',');
    const majIds = this.nodes.filter((n) => n.partitionId === 2).map((n) => n.id).join(',');
    this.emitEvent(
      'WARN',
      'CAP-CHAOS',
      `Split-Brain Partition active: Minority [${minIds}] vs Majority [${majIds}]`
    );
    return true;
  }

  public togglePacketLossChaos(): boolean {
    if (this.packetLossRate > 0) {
      this.packetLossRate = 0;
      this.jitterMs = 0;
      this.emitEvent('SUCCESS', 'NET-CHAOS', 'Network packet loss & jitter cleared (0% loss).');
      return false;
    }
    this.packetLossRate = 0.4;
    this.jitterMs = 360;
    this.emitEvent('ERROR', 'NET-CHAOS', 'Injected 40% Packet Loss & 360ms Jitter across cluster links.');
    return true;
  }

  public healCluster(): void {
    this.partitionActive = false;
    this.packetLossRate = 0;
    this.jitterMs = 0;
    for (const node of this.nodes) {
      node.partitionId = 0;
      if (node.role === 'CRASHED') {
        this.reviveNode(node);
      }
    }
    // Trigger immediate heartbeat from highest-term leader so stale minority leader steps down & logs sync
    const leaders = this.getActiveLeaders().sort((a, b) => b.currentTerm - a.currentTerm);
    if (leaders[0]) {
      leaders[0].heartbeatElapsedMs = 9999;
    }
    this.emitEvent('SUCCESS', 'HEAL', 'Cluster healed: partitions removed, nodes revived, log sync initiated.');
    this.callbacks.onClusterHealed?.();
  }

  /**
   * Parse and submit a client command (`SET k=v`, `INC k`, `CAS k old new`)
   */
  public submitClientCommand(rawInput: string, targetMinority = false): ClientWriteResult {
    const parsed = this.parseCommand(rawInput);
    if (!parsed) {
      return {
        accepted: false,
        committed: false,
        reason: 'Invalid syntax. Use SET key=val, INC key, or CAS key expected newVal',
        leaderId: null,
        term: this.getHighestTerm(),
      };
    }

    const leader = targetMinority ? this.getMinorityLeader() : this.getMajorityLeader();
    if (!leader) {
      const anyMinority = this.getMinorityLeader();
      if (anyMinority && !targetMinority) {
        // Still allow demonstrating minority write rejection if user tries while majority has no leader yet
        return {
          accepted: false,
          committed: false,
          reason: `ERR_NO_QUORUM: Majority partition has no elected Leader yet (CP safety enforced).`,
          leaderId: null,
          term: this.getHighestTerm(),
        };
      }
      return {
        accepted: false,
        committed: false,
        reason: targetMinority
          ? 'Minority island has no stale Leader to test.'
          : 'ERR_NO_LEADER: Election in progress, client write rejected (CP).',
        leaderId: null,
        term: this.getHighestTerm(),
      };
    }

    const newIndex = leader.log.length + 1;
    const entry: LogEntry = {
      index: newIndex,
      term: leader.currentTerm,
      command: parsed.canonical,
      op: parsed.op,
      key: parsed.key,
      value: parsed.value,
      expected: parsed.expected,
    };

    leader.log.push(entry);
    leader.matchIndex[leader.id] = newIndex;
    leader.nextIndex[leader.id] = newIndex + 1;

    // Immediately broadcast AppendEntries
    this.broadcastAppendEntries(leader);

    if (this.partitionActive && leader.partitionId === 1) {
      this.emitEvent(
        'WARN',
        leader.id,
        `Minority Write [${entry.command}] appended at idx #${newIndex} (UNCOMMITTED — lacks Quorum ${this.quorumSize})`
      );
      return {
        accepted: true,
        committed: false,
        reason: `Appended to minority ${leader.id} idx #${newIndex}, STALLED (cannot reach Quorum ${this.quorumSize})`,
        leaderId: leader.id,
        term: leader.currentTerm,
        index: newIndex,
      };
    }

    this.emitEvent('INFO', leader.id, `Client Write [${entry.command}] replicating at idx #${newIndex} (T${leader.currentTerm})`);
    return {
      accepted: true,
      committed: false,
      reason: `Replicating idx #${newIndex} on Leader ${leader.id} (T${leader.currentTerm})`,
      leaderId: leader.id,
      term: leader.currentTerm,
      index: newIndex,
    };
  }

  private parseCommand(raw: string): {
    op: CommandOp;
    key: string;
    value: string;
    expected?: string;
    canonical: string;
  } | null {
    const trimmed = raw.trim();
    if (!trimmed) return null;
    const parts = trimmed.split(/\s+/);
    const verb = parts[0].toUpperCase();

    if (verb === 'SET') {
      const rest = trimmed.slice(3).trim();
      if (rest.includes('=')) {
        const eqIdx = rest.indexOf('=');
        const key = rest.slice(0, eqIdx).trim();
        const value = rest.slice(eqIdx + 1).trim();
        if (!key || !value) return null;
        return { op: 'SET', key, value, canonical: `SET ${key}=${value}` };
      }
      if (parts.length >= 3) {
        const key = parts[1];
        const value = parts.slice(2).join(' ');
        return { op: 'SET', key, value, canonical: `SET ${key}=${value}` };
      }
      return null;
    }

    if (verb === 'INC' && parts.length >= 2) {
      const key = parts[1];
      return { op: 'INC', key, value: '1', canonical: `INC ${key}` };
    }

    if (verb === 'CAS' && parts.length >= 4) {
      const key = parts[1];
      const expected = parts[2];
      const value = parts[3];
      return { op: 'CAS', key, value, expected, canonical: `CAS ${key} ${expected} ${value}` };
    }

    // Fallback: allow shorthand key=val
    if (trimmed.includes('=')) {
      const eqIdx = trimmed.indexOf('=');
      const key = trimmed.slice(0, eqIdx).trim();
      const value = trimmed.slice(eqIdx + 1).trim();
      if (key && value) {
        return { op: 'SET', key, value, canonical: `SET ${key}=${value}` };
      }
    }

    return null;
  }

  public step(rawDtMs: number): void {
    if (this.paused) return;
    const dt = Math.min(rawDtMs, 50) * this.speedMultiplier;
    this.simTimeMs += dt;

    // 1. Update node timers
    for (const node of this.nodes) {
      node.pulsePhase += dt * 0.004;
      if (node.role === 'CRASHED') continue;

      if (node.role === 'LEADER') {
        node.heartbeatElapsedMs += dt;
        if (node.heartbeatElapsedMs >= 420) {
          node.heartbeatElapsedMs = 0;
          this.broadcastAppendEntries(node);
        }
      } else {
        node.electionElapsedMs += dt;
        if (node.electionElapsedMs >= node.electionTimeoutMs) {
          this.startElection(node);
        }
      }
    }

    // 2. Advance RPC packets in flight
    const remaining: RpcPacket[] = [];
    for (const pkt of this.packets) {
      pkt.progress += dt / pkt.durationMs;

      if (pkt.dropped && pkt.progress >= pkt.dropAt) {
        // Packet drops mid-flight — do not deliver
        continue;
      }

      if (pkt.progress >= 1) {
        this.deliverPacket(pkt);
      } else {
        remaining.push(pkt);
      }
    }
    this.packets = remaining;
  }

  private startElection(candidate: RaftNode): void {
    candidate.role = 'CANDIDATE';
    candidate.currentTerm += 1;
    candidate.votedFor = candidate.id;
    candidate.votesReceived = [candidate.id];
    candidate.electionElapsedMs = 0;
    candidate.electionTimeoutMs = this.randomElectionTimeout();

    const lastLogIndex = candidate.log.length;
    const lastLogTerm = lastLogIndex > 0 ? candidate.log[lastLogIndex - 1].term : 0;

    this.emitEvent('WARN', candidate.id, `Election timeout! ${candidate.id} started election for Term T${candidate.currentTerm}`);

    for (const peer of this.nodes) {
      if (peer.id === candidate.id) continue;
      this.sendRpc({
        type: 'RequestVote',
        from: candidate,
        to: peer,
        term: candidate.currentTerm,
        label: `ReqVote T${candidate.currentTerm}`,
        lastLogIndex,
        lastLogTerm,
      });
    }
  }

  private broadcastAppendEntries(leader: RaftNode): void {
    leader.heartbeatElapsedMs = 0;
    for (const peer of this.nodes) {
      if (peer.id === leader.id) continue;

      const nextIdx = leader.nextIndex[peer.id] ?? leader.log.length + 1;
      const prevLogIndex = Math.max(0, nextIdx - 1);
      const prevLogTerm = prevLogIndex > 0 && leader.log[prevLogIndex - 1]
        ? leader.log[prevLogIndex - 1].term
        : 0;
      const entries = leader.log.slice(prevLogIndex).map((e) => ({ ...e }));
      const isReplication = entries.length > 0;

      this.sendRpc({
        type: isReplication ? 'AppendEntries' : 'Heartbeat',
        from: leader,
        to: peer,
        term: leader.currentTerm,
        label: isReplication ? `Append #${entries[0].index}..#${entries[entries.length - 1].index}` : `HB T${leader.currentTerm}`,
        prevLogIndex,
        prevLogTerm,
        entries,
        leaderCommit: leader.commitIndex,
      });
    }
  }

  private sendRpc(opts: {
    type: RpcType;
    from: RaftNode;
    to: RaftNode;
    term: number;
    label: string;
    lastLogIndex?: number;
    lastLogTerm?: number;
    entries?: LogEntry[];
    prevLogIndex?: number;
    prevLogTerm?: number;
    leaderCommit?: number;
    matchIndex?: number;
    success?: boolean;
  }): void {
    let dropped = false;
    let dropAt = 1;
    let dropReason: RpcPacket['dropReason'];

    if (this.partitionActive && opts.from.partitionId !== opts.to.partitionId) {
      dropped = true;
      dropAt = 0.48;
      dropReason = 'PARTITION';
    } else if (opts.to.role === 'CRASHED') {
      dropped = true;
      dropAt = 0.85;
      dropReason = 'CRASHED';
    } else if (this.packetLossRate > 0 && Math.random() < this.packetLossRate) {
      dropped = true;
      dropAt = 0.35 + Math.random() * 0.45;
      dropReason = 'PACKET_LOSS';
    }

    const baseDuration = opts.type === 'Heartbeat' ? 230 : 270;
    const jitter = this.jitterMs > 0 ? Math.random() * this.jitterMs : Math.random() * 35;

    this.packets.push({
      id: this.nextPacketId++,
      type: opts.type,
      fromId: opts.from.id,
      toId: opts.to.id,
      term: opts.term,
      progress: 0,
      durationMs: baseDuration + jitter,
      dropped,
      dropAt,
      dropReason,
      label: opts.label,
      lastLogIndex: opts.lastLogIndex,
      lastLogTerm: opts.lastLogTerm,
      entries: opts.entries,
      prevLogIndex: opts.prevLogIndex,
      prevLogTerm: opts.prevLogTerm,
      leaderCommit: opts.leaderCommit,
      matchIndex: opts.matchIndex,
      success: opts.success,
    });
  }

  private deliverPacket(pkt: RpcPacket): void {
    const receiver = this.nodes.find((n) => n.id === pkt.toId);
    const sender = this.nodes.find((n) => n.id === pkt.fromId);
    if (!receiver || !sender || receiver.role === 'CRASHED') return;

    // Step down if higher term observed
    if (pkt.term > receiver.currentTerm) {
      const wasLeader = receiver.role === 'LEADER';
      receiver.currentTerm = pkt.term;
      receiver.role = 'FOLLOWER';
      receiver.votedFor = null;
      receiver.votesReceived = [];
      if (wasLeader) {
        this.emitEvent('WARN', receiver.id, `${receiver.id} stepped down to FOLLOWER on higher Term T${pkt.term}`);
      }
    }

    switch (pkt.type) {
      case 'RequestVote':
        this.handleRequestVote(receiver, sender, pkt);
        break;
      case 'VoteGranted':
        this.handleVoteGranted(receiver, sender, pkt);
        break;
      case 'VoteDenied':
        break;
      case 'Heartbeat':
      case 'AppendEntries':
        this.handleAppendEntries(receiver, sender, pkt);
        break;
      case 'AppendAck':
        this.handleAppendAck(receiver, sender, pkt);
        break;
    }
  }

  private handleRequestVote(receiver: RaftNode, candidate: RaftNode, pkt: RpcPacket): void {
    if (pkt.term < receiver.currentTerm) {
      this.sendRpc({
        type: 'VoteDenied',
        from: receiver,
        to: candidate,
        term: receiver.currentTerm,
        label: `Deny T${receiver.currentTerm}`,
      });
      return;
    }

    const myLastIndex = receiver.log.length;
    const myLastTerm = myLastIndex > 0 ? receiver.log[myLastIndex - 1].term : 0;
    const candLastTerm = pkt.lastLogTerm ?? 0;
    const candLastIndex = pkt.lastLogIndex ?? 0;

    const logIsUpToDate =
      candLastTerm > myLastTerm || (candLastTerm === myLastTerm && candLastIndex >= myLastIndex);

    const canVote =
      (receiver.votedFor === null || receiver.votedFor === candidate.id) && logIsUpToDate;

    if (canVote) {
      receiver.votedFor = candidate.id;
      receiver.electionElapsedMs = 0;
      this.sendRpc({
        type: 'VoteGranted',
        from: receiver,
        to: candidate,
        term: receiver.currentTerm,
        label: `Vote -> ${candidate.id}`,
      });
    } else {
      this.sendRpc({
        type: 'VoteDenied',
        from: receiver,
        to: candidate,
        term: receiver.currentTerm,
        label: `Deny`,
      });
    }
  }

  private handleVoteGranted(candidate: RaftNode, voter: RaftNode, pkt: RpcPacket): void {
    if (candidate.role !== 'CANDIDATE' || pkt.term !== candidate.currentTerm) return;
    if (!candidate.votesReceived.includes(voter.id)) {
      candidate.votesReceived.push(voter.id);
    }

    if (candidate.votesReceived.length >= this.quorumSize) {
      candidate.role = 'LEADER';
      candidate.heartbeatElapsedMs = 0;
      for (const peer of this.nodes) {
        candidate.nextIndex[peer.id] = candidate.log.length + 1;
        candidate.matchIndex[peer.id] = peer.id === candidate.id ? candidate.log.length : 0;
      }
      this.emitEvent(
        'SUCCESS',
        candidate.id,
        `${candidate.id} ELECTED LEADER for Term T${candidate.currentTerm} (${candidate.votesReceived.length}/${this.clusterSize} votes)`
      );
      this.callbacks.onLeaderElected?.(candidate.id, candidate.currentTerm);
      this.broadcastAppendEntries(candidate);
    }
  }

  private handleAppendEntries(follower: RaftNode, leader: RaftNode, pkt: RpcPacket): void {
    if (pkt.term < follower.currentTerm) {
      this.sendRpc({
        type: 'AppendAck',
        from: follower,
        to: leader,
        term: follower.currentTerm,
        label: 'Reject stale',
        success: false,
        matchIndex: 0,
      });
      return;
    }

    // Valid leader contact: reset election timer and ensure follower state
    follower.role = 'FOLLOWER';
    follower.electionElapsedMs = 0;
    follower.lastAckMs = this.simTimeMs;

    const prevIdx = pkt.prevLogIndex ?? 0;
    const prevTerm = pkt.prevLogTerm ?? 0;

    // Log Matching check
    if (prevIdx > 0) {
      if (follower.log.length < prevIdx || follower.log[prevIdx - 1].term !== prevTerm) {
        // Conflict: decrement nextIndex via unsuccessful ack
        this.sendRpc({
          type: 'AppendAck',
          from: follower,
          to: leader,
          term: follower.currentTerm,
          label: 'LogMismatch',
          success: false,
          matchIndex: Math.min(follower.log.length, Math.max(0, prevIdx - 1)),
        });
        return;
      }
    }

    const entries = pkt.entries ?? [];
    if (entries.length > 0) {
      // Truncate any conflicting suffix (e.g. uncommitted minority log after split-brain heal!)
      let insertAt = prevIdx;
      for (const entry of entries) {
        if (follower.log.length > insertAt) {
          if (follower.log[insertAt].term !== entry.term) {
            follower.log = follower.log.slice(0, insertAt);
            follower.log.push({ ...entry });
          }
        } else {
          follower.log.push({ ...entry });
        }
        insertAt++;
      }
    }

    const leaderCommit = pkt.leaderCommit ?? 0;
    if (leaderCommit > follower.commitIndex) {
      follower.commitIndex = Math.min(leaderCommit, follower.log.length);
      this.applyCommittedEntries(follower);
    }

    if (entries.length > 0 || pkt.type === 'AppendEntries') {
      this.sendRpc({
        type: 'AppendAck',
        from: follower,
        to: leader,
        term: follower.currentTerm,
        label: `Ack #${follower.log.length}`,
        success: true,
        matchIndex: follower.log.length,
      });
    }
  }

  private handleAppendAck(leader: RaftNode, follower: RaftNode, pkt: RpcPacket): void {
    if (leader.role !== 'LEADER' || pkt.term !== leader.currentTerm) return;

    if (!pkt.success) {
      const currentNext = leader.nextIndex[follower.id] ?? 1;
      leader.nextIndex[follower.id] = Math.max(1, currentNext - 1);
      return;
    }

    const matchIdx = pkt.matchIndex ?? 0;
    leader.matchIndex[follower.id] = Math.max(leader.matchIndex[follower.id] ?? 0, matchIdx);
    leader.nextIndex[follower.id] = leader.matchIndex[follower.id] + 1;

    // Advance commitIndex if a majority has replicated an entry from currentTerm
    for (let idx = leader.log.length; idx > leader.commitIndex; idx--) {
      if (leader.log[idx - 1].term !== leader.currentTerm) continue;
      let replicatedCount = 1; // Leader itself
      for (const peer of this.nodes) {
        if (peer.id === leader.id || peer.role === 'CRASHED') continue;
        if (this.partitionActive && peer.partitionId !== leader.partitionId) continue;
        if ((leader.matchIndex[peer.id] ?? 0) >= idx) {
          replicatedCount++;
        }
      }
      if (replicatedCount >= this.quorumSize) {
        const oldCommit = leader.commitIndex;
        leader.commitIndex = idx;
        this.applyCommittedEntries(leader);
        for (let c = oldCommit + 1; c <= idx; c++) {
          const committedEntry = leader.log[c - 1];
          this.emitEvent(
            'SUCCESS',
            'QUORUM',
            `Committed #${c} [${committedEntry.command}] (${replicatedCount}/${this.clusterSize} acks)`
          );
          this.callbacks.onLogCommitted?.(committedEntry, leader.currentTerm);
        }
        break;
      }
    }
  }

  private applyCommittedEntries(node: RaftNode): void {
    while (node.lastApplied < node.commitIndex && node.lastApplied < node.log.length) {
      node.lastApplied++;
      const entry = node.log[node.lastApplied - 1];
      this.applyToStateMachine(node.kvStore, entry);
    }
  }

  private applyToStateMachine(store: Record<string, string>, entry: LogEntry): void {
    switch (entry.op) {
      case 'SET':
        store[entry.key] = entry.value;
        break;
      case 'INC': {
        const prev = Number(store[entry.key] ?? 0);
        store[entry.key] = Number.isFinite(prev) ? String(prev + 1) : '1';
        break;
      }
      case 'CAS': {
        if (store[entry.key] === entry.expected) {
          store[entry.key] = entry.value;
        }
        break;
      }
    }
  }
}
