/**
 * RAFT-MESH: Consistent Hashing Ring (300+ VNodes), Circuit Breaker FSM & Token Bucket Engine
 * Lead Software Engineer & AI Architect: أ. طارق ابوعشي | Tareq Abuashi
 */

import type { RaftNode, TelemetryEvent } from './raftEngine';

export type BreakerState = 'CLOSED' | 'OPEN' | 'HALF-OPEN';

export interface VirtualNode {
  hash: number;
  angle: number; // 0..2π
  nodeId: string;
  vnodeIndex: number;
}

export interface RoutedKeyTrace {
  id: number;
  key: string;
  hash: number;
  angle: number;
  targetNodeId: string;
  targetAngle: number;
  ttl: number; // 1 -> 0 fade
}

export interface MicroservicePacket {
  id: number;
  stage: number; // 0..3 across [Gateway, Auth, Order, Raft Ledger]
  progress: number; // 0..1 across full pipeline
  status: 'IN_FLIGHT' | 'OK' | 'SHED_429' | 'OPEN_503' | 'TIMEOUT_504';
  latencyMs: number;
  key: string;
  targetShard: string;
}

export interface TelemetryPoint {
  p99Ms: number;
  rps: number;
  errorPct: number;
}

export interface MeshCallbacks {
  onBreakerChange?: (prev: BreakerState, next: BreakerState, reason: string) => void;
  onEvent?: (event: TelemetryEvent) => void;
}

/**
 * Deterministic MurmurHash3 (32-bit) for Consistent Hashing Ring projection
 */
export function murmur3_32(key: string, seed = 0x9747b28c): number {
  let h = seed >>> 0;
  const len = key.length;
  let i = 0;

  while (i + 4 <= len) {
    let k =
      (key.charCodeAt(i) & 0xff) |
      ((key.charCodeAt(i + 1) & 0xff) << 8) |
      ((key.charCodeAt(i + 2) & 0xff) << 16) |
      ((key.charCodeAt(i + 3) & 0xff) << 24);

    k = Math.imul(k, 0xcc9e2d51);
    k = (k << 15) | (k >>> 17);
    k = Math.imul(k, 0x1b873593);

    h ^= k;
    h = (h << 13) | (h >>> 19);
    h = (Math.imul(h, 5) + 0xe6546b64) >>> 0;
    i += 4;
  }

  let k1 = 0;
  switch (len & 3) {
    case 3:
      k1 ^= (key.charCodeAt(i + 2) & 0xff) << 16;
    // falls through
    case 2:
      k1 ^= (key.charCodeAt(i + 1) & 0xff) << 8;
    // falls through
    case 1:
      k1 ^= key.charCodeAt(i) & 0xff;
      k1 = Math.imul(k1, 0xcc9e2d51);
      k1 = (k1 << 15) | (k1 >>> 17);
      k1 = Math.imul(k1, 0x1b873593);
      h ^= k1;
  }

  h ^= len;
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;

  return h >>> 0;
}

const SAMPLE_KEY_PREFIXES = ['order', 'auth', 'ledger', 'session', 'cart', 'tx', 'shard', 'acct'];

export class MeshEngine {
  public vnodesPerNode = 64; // 5*64 = 320 vnodes, 7*64 = 448 vnodes (>= 300)
  public ring: VirtualNode[] = [];
  public routedTraces: RoutedKeyTrace[] = [];
  public shardCounts: Record<string, number> = {};

  // Token Bucket Rate Limiter
  public bucketCapacity = 28;
  public bucketTokens = 28;
  public bucketRefillPerSec = 16;
  public totalAllowed = 0;
  public totalShed429 = 0;

  // Circuit Breaker FSM
  public breakerState: BreakerState = 'CLOSED';
  public breakerCooldownMs = 4200;
  public breakerOpenElapsedMs = 0;
  public halfOpenSuccesses = 0;
  public readonly halfOpenSuccessThreshold = 3;
  public readonly failureRateThreshold = 0.42;
  private recentOutcomes: boolean[] = []; // true = success, false = fail
  private readonly outcomeWindow = 18;

  // Microservice traffic & latency telemetry
  public servicePackets: MicroservicePacket[] = [];
  public telemetryHistory: TelemetryPoint[] = [];
  public currentP99Ms = 14;
  public currentRps = 12;
  public currentErrorPct = 0;
  public autoBurst = false;

  private trafficAccumMs = 0;
  private sampleAccumMs = 0;
  private windowReqCount = 0;
  private windowErrorCount = 0;
  private windowLatencies: number[] = [];
  private nextTraceId = 1;
  private nextSvcPktId = 1;
  private nextEventId = 9000;
  private callbacks: MeshCallbacks;

  constructor(callbacks: MeshCallbacks = {}) {
    this.callbacks = callbacks;
    for (let i = 0; i < 48; i++) {
      this.telemetryHistory.push({ p99Ms: 14, rps: 10, errorPct: 0 });
    }
  }

  public rebuildRing(nodes: RaftNode[]): void {
    const vnodes: VirtualNode[] = [];
    this.shardCounts = {};

    for (const node of nodes) {
      this.shardCounts[node.id] = 0;
      for (let v = 0; v < this.vnodesPerNode; v++) {
        const hash = murmur3_32(`${node.id}#vnode-${v}`);
        const angle = (hash / 0xffffffff) * Math.PI * 2;
        vnodes.push({
          hash,
          angle,
          nodeId: node.id,
          vnodeIndex: v,
        });
      }
    }

    vnodes.sort((a, b) => a.hash - b.hash);
    this.ring = vnodes;
  }

  /**
   * Binary search clockwise on Consistent Hash Ring skipping CRASHED nodes
   */
  public routeKey(key: string, nodes: RaftNode[]): { nodeId: string; hash: number; angle: number; vnodeAngle: number } | null {
    if (this.ring.length === 0) return null;
    const activeSet = new Set(nodes.filter((n) => n.role !== 'CRASHED').map((n) => n.id));
    if (activeSet.size === 0) return null;

    const hash = murmur3_32(key);
    const keyAngle = (hash / 0xffffffff) * Math.PI * 2;

    // Binary search for first vnode with vnode.hash >= hash
    let lo = 0;
    let hi = this.ring.length - 1;
    let startIdx = 0;
    while (lo <= hi) {
      const mid = (lo + hi) >>> 1;
      if (this.ring[mid].hash >= hash) {
        startIdx = mid;
        hi = mid - 1;
      } else {
        lo = mid + 1;
      }
    }

    // Walk clockwise until we hit a live shard
    for (let offset = 0; offset < this.ring.length; offset++) {
      const candidate = this.ring[(startIdx + offset) % this.ring.length];
      if (activeSet.has(candidate.nodeId)) {
        this.shardCounts[candidate.nodeId] = (this.shardCounts[candidate.nodeId] || 0) + 1;
        this.routedTraces.push({
          id: this.nextTraceId++,
          key,
          hash,
          angle: keyAngle,
          targetNodeId: candidate.nodeId,
          targetAngle: candidate.angle,
          ttl: 1.0,
        });
        if (this.routedTraces.length > 18) {
          this.routedTraces.shift();
        }
        return {
          nodeId: candidate.nodeId,
          hash,
          angle: keyAngle,
          vnodeAngle: candidate.angle,
        };
      }
    }
    return null;
  }

  public getFailureRate(): number {
    if (this.recentOutcomes.length === 0) return 0;
    const fails = this.recentOutcomes.filter((ok) => !ok).length;
    return fails / this.recentOutcomes.length;
  }

  public resetResilience(): void {
    const prev = this.breakerState;
    this.breakerState = 'CLOSED';
    this.breakerOpenElapsedMs = 0;
    this.halfOpenSuccesses = 0;
    this.recentOutcomes = [];
    this.bucketTokens = this.bucketCapacity;
    if (prev !== 'CLOSED') {
      this.callbacks.onBreakerChange?.(prev, 'CLOSED', 'Manual cluster heal reset');
    }
  }

  public step(
    dtMs: number,
    nodes: RaftNode[],
    opts: {
      packetLossRate: number;
      jitterMs: number;
      hasMajorityLeader: boolean;
      paused: boolean;
      speedMultiplier: number;
    }
  ): void {
    if (opts.paused) return;
    const dt = Math.min(dtMs, 50) * opts.speedMultiplier;

    // 1. Fade consistent hash traces
    for (const trace of this.routedTraces) {
      trace.ttl = Math.max(0, trace.ttl - dt / 1800);
    }
    this.routedTraces = this.routedTraces.filter((t) => t.ttl > 0);

    // 2. Refill Token Bucket
    this.bucketTokens = Math.min(
      this.bucketCapacity,
      this.bucketTokens + (this.bucketRefillPerSec * dt) / 1000
    );

    // 3. Circuit Breaker OPEN -> HALF-OPEN cooldown
    if (this.breakerState === 'OPEN') {
      this.breakerOpenElapsedMs += dt;
      if (this.breakerOpenElapsedMs >= this.breakerCooldownMs) {
        this.transitionBreaker('HALF-OPEN', 'Cooldown elapsed; sending recovery probes');
        this.halfOpenSuccesses = 0;
      }
    }

    // 4. Generate steady gateway traffic (or high-pressure Auto-Write Burst)
    const reqIntervalMs = this.autoBurst ? 42 : 120;
    this.trafficAccumMs += dt;
    while (this.trafficAccumMs >= reqIntervalMs) {
      this.trafficAccumMs -= reqIntervalMs;
      this.dispatchGatewayRequest(nodes, opts);
    }

    // 5. Advance Microservice Pipeline packets
    const nextPackets: MicroservicePacket[] = [];
    for (const pkt of this.servicePackets) {
      const speed = pkt.status === 'IN_FLIGHT' ? 0.00145 : 0.0022;
      pkt.progress += dt * speed;
      pkt.stage = Math.min(3, Math.floor(pkt.progress * 4));
      if (pkt.progress < 1) {
        nextPackets.push(pkt);
      }
    }
    this.servicePackets = nextPackets;

    // 6. Compute rolling 250ms P99 & RPS telemetry window
    this.sampleAccumMs += dt;
    if (this.sampleAccumMs >= 250) {
      const windowSec = this.sampleAccumMs / 1000;
      this.sampleAccumMs = 0;

      const rps = Math.round(this.windowReqCount / Math.max(0.1, windowSec));
      let p99 = 12;
      if (this.windowLatencies.length > 0) {
        const sorted = [...this.windowLatencies].sort((a, b) => a - b);
        const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * 0.99));
        p99 = Math.round(sorted[idx]);
      }
      const errPct =
        this.windowReqCount > 0 ? Math.round((this.windowErrorCount / this.windowReqCount) * 100) : 0;

      this.currentRps = rps;
      this.currentP99Ms = p99;
      this.currentErrorPct = errPct;

      this.telemetryHistory.push({ p99Ms: p99, rps, errorPct: errPct });
      if (this.telemetryHistory.length > 48) {
        this.telemetryHistory.shift();
      }

      this.windowReqCount = 0;
      this.windowErrorCount = 0;
      this.windowLatencies = [];
    }
  }

  public dispatchGatewayRequest(
    nodes: RaftNode[],
    opts: { packetLossRate: number; jitterMs: number; hasMajorityLeader: boolean },
    explicitKey?: string
  ): MicroservicePacket['status'] {
    const key =
      explicitKey ??
      `${SAMPLE_KEY_PREFIXES[Math.floor(Math.random() * SAMPLE_KEY_PREFIXES.length)]}:${Math.floor(
        1000 + Math.random() * 8999
      )}`;

    const routed = this.routeKey(key, nodes);
    const targetShard = routed?.nodeId ?? 'NONE';
    this.windowReqCount++;

    // Step A: Token Bucket Rate Limiter check
    if (this.bucketTokens < 1) {
      this.totalShed429++;
      this.windowErrorCount++;
      this.windowLatencies.push(4);
      this.pushPipelinePacket(key, targetShard, 'SHED_429', 4);
      return 'SHED_429';
    }
    this.bucketTokens -= 1;
    this.totalAllowed++;

    // Step B: Circuit Breaker Fail-Fast check
    if (this.breakerState === 'OPEN') {
      this.windowErrorCount++;
      this.windowLatencies.push(2);
      this.pushPipelinePacket(key, targetShard, 'OPEN_503', 2);
      return 'OPEN_503';
    }

    // Step C: Downstream Auth -> Order -> Raft Ledger execution
    const networkFailure = opts.packetLossRate > 0 && Math.random() < opts.packetLossRate * 1.35;
    const consensusUnavailable = !opts.hasMajorityLeader;
    const failed = networkFailure || consensusUnavailable || targetShard === 'NONE';

    const baseLatency = 8 + Math.random() * 12;
    const jitterExtra = opts.jitterMs > 0 ? 140 + Math.random() * (opts.jitterMs * 1.45) : 0;
    const latencyMs = Math.round(baseLatency + jitterExtra + (failed ? 260 : 0));

    this.windowLatencies.push(latencyMs);

    if (failed) {
      this.windowErrorCount++;
      this.recordOutcome(false);
      this.pushPipelinePacket(key, targetShard, 'TIMEOUT_504', latencyMs);
      return 'TIMEOUT_504';
    }

    this.recordOutcome(true);
    this.pushPipelinePacket(key, targetShard, 'OK', latencyMs);
    return 'OK';
  }

  private pushPipelinePacket(
    key: string,
    targetShard: string,
    status: MicroservicePacket['status'],
    latencyMs: number
  ): void {
    if (this.servicePackets.length > 36) return;
    this.servicePackets.push({
      id: this.nextSvcPktId++,
      stage: 0,
      progress: 0,
      status,
      latencyMs,
      key,
      targetShard,
    });
  }

  private recordOutcome(ok: boolean): void {
    this.recentOutcomes.push(ok);
    if (this.recentOutcomes.length > this.outcomeWindow) {
      this.recentOutcomes.shift();
    }

    if (this.breakerState === 'HALF-OPEN') {
      if (ok) {
        this.halfOpenSuccesses++;
        if (this.halfOpenSuccesses >= this.halfOpenSuccessThreshold) {
          this.recentOutcomes = [true, true, true];
          this.transitionBreaker('CLOSED', '3 consecutive HALF-OPEN probes succeeded');
        }
      } else {
        this.breakerOpenElapsedMs = 0;
        this.transitionBreaker('OPEN', 'HALF-OPEN probe failed — re-tripping breaker');
      }
      return;
    }

    if (this.breakerState === 'CLOSED' && this.recentOutcomes.length >= 8) {
      const failRate = this.getFailureRate();
      if (failRate >= this.failureRateThreshold) {
        this.breakerOpenElapsedMs = 0;
        this.transitionBreaker(
          'OPEN',
          `Failure rate ${Math.round(failRate * 100)}% exceeded ${Math.round(
            this.failureRateThreshold * 100
          )}% threshold`
        );
      }
    }
  }

  private transitionBreaker(next: BreakerState, reason: string): void {
    const prev = this.breakerState;
    if (prev === next) return;
    this.breakerState = next;
    this.callbacks.onBreakerChange?.(prev, next, reason);
    this.callbacks.onEvent?.({
      id: this.nextEventId++,
      timestamp: new Date().toISOString().slice(14, 23),
      level: next === 'OPEN' ? 'ERROR' : next === 'HALF-OPEN' ? 'WARN' : 'SUCCESS',
      source: 'CIRCUIT',
      message: `Circuit Breaker: ${prev} -> ${next} (${reason})`,
    });
  }
}
