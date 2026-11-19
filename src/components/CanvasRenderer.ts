/**
 * RAFT-MESH: 60fps HiDPI Canvas 2D Renderers
 * (Raft Topology + RPC Packets, Consistent Hashing Ring, Microservices Pipeline & P99/RPS Chart)
 * Lead Software Engineer & AI Architect: أ. طارق ابوعشي | Tareq Abuashi
 */

import type { MeshEngine } from '../engine/meshEngine';
import type { RaftEngine, RaftNode, RpcType } from '../engine/raftEngine';

const SHARD_COLORS: Record<string, string> = {
  N1: '#06b6d4',
  N2: '#8b5cf6',
  N3: '#10b981',
  N4: '#f59e0b',
  N5: '#38bdf8',
  N6: '#ec4899',
  N7: '#a3e635',
};

const RPC_COLORS: Record<RpcType, string> = {
  RequestVote: '#f59e0b',
  VoteGranted: '#10b981',
  VoteDenied: '#f43f5e',
  Heartbeat: '#06b6d4',
  AppendEntries: '#a855f7',
  AppendAck: '#38bdf8',
};

export function resizeCanvasToDisplaySize(canvas: HTMLCanvasElement): { width: number; height: number; ctx: CanvasRenderingContext2D | null } {
  const rect = canvas.getBoundingClientRect();
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  const w = Math.max(100, Math.floor(rect.width));
  const h = Math.max(100, Math.floor(rect.height));

  if (canvas.width !== Math.floor(w * dpr) || canvas.height !== Math.floor(h * dpr)) {
    canvas.width = Math.floor(w * dpr);
    canvas.height = Math.floor(h * dpr);
  }
  const ctx = canvas.getContext('2d');
  if (ctx) {
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }
  return { width: w, height: h, ctx };
}

export function hitTestRaftNode(raft: RaftEngine, x: number, y: number): RaftNode | null {
  for (const node of raft.nodes) {
    const dx = x - node.x;
    const dy = y - node.y;
    if (dx * dx + dy * dy <= 34 * 34) {
      return node;
    }
  }
  return null;
}

export function renderRaftTopology(
  canvas: HTMLCanvasElement,
  raft: RaftEngine,
  hoveredNodeId: string | null
): void {
  const { width, height, ctx } = resizeCanvasToDisplaySize(canvas);
  if (!ctx) return;

  // Background
  ctx.fillStyle = '#060913';
  ctx.fillRect(0, 0, width, height);

  // Subtle tactical grid
  ctx.strokeStyle = 'rgba(139, 92, 246, 0.06)';
  ctx.lineWidth = 1;
  const gridStep = 36;
  for (let gx = (width % gridStep) / 2; gx < width; gx += gridStep) {
    ctx.beginPath();
    ctx.moveTo(gx, 0);
    ctx.lineTo(gx, height);
    ctx.stroke();
  }
  for (let gy = (height % gridStep) / 2; gy < height; gy += gridStep) {
    ctx.beginPath();
    ctx.moveTo(0, gy);
    ctx.lineTo(width, gy);
    ctx.stroke();
  }

  // Compute target coordinates for nodes
  const cx = width / 2;
  const cy = height / 2 + 6;
  const n = raft.nodes.length;

  if (!raft.partitionActive) {
    const radius = Math.min(width, height) * 0.34;
    for (let i = 0; i < n; i++) {
      const angle = -Math.PI / 2 + (i * Math.PI * 2) / n;
      const targetX = cx + Math.cos(angle) * radius;
      const targetY = cy + Math.sin(angle) * radius;
      const node = raft.nodes[i];
      if (node.x === 0 && node.y === 0) {
        node.x = targetX;
        node.y = targetY;
      } else {
        node.x += (targetX - node.x) * 0.18;
        node.y += (targetY - node.y) * 0.18;
      }
    }
  } else {
    const minNodes = raft.nodes.filter((nd) => nd.partitionId === 1);
    const majNodes = raft.nodes.filter((nd) => nd.partitionId === 2);

    const leftX = width * 0.25;
    const rightX = width * 0.74;
    const minSpread = Math.min(height * 0.26, 100);
    const majRadius = Math.min(width * 0.16, height * 0.28);

    minNodes.forEach((node, idx) => {
      const ty =
        minNodes.length === 1
          ? cy
          : cy - minSpread / 2 + (idx / (minNodes.length - 1)) * minSpread * (minNodes.length === 2 ? 1.4 : 1.7);
      const tx = leftX + (idx % 2 === 0 ? -16 : 16);
      node.x += (tx - node.x) * 0.18;
      node.y += (ty - node.y) * 0.18;
    });

    majNodes.forEach((node, idx) => {
      const angle = -Math.PI / 2 + (idx * Math.PI * 2) / majNodes.length;
      const tx = rightX + Math.cos(angle) * majRadius;
      const ty = cy + Math.sin(angle) * majRadius;
      node.x += (tx - node.x) * 0.18;
      node.y += (ty - node.y) * 0.18;
    });

    // Draw Split-Brain Partition Barrier
    ctx.save();
    ctx.strokeStyle = 'rgba(244, 63, 94, 0.75)';
    ctx.lineWidth = 2;
    ctx.setLineDash([8, 6]);
    ctx.beginPath();
    ctx.moveTo(width * 0.48, 36);
    ctx.lineTo(width * 0.48, height - 36);
    ctx.stroke();

    ctx.fillStyle = 'rgba(244, 63, 94, 0.16)';
    ctx.fillRect(width * 0.48 - 76, 44, 152, 22);
    ctx.strokeStyle = 'rgba(244, 63, 94, 0.5)';
    ctx.setLineDash([]);
    ctx.strokeRect(width * 0.48 - 76, 44, 152, 22);

    ctx.fillStyle = '#fda4af';
    ctx.font = '600 10px "Fira Code", monospace';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText('NETWORK PARTITION', width * 0.48, 55);

    ctx.fillStyle = 'rgba(245, 158, 11, 0.85)';
    ctx.fillText(`MINORITY ISLAND (${minNodes.length}/${n})`, leftX, 34);
    ctx.fillStyle = 'rgba(16, 185, 129, 0.9)';
    ctx.fillText(`MAJORITY ISLAND (${majNodes.length}/${n} — QUORUM)`, rightX, 34);
    ctx.restore();
  }

  // Draw inter-node links
  for (let i = 0; i < n; i++) {
    for (let j = i + 1; j < n; j++) {
      const a = raft.nodes[i];
      const b = raft.nodes[j];
      const partitionedLink = raft.partitionActive && a.partitionId !== b.partitionId;
      const crashedLink = a.role === 'CRASHED' || b.role === 'CRASHED';
      const hasLeader = a.role === 'LEADER' || b.role === 'LEADER';

      ctx.save();
      ctx.beginPath();
      ctx.moveTo(a.x, a.y);
      ctx.lineTo(b.x, b.y);
      if (partitionedLink) {
        ctx.strokeStyle = 'rgba(244, 63, 94, 0.12)';
        ctx.setLineDash([4, 6]);
        ctx.lineWidth = 1;
      } else if (crashedLink) {
        ctx.strokeStyle = 'rgba(71, 85, 105, 0.16)';
        ctx.lineWidth = 1;
      } else if (hasLeader) {
        ctx.strokeStyle = raft.packetLossRate > 0 ? 'rgba(245, 158, 11, 0.24)' : 'rgba(16, 185, 129, 0.24)';
        ctx.lineWidth = 1.5;
      } else {
        ctx.strokeStyle = 'rgba(6, 182, 212, 0.14)';
        ctx.lineWidth = 1;
      }
      ctx.stroke();
      ctx.restore();
    }
  }

  // Draw flying RPC packets
  for (const pkt of raft.packets) {
    const from = raft.nodes.find((nd) => nd.id === pkt.fromId);
    const to = raft.nodes.find((nd) => nd.id === pkt.toId);
    if (!from || !to) continue;

    const px = from.x + (to.x - from.x) * pkt.progress;
    const py = from.y + (to.y - from.y) * pkt.progress;
    const color = RPC_COLORS[pkt.type] || '#06b6d4';

    ctx.save();
    // Tail trail
    const tailProg = Math.max(0, pkt.progress - 0.12);
    const tx = from.x + (to.x - from.x) * tailProg;
    const ty = from.y + (to.y - from.y) * tailProg;

    ctx.strokeStyle = color;
    ctx.lineWidth = pkt.type === 'AppendEntries' || pkt.type === 'RequestVote' ? 2.6 : 1.8;
    ctx.globalAlpha = 0.65;
    ctx.beginPath();
    ctx.moveTo(tx, ty);
    ctx.lineTo(px, py);
    ctx.stroke();

    // Packet orb
    ctx.globalAlpha = 1;
    ctx.shadowColor = color;
    ctx.shadowBlur = 10;
    ctx.fillStyle = color;
    ctx.beginPath();
    ctx.arc(px, py, pkt.type === 'Heartbeat' ? 3.5 : 4.6, 0, Math.PI * 2);
    ctx.fill();

    // Drop marker if about to drop
    if (pkt.dropped && pkt.progress > pkt.dropAt - 0.08) {
      ctx.fillStyle = '#f43f5e';
      ctx.font = '700 10px "Fira Code", monospace';
      ctx.fillText('✕ DROP', px + 8, py - 6);
    } else if (pkt.type !== 'Heartbeat') {
      ctx.shadowBlur = 0;
      ctx.fillStyle = '#e2e8f0';
      ctx.font = '500 9px "Fira Code", monospace';
      ctx.fillText(pkt.label, px + 7, py - 5);
    }
    ctx.restore();
  }

  // Draw Raft Nodes
  for (const node of raft.nodes) {
    const isHovered = hoveredNodeId === node.id;
    const isMinority = raft.isNodePartitionedInMinority(node);
    const radius = isHovered ? 31 : 28;

    let mainColor = '#06b6d4'; // FOLLOWER
    if (node.role === 'LEADER') mainColor = '#10b981';
    else if (node.role === 'CANDIDATE') mainColor = '#f59e0b';
    else if (node.role === 'CRASHED') mainColor = '#f43f5e';

    ctx.save();

    // Leader halo pulse
    if (node.role === 'LEADER') {
      const pulseR = radius + 7 + Math.sin(node.pulsePhase * 1.4) * 3.5;
      ctx.strokeStyle = isMinority ? 'rgba(245, 158, 11, 0.45)' : 'rgba(16, 185, 129, 0.45)';
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.arc(node.x, node.y, pulseR, 0, Math.PI * 2);
      ctx.stroke();
    }

    // Election Timeout Progress Arc for Followers & Candidates
    if (node.role === 'FOLLOWER' || node.role === 'CANDIDATE') {
      const ratio = Math.min(1, node.electionElapsedMs / node.electionTimeoutMs);
      ctx.strokeStyle = node.role === 'CANDIDATE' ? '#f59e0b' : 'rgba(56, 189, 248, 0.7)';
      ctx.lineWidth = 3;
      ctx.beginPath();
      ctx.arc(node.x, node.y, radius + 5, -Math.PI / 2, -Math.PI / 2 + ratio * Math.PI * 2);
      ctx.stroke();
    }

    // Node body
    ctx.shadowColor = mainColor;
    ctx.shadowBlur = node.role === 'CRASHED' ? 6 : 16;
    ctx.fillStyle = node.role === 'CRASHED' ? 'rgba(30, 10, 18, 0.95)' : 'rgba(10, 17, 34, 0.95)';
    ctx.strokeStyle = mainColor;
    ctx.lineWidth = node.role === 'LEADER' ? 3 : 2;
    ctx.beginPath();
    ctx.arc(node.x, node.y, radius, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();
    ctx.shadowBlur = 0;

    // Partition ring indicator
    if (isMinority && node.role !== 'CRASHED') {
      ctx.strokeStyle = '#a855f7';
      ctx.lineWidth = 1.5;
      ctx.setLineDash([4, 3]);
      ctx.beginPath();
      ctx.arc(node.x, node.y, radius + 9, 0, Math.PI * 2);
      ctx.stroke();
      ctx.setLineDash([]);
    }

    // Node ID & Term inside circle
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillStyle = node.role === 'CRASHED' ? '#fda4af' : '#f8fafc';
    ctx.font = '700 13px "Fira Code", monospace';
    ctx.fillText(node.id, node.x, node.y - 6);

    ctx.fillStyle = mainColor;
    ctx.font = '600 10px "Fira Code", monospace';
    ctx.fillText(`T${node.currentTerm}`, node.x, node.y + 8);

    // Role Badge below node
    const roleLabel =
      node.role === 'CRASHED'
        ? 'CRASHED'
        : node.role === 'CANDIDATE'
        ? `CAND (${node.votesReceived.length}/${raft.quorumSize})`
        : isMinority
        ? `${node.role} • PART`
        : node.role;

    ctx.font = '600 9.5px "Fira Code", monospace';
    const badgeW = ctx.measureText(roleLabel).width + 12;
    const badgeY = node.y + radius + 14;

    ctx.fillStyle = 'rgba(6, 9, 19, 0.9)';
    ctx.strokeStyle = isMinority && node.role !== 'CRASHED' ? '#a855f7' : mainColor;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.roundRect(node.x - badgeW / 2, badgeY - 9, badgeW, 18, 4);
    ctx.fill();
    ctx.stroke();

    ctx.fillStyle = isMinority && node.role !== 'CRASHED' ? '#d8b4fe' : mainColor;
    ctx.fillText(roleLabel, node.x, badgeY);

    // Log & Commit Index micro-tag
    ctx.fillStyle = '#94a3b8';
    ctx.font = '500 9px "Fira Code", monospace';
    ctx.fillText(`C:${node.commitIndex} / L:${node.log.length}`, node.x, badgeY + 17);

    ctx.restore();
  }
}

export function renderHashRing(
  canvas: HTMLCanvasElement,
  mesh: MeshEngine,
  raft: RaftEngine
): void {
  const { width, height, ctx } = resizeCanvasToDisplaySize(canvas);
  if (!ctx) return;

  ctx.fillStyle = '#060913';
  ctx.fillRect(0, 0, width, height);

  const cx = width / 2;
  const cy = height / 2 + 4;
  const radius = Math.max(48, Math.min(width, height) * 0.34);

  // Outer base ring
  ctx.save();
  ctx.strokeStyle = 'rgba(139, 92, 246, 0.25)';
  ctx.lineWidth = 8;
  ctx.beginPath();
  ctx.arc(cx, cy, radius, 0, Math.PI * 2);
  ctx.stroke();

  // Quadrant hash markers
  const markers = [
    { angle: -Math.PI / 2, label: '0x0000' },
    { angle: 0, label: '0x4000' },
    { angle: Math.PI / 2, label: '0x8000' },
    { angle: Math.PI, label: '0xC000' },
  ];
  ctx.fillStyle = '#64748b';
  ctx.font = '500 9px "Fira Code", monospace';
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  for (const m of markers) {
    const mx = cx + Math.cos(m.angle) * (radius + 18);
    const my = cy + Math.sin(m.angle) * (radius + 18);
    ctx.fillText(m.label, mx, my);
  }

  const crashedSet = new Set(raft.nodes.filter((n) => n.role === 'CRASHED').map((n) => n.id));

  // Plot all 300+ virtual nodes on the ring
  for (const vn of mesh.ring) {
    const isCrashed = crashedSet.has(vn.nodeId);
    const a = vn.angle - Math.PI / 2;
    const x1 = cx + Math.cos(a) * (radius - 5);
    const y1 = cy + Math.sin(a) * (radius - 5);
    const x2 = cx + Math.cos(a) * (radius + 5);
    const y2 = cy + Math.sin(a) * (radius + 5);

    ctx.strokeStyle = isCrashed ? 'rgba(244, 63, 94, 0.25)' : SHARD_COLORS[vn.nodeId] || '#06b6d4';
    ctx.globalAlpha = isCrashed ? 0.3 : 0.72;
    ctx.lineWidth = 1.3;
    ctx.beginPath();
    ctx.moveTo(x1, y1);
    ctx.lineTo(x2, y2);
    ctx.stroke();
  }
  ctx.globalAlpha = 1;

  // Draw live routed key traces sweeping clockwise to target vnode
  for (const tr of mesh.routedTraces) {
    const startA = tr.angle - Math.PI / 2;
    let endA = tr.targetAngle - Math.PI / 2;
    if (endA < startA) endA += Math.PI * 2;
    const color = SHARD_COLORS[tr.targetNodeId] || '#06b6d4';

    ctx.save();
    ctx.globalAlpha = tr.ttl;
    ctx.strokeStyle = color;
    ctx.lineWidth = 2.4;
    ctx.beginPath();
    ctx.arc(cx, cy, radius + 9, startA, endA);
    ctx.stroke();

    // Key dot
    const kx = cx + Math.cos(startA) * (radius + 9);
    const ky = cy + Math.sin(startA) * (radius + 9);
    ctx.fillStyle = '#fbbf24';
    ctx.beginPath();
    ctx.arc(kx, ky, 3.2, 0, Math.PI * 2);
    ctx.fill();

    // Latest trace label
    if (tr === mesh.routedTraces[mesh.routedTraces.length - 1]) {
      const lx = cx + Math.cos(endA) * (radius + 20);
      const ly = cy + Math.sin(endA) * (radius + 20);
      ctx.fillStyle = '#f8fafc';
      ctx.font = '600 9px "Fira Code", monospace';
      ctx.fillText(`${tr.key}→${tr.targetNodeId}`, lx, ly);
    }
    ctx.restore();
  }

  // Center telemetry readout inside Hash Ring
  const activeCount = raft.nodes.length - crashedSet.size;
  ctx.fillStyle = '#f1f5f9';
  ctx.font = '700 13px "Fira Code", monospace';
  ctx.fillText(`${mesh.ring.length} VNODES`, cx, cy - 12);

  ctx.fillStyle = '#06b6d4';
  ctx.font = '600 10px "Fira Code", monospace';
  ctx.fillText(`Active Shards: ${activeCount}/${raft.nodes.length}`, cx, cy + 5);

  ctx.fillStyle = '#94a3b8';
  ctx.font = '500 9px "Fira Code", monospace';
  ctx.fillText('MurmurHash3_32', cx, cy + 20);

  ctx.restore();
}

export function renderPipelineAndCharts(
  canvas: HTMLCanvasElement,
  mesh: MeshEngine
): void {
  const { width, height, ctx } = resizeCanvasToDisplaySize(canvas);
  if (!ctx) return;

  ctx.fillStyle = '#060913';
  ctx.fillRect(0, 0, width, height);

  // Split canvas: Top 42% Microservices Pipeline, Bottom 58% Dual P99 & RPS Chart
  const pipeH = Math.max(82, height * 0.42);

  // --- SECTION 1: MICROSERVICES PIPELINE ---
  const stages = [
    { name: 'API Gateway', sub: `Tokens ${Math.floor(mesh.bucketTokens)}/${mesh.bucketCapacity}`, color: '#06b6d4' },
    { name: 'Auth Service', sub: 'JWT / mTLS', color: '#38bdf8' },
    {
      name: 'Order Service',
      sub: `CB: ${mesh.breakerState}`,
      color:
        mesh.breakerState === 'CLOSED'
          ? '#10b981'
          : mesh.breakerState === 'HALF-OPEN'
          ? '#f59e0b'
          : '#f43f5e',
    },
    { name: 'Raft Ledger', sub: 'CP Quorum', color: '#8b5cf6' },
  ];

  const padX = 58;
  const usableW = Math.max(120, width - padX * 2);
  const stageY = pipeH * 0.52;
  const stagePositions = stages.map((_, idx) => padX + (usableW * idx) / (stages.length - 1));

  // Connecting rails
  ctx.save();
  for (let i = 0; i < stages.length - 1; i++) {
    const x1 = stagePositions[i];
    const x2 = stagePositions[i + 1];
    const isBreakerLink = i === 2;
    ctx.strokeStyle =
      isBreakerLink && mesh.breakerState === 'OPEN'
        ? 'rgba(244, 63, 94, 0.65)'
        : 'rgba(6, 182, 212, 0.35)';
    ctx.lineWidth = 2;
    if (isBreakerLink && mesh.breakerState === 'OPEN') {
      ctx.setLineDash([5, 5]);
    } else {
      ctx.setLineDash([]);
    }
    ctx.beginPath();
    ctx.moveTo(x1, stageY);
    ctx.lineTo(x2, stageY);
    ctx.stroke();
  }
  ctx.setLineDash([]);

  // Animated microservice request pills
  for (const pkt of mesh.servicePackets) {
    const px = stagePositions[0] + (stagePositions[3] - stagePositions[0]) * pkt.progress;
    const py = stageY + Math.sin(pkt.id * 1.7) * 5;
    let pktColor = '#10b981';
    if (pkt.status === 'SHED_429') pktColor = '#f59e0b';
    else if (pkt.status === 'OPEN_503' || pkt.status === 'TIMEOUT_504') pktColor = '#f43f5e';

    ctx.fillStyle = pktColor;
    ctx.shadowColor = pktColor;
    ctx.shadowBlur = 8;
    ctx.beginPath();
    ctx.arc(px, py, 3.6, 0, Math.PI * 2);
    ctx.fill();
  }
  ctx.shadowBlur = 0;

  // Stage boxes
  for (let i = 0; i < stages.length; i++) {
    const st = stages[i];
    const sx = stagePositions[i];
    const boxW = Math.min(116, usableW / 4.2);
    const boxH = 40;

    ctx.fillStyle = 'rgba(10, 16, 32, 0.96)';
    ctx.strokeStyle = st.color;
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.roundRect(sx - boxW / 2, stageY - boxH / 2, boxW, boxH, 7);
    ctx.fill();
    ctx.stroke();

    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillStyle = '#f1f5f9';
    ctx.font = '600 10px "Fira Code", monospace';
    ctx.fillText(st.name, sx, stageY - 7);

    ctx.fillStyle = st.color;
    ctx.font = '600 9px "Fira Code", monospace';
    ctx.fillText(st.sub, sx, stageY + 8);
  }

  // Divider line
  ctx.strokeStyle = 'rgba(139, 92, 246, 0.18)';
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(12, pipeH);
  ctx.lineTo(width - 12, pipeH);
  ctx.stroke();

  // --- SECTION 2: P99 LATENCY & RPS CHART ---
  const chartTop = pipeH + 22;
  const chartBottom = height - 18;
  const chartLeft = 42;
  const chartRight = width - 42;
  const chartW = Math.max(60, chartRight - chartLeft);
  const chartH = Math.max(40, chartBottom - chartTop);

  // Header legend
  ctx.textAlign = 'left';
  ctx.fillStyle = '#f59e0b';
  ctx.font = '600 10px "Fira Code", monospace';
  ctx.fillText(`● P99 Latency: ${mesh.currentP99Ms} ms`, chartLeft, pipeH + 12);

  ctx.fillStyle = '#06b6d4';
  ctx.fillText(`● Throughput: ${mesh.currentRps} RPS`, chartLeft + 175, pipeH + 12);

  ctx.fillStyle = mesh.currentErrorPct > 15 ? '#f43f5e' : '#94a3b8';
  ctx.textAlign = 'right';
  ctx.fillText(`Err/Shed: ${mesh.currentErrorPct}%`, chartRight, pipeH + 12);

  // Chart grid
  ctx.strokeStyle = 'rgba(148, 163, 184, 0.1)';
  ctx.lineWidth = 1;
  for (let g = 0; g <= 3; g++) {
    const gy = chartTop + (chartH * g) / 3;
    ctx.beginPath();
    ctx.moveTo(chartLeft, gy);
    ctx.lineTo(chartRight, gy);
    ctx.stroke();
  }

  const pts = mesh.telemetryHistory;
  if (pts.length > 1) {
    const maxLat = Math.max(120, ...pts.map((p) => p.p99Ms));
    const maxRps = Math.max(25, ...pts.map((p) => p.rps));

    // RPS Area + Line
    ctx.beginPath();
    pts.forEach((p, idx) => {
      const px = chartLeft + (idx / (pts.length - 1)) * chartW;
      const py = chartBottom - (p.rps / maxRps) * chartH;
      if (idx === 0) ctx.moveTo(px, py);
      else ctx.lineTo(px, py);
    });
    ctx.strokeStyle = '#06b6d4';
    ctx.lineWidth = 2;
    ctx.stroke();

    ctx.lineTo(chartRight, chartBottom);
    ctx.lineTo(chartLeft, chartBottom);
    ctx.closePath();
    ctx.fillStyle = 'rgba(6, 182, 212, 0.12)';
    ctx.fill();

    // P99 Latency Line
    ctx.beginPath();
    pts.forEach((p, idx) => {
      const px = chartLeft + (idx / (pts.length - 1)) * chartW;
      const py = chartBottom - (p.p99Ms / maxLat) * chartH;
      if (idx === 0) ctx.moveTo(px, py);
      else ctx.lineTo(px, py);
    });
    ctx.strokeStyle = mesh.currentP99Ms > 180 ? '#f43f5e' : '#f59e0b';
    ctx.lineWidth = 2.2;
    ctx.stroke();

    // Axis labels
    ctx.textAlign = 'right';
    ctx.fillStyle = '#f59e0b';
    ctx.font = '500 8.5px "Fira Code", monospace';
    ctx.fillText(`${maxLat}ms`, chartLeft - 4, chartTop + 4);
    ctx.fillText('0ms', chartLeft - 4, chartBottom);

    ctx.textAlign = 'left';
    ctx.fillStyle = '#06b6d4';
    ctx.fillText(`${maxRps}rps`, chartRight + 4, chartTop + 4);
  }

  ctx.restore();
}
