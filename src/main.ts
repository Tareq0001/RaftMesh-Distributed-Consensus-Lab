/**
 * RAFT-MESH: Main Telemetry Orchestrator & 60fps Simulation Loop
 * Lead Software Engineer & AI Architect: أ. طارق ابوعشي | Tareq Abuashi
 */

import { AudioEngine } from './engine/audioEngine';
import { MeshEngine } from './engine/meshEngine';
import { RaftEngine, type TelemetryEvent } from './engine/raftEngine';
import { createDossierModal } from './components/DossierModal';
import {
  hitTestRaftNode,
  renderHashRing,
  renderPipelineAndCharts,
  renderRaftTopology,
} from './components/CanvasRenderer';

const SHARD_COLORS: Record<string, string> = {
  N1: '#06b6d4',
  N2: '#8b5cf6',
  N3: '#10b981',
  N4: '#f59e0b',
  N5: '#38bdf8',
  N6: '#ec4899',
  N7: '#a3e635',
};

function escapeHtml(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function bootstrap(): void {
  const audio = new AudioEngine();
  const dossier = createDossierModal();

  const eventItems: TelemetryEvent[] = [];
  const pushEvent = (ev: TelemetryEvent) => {
    eventItems.unshift(ev);
    if (eventItems.length > 45) eventItems.pop();
    renderEventLog();
  };

  const raft = new RaftEngine({
    onLeaderElected: () => audio.playLeaderElected(),
    onLogCommitted: () => audio.playLogCommit(),
    onNodeCrashed: () => audio.playAlertTrip(),
    onClusterHealed: () => audio.playHealRecovery(),
    onEvent: pushEvent,
  });

  const mesh = new MeshEngine({
    onBreakerChange: (_prev, next) => {
      if (next === 'OPEN') audio.playAlertTrip();
      else if (next === 'CLOSED') audio.playHealRecovery();
    },
    onEvent: pushEvent,
  });
  mesh.rebuildRing(raft.nodes);

  // Canvas references
  const raftCanvas = document.getElementById('raft-canvas') as HTMLCanvasElement;
  const hashCanvas = document.getElementById('hash-canvas') as HTMLCanvasElement;
  const pipelineCanvas = document.getElementById('pipeline-canvas') as HTMLCanvasElement;

  // Header & Toolbar controls
  const audioBtn = document.getElementById('audio-toggle-btn') as HTMLButtonElement;
  const audioTxt = document.getElementById('audio-status-text') as HTMLSpanElement;
  const openDossierBtn = document.getElementById('open-dossier-btn') as HTMLButtonElement;

  const btnSize5 = document.getElementById('btn-size-5') as HTMLButtonElement;
  const btnSize7 = document.getElementById('btn-size-7') as HTMLButtonElement;
  const btnKillLeader = document.getElementById('btn-kill-leader') as HTMLButtonElement;
  const btnSplitBrain = document.getElementById('btn-split-brain') as HTMLButtonElement;
  const btnPacketLoss = document.getElementById('btn-packet-loss') as HTMLButtonElement;
  const btnHealCluster = document.getElementById('btn-heal-cluster') as HTMLButtonElement;
  const btnPlayPause = document.getElementById('btn-play-pause') as HTMLButtonElement;
  const txtPlayPause = document.getElementById('txt-play-pause') as HTMLSpanElement;
  const speedBtns = Array.from(document.querySelectorAll('.speed-btn')) as HTMLButtonElement[];

  // Command & Telemetry elements
  const cmdForm = document.getElementById('cmd-form') as HTMLFormElement;
  const cmdInput = document.getElementById('cmd-input') as HTMLInputElement;
  const cmdFeedback = document.getElementById('cmd-feedback') as HTMLDivElement;
  const btnAutoBurst = document.getElementById('btn-auto-burst') as HTMLButtonElement;
  const btnMinorityWrite = document.getElementById('btn-minority-write') as HTMLButtonElement;
  const btnRouteSample = document.getElementById('btn-route-sample') as HTMLButtonElement;
  const quickCmdBtns = Array.from(document.querySelectorAll('.quick-cmd')) as HTMLButtonElement[];

  const hudFps = document.getElementById('hud-fps') as HTMLSpanElement;
  const hudVnodes = document.getElementById('hud-vnodes') as HTMLSpanElement;
  const hudBreaker = document.getElementById('hud-breaker-badge') as HTMLSpanElement;
  const hudP99 = document.getElementById('hud-p99') as HTMLElement;
  const hudRps = document.getElementById('hud-rps') as HTMLElement;
  const raftPulseDot = document.getElementById('raft-pulse-dot') as HTMLSpanElement;

  const valTerm = document.getElementById('val-term') as HTMLSpanElement;
  const valLeader = document.getElementById('val-leader') as HTMLSpanElement;
  const valQuorum = document.getElementById('val-quorum') as HTMLSpanElement;
  const valCommit = document.getElementById('val-commit') as HTMLSpanElement;
  const capStatusBadge = document.getElementById('cap-status-badge') as HTMLSpanElement;
  const kvStoreGrid = document.getElementById('kv-store-grid') as HTMLDivElement;
  const nodeLogsList = document.getElementById('node-logs-list') as HTMLDivElement;
  const shardDistBar = document.getElementById('shard-dist-bar') as HTMLDivElement;

  const breakerStatePill = document.getElementById('breaker-state-pill') as HTMLSpanElement;
  const valBucket = document.getElementById('val-bucket') as HTMLSpanElement;
  const barBucket = document.getElementById('bar-bucket') as HTMLDivElement;
  const valFailRate = document.getElementById('val-fail-rate') as HTMLSpanElement;
  const valShedCount = document.getElementById('val-shed-count') as HTMLSpanElement;
  const eventLogEl = document.getElementById('event-log') as HTMLDivElement;

  function renderEventLog(): void {
    eventLogEl.innerHTML = eventItems
      .slice(0, 22)
      .map(
        (ev) =>
          `<div class="event-item ${ev.level}"><strong>[${escapeHtml(ev.timestamp)}] ${escapeHtml(
            ev.source
          )}:</strong> ${escapeHtml(ev.message)}</div>`
      )
      .join('');
  }

  // Unlock Web Audio on first user interaction
  const unlockOnce = () => {
    audio.unlock();
    window.removeEventListener('pointerdown', unlockOnce);
  };
  window.addEventListener('pointerdown', unlockOnce);

  audioBtn.addEventListener('click', () => {
    const enabled = audio.toggle();
    audioBtn.classList.toggle('active', enabled);
    audioTxt.textContent = enabled ? 'Audio: ON' : 'Audio: OFF';
  });

  openDossierBtn.addEventListener('click', () => dossier.open());

  // Cluster Size Switching (5 vs 7 nodes)
  const switchClusterSize = (size: 5 | 7) => {
    raft.initCluster(size);
    mesh.rebuildRing(raft.nodes);
    mesh.resetResilience();
    btnSize5.classList.toggle('active', size === 5);
    btnSize7.classList.toggle('active', size === 7);
    btnSplitBrain.classList.remove('active');
    btnPacketLoss.classList.remove('active');
    btnSplitBrain.textContent =
      size === 5 ? 'Split-Brain Partition (2 vs 3)' : 'Split-Brain Partition (3 vs 4)';
    hudVnodes.textContent = `${mesh.ring.length} VNodes`;
    cmdFeedback.className = 'cmd-feedback';
    cmdFeedback.textContent = `Switched to ${size}-Node Raft cluster (Quorum=${raft.quorumSize}, ${mesh.ring.length} VNodes).`;
  };

  btnSize5.addEventListener('click', () => switchClusterSize(5));
  btnSize7.addEventListener('click', () => switchClusterSize(7));

  // Chaos Controls
  btnKillLeader.addEventListener('click', () => {
    const killed = raft.killCurrentLeader();
    if (!killed) {
      cmdFeedback.className = 'cmd-feedback err';
      cmdFeedback.textContent = 'No active Leader right now — watch randomized election timers!';
    }
  });

  btnSplitBrain.addEventListener('click', () => {
    const active = raft.toggleSplitBrainPartition();
    btnSplitBrain.classList.toggle('active', active);
    btnMinorityWrite.disabled = !active;
  });

  btnPacketLoss.addEventListener('click', () => {
    const active = raft.togglePacketLossChaos();
    btnPacketLoss.classList.toggle('active', active);
  });

  btnHealCluster.addEventListener('click', () => {
    raft.healCluster();
    mesh.resetResilience();
    btnSplitBrain.classList.remove('active');
    btnPacketLoss.classList.remove('active');
    btnMinorityWrite.disabled = true;
    cmdFeedback.className = 'cmd-feedback ok';
    cmdFeedback.textContent = 'Cluster healed: partitions cleared, nodes revived, logs converging.';
  });

  // Play/Pause & Speed
  btnPlayPause.addEventListener('click', () => {
    raft.paused = !raft.paused;
    btnPlayPause.classList.toggle('active', !raft.paused);
    txtPlayPause.textContent = raft.paused ? 'Paused' : 'Running';
  });

  for (const sbtn of speedBtns) {
    sbtn.addEventListener('click', () => {
      const sp = Number(sbtn.dataset.speed || 1);
      raft.speedMultiplier = sp;
      for (const b of speedBtns) b.classList.toggle('active', b === sbtn);
    });
  }

  // Client Command Submission
  const executeCommand = (raw: string, targetMinority = false) => {
    const res = raft.submitClientCommand(raw, targetMinority);
    // Also route key through Consistent Hash Ring + Gateway
    const keyMatch = raw.trim().split(/\s+/)[1]?.split('=')[0] || 'tx:client';
    mesh.dispatchGatewayRequest(
      raft.nodes,
      {
        packetLossRate: raft.packetLossRate,
        jitterMs: raft.jitterMs,
        hasMajorityLeader: Boolean(raft.getMajorityLeader()),
      },
      keyMatch
    );

    cmdFeedback.className = res.accepted && !targetMinority ? 'cmd-feedback ok' : 'cmd-feedback err';
    cmdFeedback.textContent = res.reason;
  };

  cmdForm.addEventListener('submit', (e) => {
    e.preventDefault();
    executeCommand(cmdInput.value, false);
  });

  for (const qbtn of quickCmdBtns) {
    qbtn.addEventListener('click', () => {
      const cmd = qbtn.dataset.cmd || 'SET x=42';
      cmdInput.value = cmd;
      executeCommand(cmd, false);
    });
  }

  btnMinorityWrite.addEventListener('click', () => {
    executeCommand(cmdInput.value || 'SET split=minority', true);
  });

  btnAutoBurst.addEventListener('click', () => {
    mesh.autoBurst = !mesh.autoBurst;
    btnAutoBurst.classList.toggle('active', mesh.autoBurst);
    btnAutoBurst.textContent = mesh.autoBurst ? 'Auto-Write Burst: ON' : 'Auto-Write Burst: OFF';
  });

  btnRouteSample.addEventListener('click', () => {
    const sampleKey = `key:${Math.floor(1000 + Math.random() * 9000)}`;
    const routed = mesh.routeKey(sampleKey, raft.nodes);
    if (routed) {
      cmdFeedback.className = 'cmd-feedback ok';
      cmdFeedback.textContent = `MurmurHash3("${sampleKey}") = 0x${routed.hash
        .toString(16)
        .padStart(8, '0')} → Shard ${routed.nodeId}`;
    }
  });

  // Canvas Interaction: Click any node to Crash/Revive
  let hoveredNodeId: string | null = null;
  raftCanvas.addEventListener('mousemove', (e) => {
    const rect = raftCanvas.getBoundingClientRect();
    const hit = hitTestRaftNode(raft, e.clientX - rect.left, e.clientY - rect.top);
    hoveredNodeId = hit ? hit.id : null;
    raftCanvas.style.cursor = hit ? 'pointer' : 'default';
  });

  raftCanvas.addEventListener('mouseleave', () => {
    hoveredNodeId = null;
  });

  raftCanvas.addEventListener('click', (e) => {
    const rect = raftCanvas.getBoundingClientRect();
    const hit = hitTestRaftNode(raft, e.clientX - rect.left, e.clientY - rect.top);
    if (hit) {
      raft.toggleCrashNode(hit.id);
    }
  });

  // Auto-Burst Raft Log Writer timer
  let burstWriteAccumMs = 0;
  let burstSeq = 1;

  // Periodic DOM Telemetry Refresh (~8fps for DOM, 60fps for Canvas)
  let domAccumMs = 0;
  let fpsFrameCount = 0;
  let fpsAccumMs = 0;

  function updateDomTelemetry(): void {
    const majLeader = raft.getMajorityLeader();
    const highestTerm = raft.getHighestTerm();
    const maxCommit = raft.nodes.reduce((m, n) => Math.max(m, n.commitIndex), 0);

    valTerm.textContent = `T${highestTerm}`;
    valLeader.textContent = majLeader ? majLeader.id : 'NONE';
    valLeader.style.color = majLeader ? 'var(--neon-emerald)' : 'var(--neon-rose)';
    valQuorum.textContent = `${raft.quorumSize} / ${raft.clusterSize}`;
    valCommit.textContent = String(maxCommit);

    // CAP & Pulse Dot Status
    if (raft.partitionActive) {
      capStatusBadge.textContent = majLeader ? 'CP: PARTITIONED' : 'CP: NO QUORUM';
      capStatusBadge.className = 'badge warn';
      raftPulseDot.className = 'pulse-dot warn';
    } else if (!majLeader || raft.packetLossRate > 0) {
      capStatusBadge.textContent = raft.packetLossRate > 0 ? 'NET DEGRADED' : 'ELECTING';
      capStatusBadge.className = 'badge danger';
      raftPulseDot.className = 'pulse-dot danger';
    } else {
      capStatusBadge.textContent = 'CP HEALTHY';
      capStatusBadge.className = 'badge success';
      raftPulseDot.className = 'pulse-dot';
    }

    btnMinorityWrite.disabled = !raft.partitionActive || !raft.getMinorityLeader();

    // KV Store from Majority Leader (or highest commit node)
    const refNode =
      majLeader ||
      [...raft.nodes].sort((a, b) => b.commitIndex - a.commitIndex)[0];
    const kvEntries = Object.entries(refNode.kvStore);
    kvStoreGrid.innerHTML = kvEntries
      .map(
        ([k, v]) =>
          `<span class="kv-chip">${escapeHtml(k)} = <strong>${escapeHtml(v)}</strong></span>`
      )
      .join('');

    // Per-Node Replicated Log Inspector
    nodeLogsList.innerHTML = raft.nodes
      .map((node) => {
        const tailLogs = node.log.slice(-6);
        const cells = tailLogs
          .map((entry) => {
            const committed = entry.index <= node.commitIndex;
            return `<span class="log-cell ${
              committed ? 'committed' : 'uncommitted'
            }" title="Idx #${entry.index} Term ${entry.term}: ${escapeHtml(entry.command)}">#${
              entry.index
            }:T${entry.term} ${escapeHtml(entry.key)}</span>`;
          })
          .join('');
        const partFlag = raft.isNodePartitionedInMinority(node) ? ' • MIN' : '';
        return `
          <div class="node-log-row" data-node="${node.id}" title="Click to Crash/Revive ${node.id}">
            <div class="node-log-meta">
              <strong>${node.id}</strong>
              <span class="role-tag ${node.role}">${node.role.slice(0, 4)}${partFlag}</span>
              <span style="color:#94a3b8">C${node.commitIndex}</span>
            </div>
            <div class="log-cells">${cells || '<span style="color:#475569;font-size:0.62rem;">empty</span>'}</div>
          </div>
        `;
      })
      .join('');

    // Allow clicking node rows to Crash/Revive as well
    Array.from(nodeLogsList.querySelectorAll('.node-log-row')).forEach((row) => {
      row.addEventListener('click', () => {
        const nid = (row as HTMLElement).dataset.node;
        if (nid) raft.toggleCrashNode(nid);
      });
    });

    // Shard Key Distribution Bar
    shardDistBar.innerHTML = raft.nodes
      .map((node) => {
        const count = mesh.shardCounts[node.id] || 0;
        const color = SHARD_COLORS[node.id] || '#06b6d4';
        return `<span class="shard-pill ${
          node.role === 'CRASHED' ? 'crashed' : ''
        }" style="border-color:${color}66"><strong style="color:${color}">${
          node.id
        }</strong>: ${count}</span>`;
      })
      .join('');

    // Resilience Telemetry
    const cb = mesh.breakerState;
    hudBreaker.textContent = `CB: ${cb}`;
    hudBreaker.className = `breaker-inline ${cb.toLowerCase()}`;
    breakerStatePill.textContent = cb;
    breakerStatePill.className = `badge ${
      cb === 'CLOSED' ? 'success' : cb === 'HALF-OPEN' ? 'warn' : 'danger'
    }`;

    hudP99.textContent = `${mesh.currentP99Ms} ms`;
    hudRps.textContent = String(mesh.currentRps);

    const tokensFloor = Math.floor(mesh.bucketTokens);
    valBucket.textContent = `${tokensFloor} / ${mesh.bucketCapacity}`;
    barBucket.style.width = `${Math.round((mesh.bucketTokens / mesh.bucketCapacity) * 100)}%`;
    valFailRate.textContent = `${Math.round(mesh.getFailureRate() * 100)}%`;
    valShedCount.textContent = String(mesh.totalShed429);
  }

  let lastTime = performance.now();

  function frame(now: number): void {
    const dtMs = now - lastTime;
    lastTime = now;

    // Step Raft & Mesh engines
    raft.step(dtMs);
    mesh.step(dtMs, raft.nodes, {
      packetLossRate: raft.packetLossRate,
      jitterMs: raft.jitterMs,
      hasMajorityLeader: Boolean(raft.getMajorityLeader()),
      paused: raft.paused,
      speedMultiplier: raft.speedMultiplier,
    });

    // Auto-Write Burst sends periodic Raft writes
    if (mesh.autoBurst && !raft.paused) {
      burstWriteAccumMs += dtMs * raft.speedMultiplier;
      if (burstWriteAccumMs >= 650) {
        burstWriteAccumMs = 0;
        const cmd = burstSeq % 2 === 0 ? 'INC hits' : `SET tx=${100 + (burstSeq % 899)}`;
        burstSeq++;
        raft.submitClientCommand(cmd, false);
      }
    }

    // Render 3 Canvases at 60fps
    renderRaftTopology(raftCanvas, raft, hoveredNodeId);
    renderHashRing(hashCanvas, mesh, raft);
    renderPipelineAndCharts(pipelineCanvas, mesh);

    // FPS tracking
    fpsFrameCount++;
    fpsAccumMs += dtMs;
    if (fpsAccumMs >= 500) {
      const fps = Math.round((fpsFrameCount * 1000) / fpsAccumMs);
      hudFps.textContent = `${fps} FPS`;
      fpsFrameCount = 0;
      fpsAccumMs = 0;
    }

    // Throttled DOM update
    domAccumMs += dtMs;
    if (domAccumMs >= 120) {
      domAccumMs = 0;
      updateDomTelemetry();
    }

    requestAnimationFrame(frame);
  }

  updateDomTelemetry();
  renderEventLog();
  requestAnimationFrame(frame);
}

bootstrap();
