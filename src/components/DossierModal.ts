/**
 * RAFT-MESH: Distributed Systems Theoretical & Architecture Dossier Modal
 * Lead Software Engineer & AI Architect: أ. طارق ابوعشي | Tareq Abuashi
 */

export function createDossierModal(): { open: () => void; close: () => void } {
  const modalHtml = `
    <div id="dossier-modal" class="modal-overlay hidden" role="dialog" aria-modal="true" aria-labelledby="dossier-title">
      <div class="modal-dialog">
        <div class="modal-header">
          <div class="modal-title-wrap">
            <span class="modal-badge">DISTRIBUTED SYSTEMS DOSSIER • #95</span>
            <h2 id="dossier-title">RAFT-MESH: Consensus, Consistent Hashing &amp; Resilience Architecture</h2>
            <p class="modal-author">
              Lead Software Engineer &amp; AI Architect: <strong>أ. طارق ابوعشي | Tareq Abuashi</strong>
            </p>
          </div>
          <button id="close-dossier-btn" class="close-btn" aria-label="Close Dossier Modal">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <line x1="18" y1="6" x2="6" y2="18"></line>
              <line x1="6" y1="6" x2="18" y2="18"></line>
            </svg>
          </button>
        </div>

        <div class="modal-body custom-scroll">
          <!-- 01: RAFT SAFETY GUARANTEES -->
          <section class="math-section">
            <span class="section-tag">01 / RAFT CONSENSUS INVARIANTS</span>
            <h3>Election Safety, Log Matching &amp; Leader Completeness</h3>
            <p>
              Raft decomposes consensus into Leader Election, Log Replication, and Safety. Every node maintains a monotonically increasing
              <span class="highlight-text">currentTerm</span> and randomized election timeout <span class="highlight-text">T<sub>elec</sub> ∈ [1450ms, 2700ms]</span> to prevent split-vote livelocks.
            </p>
            <div class="formula-box highlight">
              <div><strong>Election Safety:</strong> ∀ Term T, |{Leader(T)}| ≤ 1  (Requires Q = ⌊N / 2⌋ + 1 votes)</div>
              <div><strong>Log Matching:</strong> (log[i].term == log'[i].term) ⟹ ∀ k ≤ i, log[k] ≡ log'[k]</div>
              <div><strong>Leader Completeness:</strong> If entry e is committed in Term T, e is guaranteed present in Leader(T') ∀ T' &gt; T</div>
              <div><strong>Quorum Bound:</strong> 5-Node Cluster ⇒ Q = 3 (Tolerates f = 2 crashes) | 7-Node Cluster ⇒ Q = 4 (Tolerates f = 3 crashes)</div>
            </div>
          </section>

          <!-- 02: CAP & PACELC -->
          <section class="math-section">
            <span class="section-tag">02 / CAP THEOREM &amp; PACELC TRADEOFFS</span>
            <h3>CP Consistency During Split-Brain Partitions</h3>
            <p>
              Under the Brewer/Gilbert-Lynch CAP Theorem, Raft is strictly <span class="highlight-text">CP (Consistent + Partition Tolerant)</span> and <span class="highlight-text">PACELC (PC/EC)</span>.
              During a <span class="highlight-text">Split-Brain Partition (2 vs 3)</span>, the minority island cannot assemble Q = 3 acknowledgments; uncommitted writes stall rather than diverging.
            </p>
            <div class="formula-box">
              <div><strong>Quorum Intersection:</strong> |Q<sub>1</sub>| + |Q<sub>2</sub>| &gt; N ⟹ Q<sub>1</sub> ∩ Q<sub>2</sub> ≠ ∅</div>
              <div><strong>Split-Brain Resolution:</strong> Upon partition heal, higher Term T<sub>maj</sub> &gt; T<sub>min</sub> forces stale minority leader to step down and truncate conflicting uncommitted suffix via prevLogIndex/prevLogTerm.</div>
            </div>
          </section>

          <!-- 03: CONSISTENT HASHING -->
          <section class="math-section">
            <span class="section-tag">03 / CONSISTENT HASHING &amp; VIRTUAL NODES</span>
            <h3>MurmurHash3_32 Unit Ring with 300+ VNodes</h3>
            <p>
              Physical Raft shards are projected onto a 32-bit circular hash space <span class="highlight-text">[0, 2³² − 1]</span> using
              <span class="highlight-text">64 virtual nodes per shard</span> (320 vnodes in 5-node mode; 448 vnodes in 7-node mode).
            </p>
            <div class="formula-box">
              <div><strong>Ring Projection:</strong> θ(key) = (MurmurHash3_32(key) / 2³²) × 2π</div>
              <div><strong>Routing Lookup:</strong> O(log(N · V)) binary search clockwise to first active vnode</div>
              <div><strong>Bounded Remapping:</strong> On single shard failure, only ≈ K / N keys migrate instead of K · (N − 1) / N</div>
            </div>
          </section>

          <!-- 04: CIRCUIT BREAKER & TOKEN BUCKET -->
          <section class="math-section">
            <span class="section-tag">04 / MICROSERVICES RESILIENCE PATTERNS</span>
            <h3>Circuit Breaker FSM &amp; Token Bucket Rate Limiter</h3>
            <p>
              The API Gateway enforces admission control via a continuous <span class="highlight-text">Token Bucket</span> before requests traverse
              <span class="highlight-text">Gateway → Auth → Order → Raft Ledger</span>, guarded by a 3-state <span class="highlight-text">Circuit Breaker FSM</span>.
            </p>
            <div class="formula-box">
              <div><strong>Token Bucket:</strong> B(t) = min(C, B<sub>0</sub> + r · Δt), C = 28 tokens, r = 16 req/s (Excess shed with HTTP 429)</div>
              <div><strong>CLOSED → OPEN:</strong> Sliding window (W = 18) error rate ≥ 42% triggers fail-fast HTTP 503</div>
              <div><strong>OPEN → HALF-OPEN:</strong> After cooldown T<sub>reset</sub> = 4200 ms, probe requests test downstream recovery</div>
              <div><strong>HALF-OPEN → CLOSED:</strong> 3 consecutive successful probes restore full traffic flow</div>
            </div>
          </section>
        </div>

        <div class="modal-footer">
          <span class="footer-note">RAFT-MESH Simulator • Architected by أ. طارق ابوعشي | Tareq Abuashi</span>
          <button id="confirm-dossier-btn" class="action-btn">Return to Telemetry</button>
        </div>
      </div>
    </div>
  `;

  document.body.insertAdjacentHTML('beforeend', modalHtml);

  const modal = document.getElementById('dossier-modal') as HTMLDivElement;
  const closeBtn = document.getElementById('close-dossier-btn') as HTMLButtonElement;
  const confirmBtn = document.getElementById('confirm-dossier-btn') as HTMLButtonElement;

  const open = () => modal.classList.remove('hidden');
  const close = () => modal.classList.add('hidden');

  closeBtn.addEventListener('click', close);
  confirmBtn.addEventListener('click', close);
  modal.addEventListener('click', (e) => {
    if (e.target === modal) close();
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && !modal.classList.contains('hidden')) close();
  });

  return { open, close };
}
