/**
 * RAFT-MESH: Web Audio API Telemetry Sound Engine
 * Lead Software Engineer & AI Architect: أ. طارق ابوعشي | Tareq Abuashi
 */

export class AudioEngine {
  private ctx: AudioContext | null = null;
  private enabled = true;
  private lastCommitTime = 0;

  private ensureContext(): AudioContext | null {
    if (!this.enabled) return null;
    if (!this.ctx && typeof window !== 'undefined') {
      const AudioCtx = window.AudioContext || (window as unknown as { webkitAudioContext: typeof AudioContext }).webkitAudioContext;
      if (AudioCtx) {
        this.ctx = new AudioCtx();
      }
    }
    if (this.ctx && this.ctx.state === 'suspended') {
      this.ctx.resume().catch(() => {});
    }
    return this.ctx;
  }

  public isEnabled(): boolean {
    return this.enabled;
  }

  public toggle(): boolean {
    this.enabled = !this.enabled;
    if (this.enabled) {
      this.ensureContext();
      this.playTone(523.25, 0.08, 'sine', 0.06);
    }
    return this.enabled;
  }

  public unlock(): void {
    this.ensureContext();
  }

  private playTone(
    freq: number,
    duration: number,
    type: OscillatorType = 'sine',
    volume = 0.06,
    delay = 0
  ): void {
    const ctx = this.ensureContext();
    if (!ctx) return;

    const now = ctx.currentTime + delay;
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();

    osc.type = type;
    osc.frequency.setValueAtTime(freq, now);

    gain.gain.setValueAtTime(0.0001, now);
    gain.gain.exponentialRampToValueAtTime(volume, now + 0.012);
    gain.gain.exponentialRampToValueAtTime(0.0001, now + duration);

    osc.connect(gain);
    gain.connect(ctx.destination);

    osc.start(now);
    osc.stop(now + duration + 0.02);
  }

  /** Warm ascending harmonic triad when a new Raft Leader is elected */
  public playLeaderElected(): void {
    this.playTone(440, 0.14, 'triangle', 0.065, 0);
    this.playTone(554.37, 0.14, 'triangle', 0.06, 0.07);
    this.playTone(659.25, 0.24, 'sine', 0.07, 0.14);
  }

  /** Crisp data-ack chime when a log entry reaches majority commit */
  public playLogCommit(): void {
    const nowMs = performance.now();
    if (nowMs - this.lastCommitTime < 90) return;
    this.lastCommitTime = nowMs;
    this.playTone(880, 0.07, 'sine', 0.04, 0);
    this.playTone(1174.66, 0.09, 'sine', 0.035, 0.035);
  }

  /** Alert pulse when Circuit Breaker trips OPEN or node crashes */
  public playAlertTrip(): void {
    this.playTone(220, 0.16, 'sawtooth', 0.05, 0);
    this.playTone(164.81, 0.22, 'sawtooth', 0.045, 0.1);
  }

  /** Clean shimmer when cluster heals or Circuit Breaker recovers to CLOSED */
  public playHealRecovery(): void {
    this.playTone(523.25, 0.12, 'sine', 0.05, 0);
    this.playTone(659.25, 0.12, 'sine', 0.05, 0.06);
    this.playTone(783.99, 0.2, 'sine', 0.055, 0.12);
  }
}
