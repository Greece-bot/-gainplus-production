/**
 * ============================================================================
 * GAINPLUS+ — FAST REAL-TICK 5-STRATEGY ENGINE
 * ============================================================================
 * Zero-latency, allocation-stable, single-threaded tick evaluation core.
 *
 * HARD GUARANTEES
 *  - No timers, no promises, no `await`, no network I/O anywhere in the tick path.
 *  - Windows 100 / 300 / 500 / 1000 are pre-warmed ring-buffer VIEWS, never gates.
 *  - IDLE -> LOCKED is atomic within one synchronous tick frame.
 *  - EXECUTION REMAINS OFF: this class only emits signal descriptors.
 *
 * INTEGRATION CONTRACT (read before wiring up)
 *  - Auth, DB/Neon, Vercel config, and UI are untouched by this file.
 *  - The `STRATEGY MATH` section below is the ONLY place strategy conditions live.
 *    The bodies shipped here are the standard Deriv contract predicates, provided
 *    so the engine is runnable end-to-end. They are NOT a substitute for your
 *    authoritative XML/JS rules. Overwrite each `evaluate()` body with your
 *    existing conditions verbatim — do not alter this engine to accommodate them.
 * ============================================================================
 */

/* ==========================================================================
 * TYPES
 * ========================================================================== */

export type StrategyId =
  | 'EVEN_ODD'
  | 'OVER_UNDER'
  | 'MATCHES'
  | 'RISE_FALL'
  | 'DIFFERS';

export type EngineState = 'IDLE' | 'LOCKED';
export type CycleOutcome = 'WIN' | 'LOSS';

export interface RawTick {
  quote?: number | string | null;
  price?: number | string | null;
  epoch?: number | string | null;
  timestamp?: number | string | null;
  symbol?: string;
  id?: string | number;
}

export interface NormalizedTick {
  /** Validated, finite, > 0. */
  readonly price: number;
  /** Validated monotonic epoch in seconds. */
  readonly epoch: number;
  /** Last digit at the configured pip size, precomputed once per tick. */
  readonly digit: number;
  readonly symbol: string;
  readonly receivedAt: number;
}

export interface StrategyResult {
  direction: string;
  meta?: Record<string, number>;
}

export interface EngineSignal {
  id: string;
  strategy: StrategyId;
  direction: string;
  tick: NormalizedTick;
  /** 0-based index of this tick in the engine's lifetime. */
  tickIndex: number;
  epoch: number;
  emittedAt: number;
  /** Wall time spent evaluating this tick, in microseconds. */
  evalMicros: number;
  meta?: Record<string, number>;
}

export interface BreakerConfig {
  enabled: boolean;
  /** Consecutive malformed/stale/discontinuous ticks before the breaker trips. */
  maxConsecutiveAnomalies: number;
  /** Epoch delta above this marks a stream discontinuity. */
  maxTickGapSeconds: number;
}

export interface EngineConfig {
  symbol: string;
  /** Decimal places used to derive the last digit. Must match your Deriv feed. */
  pipSize: number;
  /** Ring buffer capacity. Must be >= max(windows). */
  bufferCapacity: number;
  /** Pre-warmed window sizes, exposed as zero-copy views. */
  windows: readonly number[];
  /** OVER/UNDER barrier digit (0-9). */
  overUnderBarrier: number;
  /** MATCHES prediction digit (0-9). */
  matchesPrediction: number;
  /** First match wins. Reorder to change priority. */
  strategyOrder: readonly StrategyId[];
  /**
   * Minimum buffered ticks before a strategy may be evaluated.
   * Set these to whatever your XML rules require — they are cold-start guards,
   * not artificial delays. The engine never waits for NEW ticks beyond this.
   */
  minSamples: Readonly<Record<StrategyId, number>>;
  breaker: BreakerConfig;
}

export interface EngineStats {
  ticksIngested: number;
  ticksRejected: number;
  signalsEmitted: number;
  wins: number;
  losses: number;
  consecutiveLosses: number;
  lastEvalMicros: number;
  maxEvalMicros: number;
  bufferSize: number;
  bufferedTotal: number;
  state: EngineState;
  breakerOpen: boolean;
}

export interface EngineOptions {
  config?: Partial<EngineConfig>;
  /** Invoked synchronously, immediately after the IDLE -> LOCKED transition. */
  onSignal?: (signal: EngineSignal) => void;
}

/* ==========================================================================
 * CONSTANTS
 * ========================================================================== */

const POW10: readonly number[] = [
  1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000,
  10_000_000_000,
];

const ALL_STRATEGIES: readonly StrategyId[] = [
  'EVEN_ODD',
  'OVER_UNDER',
  'MATCHES',
  'RISE_FALL',
  'DIFFERS',
];

/** EXCLUDED BY POLICY: Higher/Lower, Touch/No Touch, Repeat. */
export const EXCLUDED_STRATEGIES: readonly string[] = [
  'HIGHER_LOWER',
  'TOUCH_NO_TOUCH',
  'REPEAT',
];

export const DEFAULT_CONFIG: EngineConfig = {
  symbol: 'R_100',
  pipSize: 2,
  bufferCapacity: 1000,
  windows: [100, 300, 500, 1000],
  overUnderBarrier: 4,
  matchesPrediction: 5,
  strategyOrder: ALL_STRATEGIES,
  minSamples: {
    EVEN_ODD: 1,
    OVER_UNDER: 1,
    MATCHES: 1,
    RISE_FALL: 2,
    DIFFERS: 1,
  },
  breaker: {
    enabled: true,
    maxConsecutiveAnomalies: 10,
    maxTickGapSeconds: 120,
  },
};

/* ==========================================================================
 * RING BUFFER — O(1) push, zero allocation, no shift()
 * ========================================================================== */

/**
 * Fixed-capacity ring buffer over typed arrays.
 * Index convention: `at(0)` = most recent tick, `at(k)` = k ticks back.
 * Callers MUST ensure `k < size`.
 */
export class TickBuffer {
  readonly capacity: number;

  private readonly prices: Float64Array;
  private readonly epochs: Float64Array;
  private readonly digits: Uint8Array;

  private cursor = 0; // next write slot
  private _size = 0;
  private _total = 0;

  constructor(capacity: number) {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new RangeError(`TickBuffer capacity must be a positive integer, got ${capacity}`);
    }
    this.capacity = capacity;
    this.prices = new Float64Array(capacity);
    this.epochs = new Float64Array(capacity);
    this.digits = new Uint8Array(capacity);
  }

  get size(): number {
    return this._size;
  }

  get total(): number {
    return this._total;
  }

  get full(): boolean {
    return this._size === this.capacity;
  }

  push(price: number, epoch: number, digit: number): void {
    const c = this.cursor;
    this.prices[c] = price;
    this.epochs[c] = epoch;
    this.digits[c] = digit;

    const next = c + 1;
    this.cursor = next === this.capacity ? 0 : next;

    if (this._size < this.capacity) this._size++;
    this._total++;
  }

  priceAt(i: number): number {
    return this.prices[this.slot(i)];
  }

  epochAt(i: number): number {
    return this.epochs[this.slot(i)];
  }

  digitAt(i: number): number {
    return this.digits[this.slot(i)];
  }

  private slot(i: number): number {
    let s = this.cursor - 1 - i;
    if (s < 0) s += this.capacity;
    return s;
  }

  clear(): void {
    this.cursor = 0;
    this._size = 0;
    this._total = 0;
  }
}

/**
 * Zero-copy windowed view over a TickBuffer. Instances are created once at
 * construction and reused forever — no per-tick allocation.
 */
export class WindowView {
  constructor(
    private readonly buffer: TickBuffer,
    readonly length: number,
  ) {}

  /** True once the buffer holds at least `length` ticks. Never used as a gate. */
  get ready(): boolean {
    return this.buffer.size >= this.length;
  }

  /** Number of ticks actually available in this window right now. */
  get available(): number {
    return Math.min(this.buffer.size, this.length);
  }

  priceAt(i: number): number {
    return this.buffer.priceAt(i);
  }

  epochAt(i: number): number {
    return this.buffer.epochAt(i);
  }

  digitAt(i: number): number {
    return this.buffer.digitAt(i);
  }
}

/* ==========================================================================
 * STRATEGY CONTEXT
 * ========================================================================== */

/**
 * Ephemeral evaluation context. Reused across ticks — do NOT retain a reference
 * to it inside a strategy (store scalars, not the object).
 */
export interface StrategyContext {
  readonly tick: NormalizedTick;
  readonly index: number;
  readonly bufferSize: number;
  readonly config: EngineConfig;

  /** k ticks back, k >= 1. Caller must guarantee k < bufferSize. */
  prevPrice(k: number): number;
  prevEpoch(k: number): number;
  prevDigit(k: number): number;

  /** Pre-warmed window view. Length must exist in `config.windows`. */
  window(length: number): WindowView;
}

interface Strategy {
  readonly id: StrategyId;
  readonly minSamples: number;
  evaluate(ctx: StrategyContext): StrategyResult | null;
}

/* ==========================================================================
 * ############################################################################
 * #                                                                          #
 * #   STRATEGY MATH — AUTHORITATIVE ZONE                                     #
 * #                                                                          #
 * #   Everything above and below this block is execution architecture.        #
 * #   THIS block is the only place strategy logic is allowed to live.         #
 * #                                                                          #
 * #   The five bodies below are the standard Deriv contract predicates and    #
 * #   exist so the engine runs out of the box. REPLACE each body with your    #
 * #   existing GainPlus+ XML / JS conditions VERBATIM. Do not soften them,    #
 * #   do not add frequency or momentum thresholds that were not there.        #
 * #                                                                          #
 * #   Only these 5 strategies are permitted. Higher/Lower, Touch/No Touch,    #
 * #   and Repeat must never appear here.                                      #
 * #                                                                          #
 * ############################################################################
 * ========================================================================== */

/** 1. EVEN / ODD */
const EVEN_ODD: Strategy = {
  id: 'EVEN_ODD',
  minSamples: 1,
  evaluate(ctx: StrategyContext): StrategyResult | null {
    const d = ctx.tick.digit;
    return {
      direction: (d & 1) === 0 ? 'EVEN' : 'ODD',
      meta: { digit: d },
    };
  },
};

/** 2. OVER / UNDER */
const OVER_UNDER: Strategy = {
  id: 'OVER_UNDER',
  minSamples: 1,
  evaluate(ctx: StrategyContext): StrategyResult | null {
    const d = ctx.tick.digit;
    const barrier = ctx.config.overUnderBarrier;
    if (d === barrier) return null; // exact hit is neither OVER nor UNDER
    return {
      direction: d > barrier ? 'OVER' : 'UNDER',
      meta: { digit: d, barrier },
    };
  },
};

/** 3. MATCHES */
const MATCHES: Strategy = {
  id: 'MATCHES',
  minSamples: 1,
  evaluate(ctx: StrategyContext): StrategyResult | null {
    const d = ctx.tick.digit;
    const prediction = ctx.config.matchesPrediction;
    if (d !== prediction) return null;
    return {
      direction: 'MATCHES',
      meta: { digit: d, prediction },
    };
  },
};

/** 4. RISE / FALL */
const RISE_FALL: Strategy = {
  id: 'RISE_FALL',
  minSamples: 2,
  evaluate(ctx: StrategyContext): StrategyResult | null {
    if (ctx.bufferSize < 2) return null;
    const current = ctx.tick.price;
    const previous = ctx.prevPrice(1);
    if (current === previous) return null; // flat tick is not a direction
    return {
      direction: current > previous ? 'RISE' : 'FALL',
      meta: { delta: current - previous },
    };
  },
};

/** 5. DIFFERS */
const DIFFERS: Strategy = {
  id: 'DIFFERS',
  minSamples: 1,
  evaluate(ctx: StrategyContext): StrategyResult | null {
    const d = ctx.tick.digit;
    const prediction = ctx.config.matchesPrediction;
    if (d === prediction) return null;
    return {
      direction: 'DIFFERS',
      meta: { digit: d, prediction },
    };
  },
};

const STRATEGY_REGISTRY: Readonly<Record<StrategyId, Strategy>> = {
  EVEN_ODD,
  OVER_UNDER,
  MATCHES,
  RISE_FALL,
  DIFFERS,
};

/* ==========================================================================
 * ########################  END STRATEGY MATH  ##############################
 * ========================================================================== */

/* ==========================================================================
 * ENGINE
 * ========================================================================== */

export class GainPlusEngine {
  private readonly config: EngineConfig;
  private readonly buffer: TickBuffer;
  private readonly views: Map<number, WindowView>;
  private readonly strategies: readonly Strategy[];
  private readonly minRequiredSamples: number;
  private readonly onSignal?: (signal: EngineSignal) => void;
  private readonly clock: () => number;

  /** ---- STATE MACHINE ---- */
  private state: EngineState = 'IDLE';
  private lockedSignal: EngineSignal | null = null;

  /** ---- CIRCUIT BREAKER ---- */
  private breakerOpen = false;
  private consecutiveAnomalies = 0;

  /** ---- INTEGRITY ---- */
  private lastEpoch = Number.NEGATIVE_INFINITY;

  /** ---- REENTRANCY ---- */
  private evaluating = false;

  /** ---- COUNTERS ---- */
  private signalSeq = 0;
  private ticksIngested = 0;
  private ticksRejected = 0;
  private signalsEmitted = 0;
  private wins = 0;
  private losses = 0;
  private consecutiveLosses = 0;
  private lastEvalMicros = 0;
  private maxEvalMicros = 0;

  /** Reused evaluation context — never reallocated. */
  private readonly ctx: StrategyContext;

  constructor(options: EngineOptions = {}) {
    this.config = GainPlusEngine.resolveConfig(options.config);
    this.onSignal = options.onSignal;

    this.buffer = new TickBuffer(this.config.bufferCapacity);

    // Pre-build every window view once.
    this.views = new Map<number, WindowView>();
    for (const w of this.config.windows) {
      if (!Number.isInteger(w) || w < 1) {
        throw new RangeError(`Window size must be a positive integer, got ${w}`);
      }
      if (w > this.config.bufferCapacity) {
        throw new RangeError(
          `Window ${w} exceeds bufferCapacity ${this.config.bufferCapacity}`,
        );
      }
      if (!this.views.has(w)) this.views.set(w, new WindowView(this.buffer, w));
    }
    // Always expose a 2-tick view for adjacent-tick comparisons.
    if (!this.views.has(2)) this.views.set(2, new WindowView(this.buffer, 2));

    // Resolve and validate strategy order.
    const ordered: Strategy[] = [];
    for (const id of this.config.strategyOrder) {
      const strategy = STRATEGY_REGISTRY[id];
      if (!strategy) {
        throw new Error(
          `Unknown or excluded strategy "${id}". Permitted: ${ALL_STRATEGIES.join(', ')}`,
        );
      }
      if (EXCLUDED_STRATEGIES.includes(id as unknown as string)) {
        throw new Error(`Strategy "${id}" is explicitly excluded from GainPlus+.`);
      }
      ordered.push(strategy);
    }
    if (ordered.length === 0) {
      throw new Error('strategyOrder must contain at least one strategy.');
    }
    this.strategies = ordered;

    this.minRequiredSamples = ordered.reduce(
      (max, s) => Math.max(max, this.config.minSamples[s.id] ?? s.minSamples),
      1,
    );

    this.clock =
      typeof performance !== 'undefined' && typeof performance.now === 'function'
        ? () => performance.now()
        : () => Date.now();

    // Single reused context object.
    const self = this;
    this.ctx = {
      tick: undefined as unknown as NormalizedTick,
      index: -1,
      bufferSize: 0,
      config: this.config,
      prevPrice(k: number): number {
        return self.buffer.priceAt(k);
      },
      prevEpoch(k: number): number {
        return self.buffer.epochAt(k);
      },
      prevDigit(k: number): number {
        return self.buffer.digitAt(k);
      },
      window(length: number): WindowView {
        const view = self.views.get(length);
        if (!view) {
          throw new RangeError(
            `Window ${length} was not pre-warmed. Declare it in config.windows.`,
          );
        }
        return view;
      },
    };
  }

  /* ------------------------------------------------------------------ */
  /* PUBLIC API                                                          */
  /* ------------------------------------------------------------------ */

  /**
   * Ingest one raw Deriv WebSocket tick. Fully synchronous. Returns the emitted
   * signal, or null if the tick produced no signal (or was rejected).
   *
   * Target: < 1 ms. Typical: single-digit microseconds.
   */
  ingest(raw: unknown): EngineSignal | null {
    if (this.evaluating) return null; // reentrancy guard

    const t0 = this.clock();

    const tick = this.normalize(raw);
    if (tick === null) {
      this.ticksRejected++;
      this.noteAnomaly();
      return null;
    }

    this.lastEpoch = tick.epoch;
    this.buffer.push(tick.price, tick.epoch, tick.digit);
    this.ticksIngested++;

    // Breaker open: keep buffering for warm-up, but never evaluate.
    if (this.breakerOpen) return null;

    // LOCKED: the circuit breaker for signal flicker. Ignore all strategy math.
    if (this.state === 'LOCKED') return null;

    // Cold-start guard: minimum samples only, never a wait for fresh ticks.
    if (this.buffer.size < this.minRequiredSamples) return null;

    // ---- EVALUATE ----
    let result: { strategy: StrategyId; direction: string; meta?: Record<string, number> } | null =
      null;

    this.evaluating = true;
    try {
      result = this.evaluate(tick);
    } finally {
      this.evaluating = false;
    }

    const t1 = this.clock();
    const micros = (t1 - t0) * 1000;
    this.lastEvalMicros = micros;
    if (micros > this.maxEvalMicros) this.maxEvalMicros = micros;

    if (result === null) return null;

    // ---- ATOMIC LOCK + EMIT ----
    this.signalSeq++;
    const signal: EngineSignal = {
      id: `${this.config.symbol}:${this.signalSeq}`,
      strategy: result.strategy,
      direction: result.direction,
      tick,
      tickIndex: this.buffer.total - 1,
      epoch: tick.epoch,
      emittedAt: Date.now(),
      evalMicros: micros,
      meta: result.meta,
    };

    this.state = 'LOCKED';
    this.lockedSignal = signal;
    this.signalsEmitted++;

    if (this.onSignal) this.onSignal(signal);

    return signal;
  }

  /**
   * Clear the locked signal and return the engine to IDLE.
   * The next incoming tick frame resumes evaluation immediately — no timer, no delay.
   *
   * @returns true if a locked cycle was actually settled.
   */
  resetCycle(outcome: CycleOutcome): boolean {
    if (this.state !== 'LOCKED') return false;

    if (outcome === 'WIN') {
      this.wins++;
      this.consecutiveLosses = 0;
    } else {
      this.losses++;
      this.consecutiveLosses++;
    }

    this.state = 'IDLE';
    this.lockedSignal = null;
    return true;
  }

  /** Manually close the circuit breaker after an integrity fault. */
  resetBreaker(): void {
    this.breakerOpen = false;
    this.consecutiveAnomalies = 0;
    this.lastEpoch = Number.NEGATIVE_INFINITY;
  }

  /** Full teardown: buffer, lock, breaker, counters. */
  forceReset(): void {
    this.buffer.clear();
    this.state = 'IDLE';
    this.lockedSignal = null;
    this.breakerOpen = false;
    this.consecutiveAnomalies = 0;
    this.lastEpoch = Number.NEGATIVE_INFINITY;
    this.lastEvalMicros = 0;
    this.maxEvalMicros = 0;
  }

  getState(): EngineState {
    return this.state;
  }

  isLocked(): boolean {
    return this.state === 'LOCKED';
  }

  isBreakerOpen(): boolean {
    return this.breakerOpen;
  }

  getLockedSignal(): EngineSignal | null {
    return this.lockedSignal;
  }

  getBuffer(): TickBuffer {
    return this.buffer;
  }

  getWindow(length: number): WindowView {
    const view = this.views.get(length);
    if (!view) throw new RangeError(`Window ${length} is not pre-warmed.`);
    return view;
  }

  /** True once every window declared in config.windows is fully populated. */
  isFullyWarmed(): boolean {
    for (const view of this.views.values()) {
      if (view.length <= 2) continue; // internal helper view
      if (!view.ready) return false;
    }
    return true;
  }

  getStats(): EngineStats {
    return {
      ticksIngested: this.ticksIngested,
      ticksRejected: this.ticksRejected,
      signalsEmitted: this.signalsEmitted,
      wins: this.wins,
      losses: this.losses,
      consecutiveLosses: this.consecutiveLosses,
      lastEvalMicros: this.lastEvalMicros,
      maxEvalMicros: this.maxEvalMicros,
      bufferSize: this.buffer.size,
      bufferedTotal: this.buffer.total,
      state: this.state,
      breakerOpen: this.breakerOpen,
    };
  }

  /**
   * Deterministic offline replay. Given the same tick sequence as a live feed,
   * this yields byte-identical signals — use it to verify the refactor against
   * your existing engine before switching over.
   */
  replay(ticks: readonly unknown[]): EngineSignal[] {
    const out: EngineSignal[] = [];
    for (let i = 0; i < ticks.length; i++) {
      const signal = this.ingest(ticks[i]);
      if (signal !== null) out.push(signal);
    }
    return out;
  }

  /* ------------------------------------------------------------------ */
  /* INTERNALS                                                           */
  /* ------------------------------------------------------------------ */

  private evaluate(
    tick: NormalizedTick,
  ): { strategy: StrategyId; direction: string; meta?: Record<string, number> } | null {
    const ctx = this.ctx;
    (ctx as { tick: NormalizedTick }).tick = tick;
    (ctx as { index: number }).index = this.buffer.total - 1;
    (ctx as { bufferSize: number }).bufferSize = this.buffer.size;

    const strategies = this.strategies;
    for (let i = 0; i < strategies.length; i++) {
      const strategy = strategies[i];
      const required = this.config.minSamples[strategy.id] ?? strategy.minSamples;
      if (this.buffer.size < required) continue;

      const result = strategy.evaluate(ctx);
      if (result !== null) {
        return { strategy: strategy.id, direction: result.direction, meta: result.meta };
      }
    }
    return null;
  }

  private normalize(raw: unknown): NormalizedTick | null {
    if (raw === null || typeof raw !== 'object') return null;

    const r = raw as RawTick;

    // ---- PRICE ----
    const rawPrice = r.quote !== undefined && r.quote !== null ? r.quote : r.price;
    if (rawPrice === undefined || rawPrice === null || rawPrice === '') return null;

    const price = typeof rawPrice === 'number' ? rawPrice : Number(rawPrice);
    if (!Number.isFinite(price) || price <= 0) return null;

    // ---- EPOCH ----
    const rawEpoch = r.epoch !== undefined && r.epoch !== null ? r.epoch : r.timestamp;
    if (rawEpoch === undefined || rawEpoch === null || rawEpoch === '') return null;

    const epoch = typeof rawEpoch === 'number' ? rawEpoch : Number(rawEpoch);
    if (!Number.isFinite(epoch) || epoch <= 0) return null;

    // Stale / out-of-order tick — drop silently into the anomaly counter.
    if (epoch < this.lastEpoch) return null;

    // Stream discontinuity (WS drop, reconnect, sleep).
    if (
      this.config.breaker.enabled &&
      this.lastEpoch !== Number.NEGATIVE_INFINITY &&
      epoch - this.lastEpoch > this.config.breaker.maxTickGapSeconds
    ) {
      this.noteAnomaly();
    } else {
      this.consecutiveAnomalies = 0;
    }

    return {
      price,
      epoch,
      digit: this.lastDigit(price),
      symbol: typeof r.symbol === 'string' && r.symbol.length > 0 ? r.symbol : this.config.symbol,
      receivedAt: Date.now(),
    };
  }

  /** Last digit at the configured pip size. Computed once per tick, never per strategy. */
  private lastDigit(price: number): number {
    const pip = this.config.pipSize;
    const factor = POW10[pip];
    if (factor === undefined) {
      throw new RangeError(`pipSize ${pip} is out of range (0-10).`);
    }
    const scaled = Math.round(price * factor);
    const d = scaled % 10;
    return d < 0 ? d + 10 : d;
  }

  private noteAnomaly(): void {
    if (!this.config.breaker.enabled) return;
    this.consecutiveAnomalies++;
    if (this.consecutiveAnomalies >= this.config.breaker.maxConsecutiveAnomalies) {
      this.breakerOpen = true;
    }
  }

  private static resolveConfig(partial?: Partial<EngineConfig>): EngineConfig {
    const base = DEFAULT_CONFIG;
    if (!partial) return base;

    const windows = partial.windows ?? base.windows;
    const bufferCapacity = partial.bufferCapacity ?? base.bufferCapacity;

    if (bufferCapacity < Math.max(...windows)) {
      throw new RangeError(
        `bufferCapacity ${bufferCapacity} must be >= largest window ${Math.max(...windows)}.`,
      );
    }

    const pipSize = partial.pipSize ?? base.pipSize;
    if (!Number.isInteger(pipSize) || pipSize < 0 || pipSize > 10) {
      throw new RangeError(`pipSize must be an integer 0-10, got ${pipSize}.`);
    }

    const overUnderBarrier = partial.overUnderBarrier ?? base.overUnderBarrier;
    if (!Number.isInteger(overUnderBarrier) || overUnderBarrier < 0 || overUnderBarrier > 9) {
      throw new RangeError(`overUnderBarrier must be a digit 0-9, got ${overUnderBarrier}.`);
    }

    const matchesPrediction = partial.matchesPrediction ?? base.matchesPrediction;
    if (!Number.isInteger(matchesPrediction) || matchesPrediction < 0 || matchesPrediction > 9) {
      throw new RangeError(`matchesPrediction must be a digit 0-9, got ${matchesPrediction}.`);
    }

    return {
      symbol: partial.symbol ?? base.symbol,
      pipSize,
      bufferCapacity,
      windows,
      overUnderBarrier,
      matchesPrediction,
      strategyOrder: partial.strategyOrder ?? base.strategyOrder,
      minSamples: { ...base.minSamples, ...(partial.minSamples ?? {}) },
      breaker: { ...base.breaker, ...(partial.breaker ?? {}) },
    };
  }
}

export default GainPlusEngine;
