// AUTO-SYNCED from private repo. Do not edit here -- edits are overwritten on the next sync.
// Source of record: engine/scripts/evolve.mjs in the private repo.

// The evolutionary bot-training runner (docs/plans/evolutionary-bot-training-track1-plan.md,
// Checkpoint 2, 2026-08-16). Plays per-region weight vectors against a locked baseline anchor
// via bot-vs-bot games, writes incremental JSONL + a rewritten summary after each region's
// generation completes.
//
// Usage from engine/ :
//   npm run build && node scripts/evolve.mjs [OPTIONS]
//
// Options (all optional):
//   --generations N       How many generations to run before stopping. Default: 20.
//   --regions "a,b,c"     Comma-separated region names (must match EVOLVING_REGIONS). Default:
//                         all regions EXCEPT those in DEFAULT_EXCLUDED_REGIONS (see below).
//                         Explicit --regions overrides the exclusion list -- listing an
//                         excluded region here IS the opt-in.
//   --includeExcluded     Include regions from DEFAULT_EXCLUDED_REGIONS in the default set.
//                         Only meaningful when --regions is NOT specified. See that constant's
//                         own doc comment for why regions get excluded (mirror-match pathology
//                         that produces no fitness signal).
//   --matchupGames N|"a/b"
//                         Games per matchup. A single number sets BOTH the champion round-robin
//                         and intra-population counts to N. The "a/b" form sets champion games
//                         to a and peer games to b (e.g. "4/2"). Default: adaptive schedule by
//                         generation number -- gens 1-3 use 4/2, gens 4-8 use 6/3, gen 9+ uses
//                         8/4 (see ADAPTIVE_MATCHUP_SCHEDULE). Explicit --matchupGames disables
//                         the adaptive schedule and uses the given value for every generation.
//   --populationSize N    Population size per region (min 3: 1 baseline + 2 evolvers). Default:
//                         8 (down from Checkpoint 2's 10 -- one baseline slot + 7 evolvers,
//                         quadratic savings on peer matchups). Smoke tests use 3-5; every
//                         extra slot multiplies work by roughly (slot count - 1) since peers
//                         are all-vs-all.
//   --maxTurns N|"default=X,Region=Y,..."
//                         Hard cap on turns per game. A single number sets the cap for every
//                         region. The comma-separated form lets specific regions have their
//                         OWN caps (e.g. "default=200,Cald=100" caps Cald games at 100 turns
//                         while every other region uses 200). Region names in the CLI value
//                         match EVOLVING_REGIONS exactly. Default: 200 for all regions.
//                         Games hitting the cap count as draws for fitness.
//   --maxActions N        Hard cap on total actions across a game. Default: 20000. Rarely hit
//                         at reasonable maxTurns; kept for symmetry with playRandomGame's own
//                         safety net.
//   --gameTimeoutSec N    Wall-clock cap per game (seconds). A game that runs longer is
//                         abandoned and counted as 'stuck'. Default: 300 (5 min). Belt-and-
//                         braces against decision-heavy games that maxTurns can't shortcut --
//                         a game hitting maxTurns=200 with expensive per-move search can
//                         still take an hour, which is what killed the Underneath run
//                         2026-08-18. Set to 0 to disable.
//   --workers N           Parallel worker processes (Checkpoint 2.5). Default: 1
//                         (single-process, backward-compatible with the C2 sequential runner).
//                         Each worker is one child_process.fork of evolve-worker.mjs and pins
//                         one CPU core when actively evaluating. Games are deterministic per
//                         (weights, seed), so parallelism can't affect outcomes -- only wall
//                         time. Recommended: start with 4, ramp up while watching Task
//                         Manager's CPU + memory (each worker loads its own ~10-20MB card DB
//                         copy at startup). Ceiling is (physical cores - 1 or 2) to leave the
//                         machine responsive; N > physical cores is counterproductive.
//   --out DIR             Where to write bot-weights.json / evolution-*.jsonl / summary.txt.
//                         Default: engine/ (mirrors the fuzz harness's own output layout).
//   --decks DIR           Where to load the 13 shipped default-deck JSONs from.
//                         Default: app/resources/default-decks.
//
// INCREMENTAL, same as fuzz.mjs: a Ctrl-C at any moment loses at most the in-flight region's
// current generation (still working). Every completed region-generation appends immediately to
// evolution-generations.jsonl and rewrites evolution-summary.txt, both readable at any moment.

import { readFileSync, existsSync, unlinkSync, appendFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { fork } from 'node:child_process';

// Same reason fuzz.mjs does this: side-effect import to register every card's effect. Without
// it, getCardEffectByName returns undefined for every card and the evaluator scores broken.
import '../engine.bundle.js';
import { buildCardDatabase } from '../engine.bundle.js';
import {
  EVOLVING_REGIONS,
  loadWeightsFile,
  writeWeightsFile,
} from '../engine.bundle.js';
import {
  seedRegionPopulation,
  warmStartRegionPopulation,
  pickChampion,
  buildNextGeneration,
  SLOT_BASELINE,
} from '../engine.bundle.js';
import { mutateWeights } from '../engine.bundle.js';
import {
  playMatchup,
  playBotVsBotGame,
  combineFitness,
  seedFor,
  pointsFor,
  CHAMPION_ROUND_ROBIN_GAMES,
  POPULATION_MATCH_GAMES,
} from '../engine.bundle.js';

/** Seed per game (2026-08-25 change): genuinely-random uint32 via `Math.random()`. Every game
 *  gets a fresh shuffle + fresh opening hands. Replaces the deterministic `seedFor(gen, region,
 *  cand, opp, gameIdx)` game-seed derivation to prevent over-fitting mutations to specific
 *  card-draw patterns -- see roundrobin.mjs's `randomGameSeed` for the full rationale. `seedFor`
 *  itself stays used for non-game deterministic uses (warm-start pop below, selectionRngFor). */
function randomGameSeed() {
  return (Math.floor(Math.random() * 4294967296)) >>> 0;
}
import {
  initJsonlFile,
  rotateJsonlFile,
  appendGenerationRow,
  writeSummary,
  buildGenerationRow,
} from '../engine.bundle.js';
import { createSeededRng } from '../engine.bundle.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(here, '..', '..');

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : process.argv[i + 1];
}

const GENERATIONS = Number(arg('generations', 20));
const REGIONS_FILTER = arg('regions', null);
const INCLUDE_EXCLUDED = process.argv.includes('--includeExcluded');
const MATCHUP_GAMES_OVERRIDE = arg('matchupGames', null);
const POPULATION_SIZE_OVERRIDE = arg('populationSize', null);
const MAX_TURNS_ARG = arg('maxTurns', null);
const MAX_ACTIONS = arg('maxActions', null);
const GAME_TIMEOUT_SEC = Number(arg('gameTimeoutSec', 300));
const WORKERS = Number(arg('workers', 1));
const OUT_DIR = path.resolve(arg('out', path.join(here, '..')));
const DECKS_DIR = path.resolve(arg('decks', path.join(REPO_ROOT, 'app', 'resources', 'default-decks')));

/** Regions excluded from the default region set (2026-08-18, after gen 1's Underneath disaster).
 *  Underneath's mirror-match is the extreme case of the "playing not to lose" pathology: both
 *  sides are pure defense/stall with no closer, games run to maxTurns without deciding, per-game
 *  cost balloons because the bot's lookahead has many defensive options to evaluate each turn.
 *  Every Underneath game in gen 1 was still in-flight after 30+ hours, at which point the run
 *  was killed. Skipping by default; opt back in with --includeExcluded (or explicit --regions).
 *  See CURRENT-INVESTIGATION.md for the three-path escalation ladder we plan to explore for
 *  Underneath specifically (deck redesign / cross-region training / region-scoped per-card
 *  weights). */
const DEFAULT_EXCLUDED_REGIONS = new Set(['Underneath']);

/** Adaptive matchup-games schedule (2026-08-18). Early generations have all-baseline populations
 *  where fitness precision doesn't matter (any distinguishing signal is pure noise); mid gens
 *  need enough games to rank a diverging population; late gens need enough games to distinguish
 *  fine differences between well-tuned champions. Spending 8/4 games on gen 1 is measurement of
 *  noise at high precision -- wasted compute. Explicit --matchupGames disables this schedule
 *  and uses the given value for every generation. */
const ADAPTIVE_MATCHUP_SCHEDULE = [
  { fromGen: 1, championGames: 4, peerGames: 2 },
  { fromGen: 4, championGames: 6, peerGames: 3 },
  { fromGen: 9, championGames: 8, peerGames: 4 },
];
function adaptiveMatchupGames(generation) {
  let selected = ADAPTIVE_MATCHUP_SCHEDULE[0];
  for (const rung of ADAPTIVE_MATCHUP_SCHEDULE) {
    if (generation >= rung.fromGen) selected = rung;
  }
  return selected;
}

/** Parse --matchupGames: accepts either a single number (both counts = N) or "a/b" (champion=a,
 *  peer=b). Returns null if the CLI arg wasn't specified -- caller then falls back to the
 *  adaptive schedule. */
function parseMatchupGamesOverride(raw) {
  if (raw === null) return null;
  if (raw.includes('/')) {
    const [c, p] = raw.split('/').map((s) => Number(s.trim()));
    if (!Number.isFinite(c) || !Number.isFinite(p)) throw new Error(`Invalid --matchupGames "${raw}" -- use N or a/b`);
    return { championGames: c, peerGames: p };
  }
  const n = Number(raw);
  if (!Number.isFinite(n)) throw new Error(`Invalid --matchupGames "${raw}"`);
  return { championGames: n, peerGames: n };
}
const MATCHUP_GAMES_OVERRIDE_PARSED = parseMatchupGamesOverride(MATCHUP_GAMES_OVERRIDE);

/** Parse --maxTurns: accepts either a single number (every region uses N) or "default=X,Region=Y,..."
 *  Returns { default, perRegion } where perRegion is a Map<string, number>. Regions absent from
 *  perRegion fall back to `default`. */
function parseMaxTurnsCap(raw) {
  if (raw === null) return { default: 200, perRegion: new Map() };
  if (!raw.includes('=')) {
    const n = Number(raw);
    if (!Number.isFinite(n)) throw new Error(`Invalid --maxTurns "${raw}"`);
    return { default: n, perRegion: new Map() };
  }
  const perRegion = new Map();
  let defaultCap = 200;
  for (const entry of raw.split(',')) {
    const [key, val] = entry.split('=').map((s) => s.trim());
    const n = Number(val);
    if (!Number.isFinite(n)) throw new Error(`Invalid --maxTurns entry "${entry}"`);
    if (key === 'default') defaultCap = n;
    else perRegion.set(key, n);
  }
  return { default: defaultCap, perRegion };
}
const MAX_TURNS_CAP = parseMaxTurnsCap(MAX_TURNS_ARG);

function maxTurnsForRegion(region) {
  return MAX_TURNS_CAP.perRegion.get(region) ?? MAX_TURNS_CAP.default;
}

function perGameCapsForRegion(region) {
  return {
    maxTurns: maxTurnsForRegion(region),
    maxActions: MAX_ACTIONS !== null ? Number(MAX_ACTIONS) : undefined,
    // 0 disables the wall-clock cap entirely (opt-out for long-running runs).
    timeoutMs: GAME_TIMEOUT_SEC > 0 ? GAME_TIMEOUT_SEC * 1000 : undefined,
  };
}

const CARDS_PATH = path.join(REPO_ROOT, 'data-pipeline', 'output', 'cards_final.json');
const WEIGHTS_JSON_PATH = path.join(REPO_ROOT, 'engine', 'data', 'bot-weights.json');
const JSONL_PATH = path.join(OUT_DIR, 'evolution-generations.jsonl');
const SUMMARY_PATH = path.join(OUT_DIR, 'evolution-summary.txt');
const ERRORS_JSONL_PATH = path.join(OUT_DIR, 'evolution-errors.jsonl');
const STOP_FLAG = path.join(OUT_DIR, 'evolution-STOP.txt');

// Map deck-id -> region name, verbatim from Play.tsx's DEFAULT_DECK_REGION (interface contract
// with the parallel session's work). Kept in-sync manually; if a new default deck ships, add
// its mapping here alongside app-side updates.
const DECK_ID_TO_REGION = {
  'default-arderial': 'Arderial',
  'default-bograth': 'Bograth',
  'default-cald': 'Cald',
  'default-core': 'Core',
  'default-dresh': "d'Resh",
  'default-kybars-teeth': "Kybar's Teeth",
  'default-nar': 'Nar',
  'default-naroom': 'Naroom',
  'default-orothe': 'Orothe',
  'default-paradwyn': 'Paradwyn',
  'default-underneath': 'Underneath',
  'default-universal': 'Universal',
  'default-weave': 'Weave',
};

function expandDeck(deckJson) {
  // Deck JSON: { magiKeys: number[], cardCounts: [key, count][] } -> engine-shape flat arrays.
  const deckCardKeys = [];
  for (const [key, count] of deckJson.cardCounts) {
    for (let i = 0; i < count; i += 1) deckCardKeys.push(key);
  }
  return { deckCardKeys, magiOrder: [...deckJson.magiKeys] };
}

function loadRegionalDecks() {
  const decksByRegion = new Map();
  for (const [deckId, region] of Object.entries(DECK_ID_TO_REGION)) {
    const filePath = path.join(DECKS_DIR, `${deckId}.json`);
    if (!existsSync(filePath)) {
      console.error(`Missing deck file: ${filePath}`);
      process.exit(1);
    }
    const parsed = JSON.parse(readFileSync(filePath, 'utf-8'));
    decksByRegion.set(region, expandDeck(parsed));
  }
  return decksByRegion;
}

function selectedRegions() {
  // Explicit --regions ALWAYS wins over the exclusion list -- listing an excluded region IS the
  // opt-in for that region. The exclusion list only affects the DEFAULT (no --regions) set.
  if (REGIONS_FILTER) {
    const wanted = new Set(REGIONS_FILTER.split(',').map((s) => s.trim()));
    const filtered = EVOLVING_REGIONS.filter((r) => wanted.has(r));
    if (filtered.length === 0) {
      console.error(`--regions "${REGIONS_FILTER}" matched none of EVOLVING_REGIONS: ${EVOLVING_REGIONS.join(', ')}`);
      process.exit(1);
    }
    return filtered;
  }
  if (INCLUDE_EXCLUDED) return EVOLVING_REGIONS;
  return EVOLVING_REGIONS.filter((r) => !DEFAULT_EXCLUDED_REGIONS.has(r));
}

// ============================================================================
// Worker pool (Checkpoint 2.5)
// ============================================================================

/** Simple Promise-based worker pool over child_process.fork -- each worker is a separate Node
 *  process running evolve-worker.mjs, communicating via IPC .send()/.on('message'). A worker
 *  loads the card DB once at startup, then loops on job messages until told to shut down.
 *
 *  A `runJob` call returns a Promise that resolves with the game outcome. Jobs queue if all
 *  workers are busy; the first free worker picks the head of the queue. Workers are added to
 *  the free pool only AFTER emitting 'ready' -- so early jobs don't serialize through the
 *  first-loaded worker while others are still parsing the card DB. */
class WorkerPool {
  constructor(size, workerScriptPath, cardsPath) {
    this.workers = [];
    this.freeWorkers = [];
    this.queue = [];
    this.pending = new Map(); // jobId -> {resolve, reject}
    this.nextJobId = 1;
    this.stopRequested = false;
    this.readyPromise = new Promise((resolveReady) => {
      let readyCount = 0;
      for (let i = 0; i < size; i += 1) {
        const w = fork(workerScriptPath, [cardsPath], { stdio: 'inherit' });
        w.on('message', (msg) => this._onMessage(w, msg));
        w.on('exit', (code, signal) => {
          if (code !== 0 && code !== null) {
            console.error(`worker ${w.pid} exited with code ${code} (signal ${signal})`);
          }
        });
        w.on('error', (err) => console.error(`worker ${w.pid} error:`, err));
        this.workers.push(w);
        w._onReady = () => {
          readyCount += 1;
          this.freeWorkers.push(w);
          this._drain();
          if (readyCount === size) resolveReady();
        };
      }
    });
  }

  _onMessage(worker, msg) {
    if (msg.type === 'ready') {
      if (typeof worker._onReady === 'function') worker._onReady();
    } else if (msg.type === 'result') {
      const p = this.pending.get(msg.jobId);
      if (!p) return;
      this.pending.delete(msg.jobId);
      if (msg.error) {
        // Dedupe per-message: log the FIRST occurrence of each distinct error message
        // (with a running count), not every occurrence. When a region-wide bug triggers on
        // every game (the d'Resh case, 2026-08-16) this collapses hundreds of identical
        // lines into a single labeled counter -- otherwise the terminal fills before you
        // even notice something's wrong.
        if (!this.errorCounts) this.errorCounts = new Map();
        const seen = this.errorCounts.get(msg.error);
        if (!seen) {
          this.errorCounts.set(msg.error, 1);
          console.error(`  [first occurrence] game threw: ${msg.error} -- treating as stuck`);
        } else {
          this.errorCounts.set(msg.error, seen + 1);
          // Log a "still hitting X" message every 25 occurrences so a persistent bug is
          // visible but not spammy.
          if ((seen + 1) % 25 === 0) {
            console.error(`  [${seen + 1} occurrences so far] "${msg.error.slice(0, 80)}${msg.error.length > 80 ? '...' : ''}"`);
          }
        }
        // Persist to JSONL for post-run diagnosis (2026-08-19, after realizing console-only
        // logging loses data if the terminal closes). One row per occurrence, append-only.
        try {
          appendFileSync(ERRORS_JSONL_PATH, JSON.stringify({ timestamp: new Date().toISOString(), signature: msg.error }) + '\n', 'utf-8');
        } catch { /* file-write failure isn't fatal to the run */ }
      }
      p.resolve(msg.result);
      this.freeWorkers.push(worker);
      // Progress hook (C2.6): evaluatePopulationParallel installs this to log
      // "N/M games done" periodically. Called AFTER resolve so the counter reflects a
      // truly-completed job. Null when no evaluation is currently owning the pool.
      if (this._onJobCompleteHook) this._onJobCompleteHook();
      this._drain();
    }
  }

  _drain() {
    // stopRequested short-circuits BOTH new dispatch and existing queue -- Promise.all in
    // evaluatePopulationParallel is waiting on the pending jobs to resolve, so we must drain
    // the queue ourselves by resolving each queued job as a stuck GameResult rather than
    // leaving it pending forever (which would deadlock the shutdown path).
    if (this.stopRequested) {
      while (this.queue.length > 0) {
        const { jobId } = this.queue.shift();
        const p = this.pending.get(jobId);
        if (p) {
          this.pending.delete(jobId);
          p.resolve({
            outcome: 'stuck',
            turnsPlayed: 0,
            actionsApplied: 0,
            advancePhaseCount: 0,
            declareAttackCount: 0,
            playCardCount: 0,
            usePowerCount: 0,
          });
        }
      }
      return;
    }
    while (this.queue.length > 0 && this.freeWorkers.length > 0) {
      const worker = this.freeWorkers.shift();
      const { jobId, payload } = this.queue.shift();
      worker.send({ type: 'job', jobId, ...payload });
    }
  }

  /** Halts new dispatch and drains the queue to 'stuck' outcomes. In-flight jobs already
   *  on a worker still complete (there's no cancel-in-worker mechanism -- the game runs to
   *  its own natural end), but they typically finish within a game-time (~30s) and Promise.all
   *  resolves after the last one. Every subsequent runJob() call also short-circuits to 'stuck'
   *  so anything queued after a stop request doesn't hang. */
  requestStop() {
    this.stopRequested = true;
    this._drain();
  }

  /** Hard-stop: drain BOTH the queue AND any pending in-flight jobs to 'stuck' results. Used
   *  by the 'hard' stop mode before SIGKILL'ing workers -- workers that die with jobs in flight
   *  can never send back their result, so without this the master's Promise.all in
   *  evaluatePopulationParallel would hang forever. Every pending Promise resolves 'stuck'
   *  RIGHT NOW; the actual SIGKILL happens in the caller. */
  drainAllPendingAsStuck() {
    const stuckResult = {
      outcome: 'stuck',
      turnsPlayed: 0,
      actionsApplied: 0,
      advancePhaseCount: 0,
      declareAttackCount: 0,
      playCardCount: 0,
      usePowerCount: 0,
    };
    for (const [jobId, p] of this.pending.entries()) {
      p.resolve(stuckResult);
    }
    this.pending.clear();
    this.queue.length = 0;
  }

  runJob(payload) {
    const jobId = this.nextJobId;
    this.nextJobId += 1;
    return new Promise((resolve, reject) => {
      this.pending.set(jobId, { resolve, reject });
      this.queue.push({ jobId, payload });
      this._drain();
    });
  }

  async ready() {
    return this.readyPromise;
  }

  async shutdown() {
    for (const w of this.workers) {
      try { w.send({ type: 'shutdown' }); } catch { /* worker already dead */ }
    }
    // Give workers a moment to exit cleanly; don't wait forever.
    await new Promise((r) => setTimeout(r, 200));
    for (const w of this.workers) {
      if (!w.killed) w.kill();
    }
  }
}

async function evaluatePopulationParallel(pool, population, region, regionIndex, deck, magiOrder, generation) {
  const baselineWeights = population.slots[SLOT_BASELINE].weights;
  const caps = perGameCapsForRegion(region);
  const games = MATCHUP_GAMES_OVERRIDE_PARSED ?? adaptiveMatchupGames(generation);
  const championGames = games.championGames;
  const peerGames = games.peerGames;

  // Build every game job needed for this region's generation in one flat list. Each job
  // knows which slot's fitness it contributes to and whether the credit goes to that slot's
  // baseline or peer half of the fitness formula.
  const jobs = [];
  for (let evolverSlot = 0; evolverSlot < population.slots.length; evolverSlot += 1) {
    const evolverWeights = population.slots[evolverSlot].weights;
    // vs baseline
    for (let g = 0; g < championGames; g += 1) {
      jobs.push({
        evolverSlot,
        matchupType: 'baseline',
        payload: {
          region, deck, magiOrder,
          p1Weights: evolverWeights,
          p2Weights: baselineWeights,
          seed: randomGameSeed(),
          maxTurns: caps.maxTurns,
          maxActions: caps.maxActions,
          timeoutMs: caps.timeoutMs,
        },
      });
    }
    // vs peers
    for (let opponentSlot = 0; opponentSlot < population.slots.length; opponentSlot += 1) {
      if (opponentSlot === SLOT_BASELINE) continue;
      if (opponentSlot === evolverSlot) continue;
      const opponentWeights = population.slots[opponentSlot].weights;
      for (let g = 0; g < peerGames; g += 1) {
        jobs.push({
          evolverSlot,
          matchupType: 'peer',
          payload: {
            region, deck, magiOrder,
            p1Weights: evolverWeights,
            p2Weights: opponentWeights,
            seed: randomGameSeed(),
            maxTurns: caps.maxTurns,
            maxActions: caps.maxActions,
            timeoutMs: caps.timeoutMs,
          },
        });
      }
    }
  }

  // Mid-region progress tracking (Checkpoint 2.6, 2026-08-18). Prior to this, a region-generation
  // was totally silent from start to end -- Paradwyn ran 9 hours with no output, no way to tell
  // if it was stuck or just slow. Now the pool logs completion progress every N jobs or every T
  // seconds (whichever first) so the user always has an ETA.
  const totalJobs = jobs.length;
  const regionStartMs = Date.now();
  let completedJobs = 0;
  let lastLogAtMs = regionStartMs;
  let lastLogAtCount = 0;
  const LOG_EVERY_JOBS = Math.max(20, Math.floor(totalJobs / 20)); // ~20 log lines per region max
  const LOG_EVERY_MS = 90_000; // 90s
  const onJobComplete = () => {
    completedJobs += 1;
    const nowMs = Date.now();
    const shouldLog = (completedJobs - lastLogAtCount) >= LOG_EVERY_JOBS || (nowMs - lastLogAtMs) >= LOG_EVERY_MS;
    if (shouldLog && completedJobs < totalJobs) {
      lastLogAtCount = completedJobs;
      lastLogAtMs = nowMs;
      const pct = (completedJobs / totalJobs * 100).toFixed(0);
      const elapsedS = ((nowMs - regionStartMs) / 1000).toFixed(0);
      const remainingS = completedJobs > 0
        ? (((nowMs - regionStartMs) / completedJobs) * (totalJobs - completedJobs) / 1000).toFixed(0)
        : '?';
      process.stdout.write(`  region ${region} gen ${generation}: ${completedJobs}/${totalJobs} games (${pct}%), ${elapsedS}s elapsed, ~${remainingS}s remaining\n`);
    }
  };
  pool._onJobCompleteHook = onJobComplete;

  // Fire every job at the pool; Promise.all waits for the last to finish. Pool serializes
  // internally when workers are busier than the queue; from here it looks like one call. Each
  // result is a rich GameResult (C2.6 telemetry) -- outcome + turn/action/verb counts.
  const results = await Promise.all(jobs.map((j) => pool.runJob(j.payload)));

  pool._onJobCompleteHook = null;

  // Broken-region canary (2026-08-16, after the d'Resh crash): if the vast majority of games
  // in a region resolved as 'stuck', the region's fitness numbers are noise from a persistent
  // engine bug -- report and let the outer loop decide whether to continue. Threshold is
  // deliberately high (50%): a well-evolved population may still produce some stuck games
  // (rare edge cases), but half a region turning to draws indicates a real problem.
  const stuckCount = results.filter((r) => r.outcome === 'stuck').length;
  if (stuckCount / results.length > 0.5) {
    console.error(`\n  !! region ${region} had ${stuckCount}/${results.length} 'stuck' outcomes -- fitness data is unreliable. Region will be SKIPPED this generation (previous champion, if any, remains).`);
    // Signal to the outer loop via a flag on the population itself -- caller checks this.
    population._skipThisGen = true;
    return;
  }

  // Aggregate outcomes into per-slot baseline / peer buckets, then compute fitness identically
  // to the sequential path in evaluatePopulation.
  const perSlot = new Map(); // evolverSlot -> {baselineWins, baselineGames, peerWins, peerGames}
  // Region-wide telemetry: counts of every outcome kind + running totals for per-game averages.
  // These get bundled into the GenerationRow so the summary can print them per-region.
  const regionStats = {
    outcomeCounts: { p1: 0, p2: 0, p1ByTiebreak: 0, p2ByTiebreak: 0, drawByDefeat: 0, drawByCap: 0, stuck: 0 },
    turnsSum: 0, turnsMax: 0,
    actionsSum: 0,
    advancePhaseSum: 0,
    declareAttackSum: 0,
    playCardSum: 0,
    usePowerSum: 0,
    gameCount: results.length,
  };
  for (let i = 0; i < jobs.length; i += 1) {
    const j = jobs[i];
    const r = results[i];
    const points = pointsFor(r.outcome, 'p1');
    const entry = perSlot.get(j.evolverSlot) ?? { baselineWins: 0, baselineGames: 0, peerWins: 0, peerGames: 0 };
    if (j.matchupType === 'baseline') {
      entry.baselineWins += points;
      entry.baselineGames += 1;
    } else {
      entry.peerWins += points;
      entry.peerGames += 1;
    }
    perSlot.set(j.evolverSlot, entry);

    // Region-wide telemetry aggregation.
    regionStats.outcomeCounts[r.outcome] += 1;
    regionStats.turnsSum += r.turnsPlayed;
    if (r.turnsPlayed > regionStats.turnsMax) regionStats.turnsMax = r.turnsPlayed;
    regionStats.actionsSum += r.actionsApplied;
    regionStats.advancePhaseSum += r.advancePhaseCount;
    regionStats.declareAttackSum += r.declareAttackCount;
    regionStats.playCardSum += r.playCardCount;
    regionStats.usePowerSum += r.usePowerCount;
  }
  // Stash on the population for the outer loop to read into the GenerationRow.
  population._regionStats = regionStats;

  for (let evolverSlot = 0; evolverSlot < population.slots.length; evolverSlot += 1) {
    const slot = population.slots[evolverSlot];
    const e = perSlot.get(evolverSlot) ?? { baselineWins: 0, baselineGames: 0, peerWins: 0, peerGames: 0 };
    const winRateVsBaseline = e.baselineGames > 0 ? e.baselineWins / e.baselineGames : 0.5;
    const winRateVsPeers = e.peerGames > 0 ? e.peerWins / e.peerGames : 0.5;
    slot.fitness = combineFitness(winRateVsBaseline, winRateVsPeers);
    slot._winRateVsBaseline = winRateVsBaseline;
    process.stdout.write(`  gen ${generation} region ${region} slot ${evolverSlot}: fitness ${slot.fitness.toFixed(3)} (vs-baseline ${(winRateVsBaseline * 100).toFixed(0)}%)\n`);
  }
}

function evaluatePopulation(cardDb, population, region, regionIndex, deck, magiOrder, generation) {
  // For each slot in `population`, compute its fitness = 0.6 * winRateVsBaseline + 0.4 *
  // meanWinRateVsPeers. Baseline slot is evaluated too so its fitness reading is visible in the
  // report, but its "fitness" is just its own win-rate against ITSELF -- always ~0.5 (a canary
  // for sample-size noise, not a leaderboard entry).
  const baselineWeights = population.slots[SLOT_BASELINE].weights;
  const caps = perGameCapsForRegion(region);
  const games = MATCHUP_GAMES_OVERRIDE_PARSED ?? adaptiveMatchupGames(generation);
  const championGames = games.championGames;
  const peerGames = games.peerGames;
  for (let evolverSlot = 0; evolverSlot < population.slots.length; evolverSlot += 1) {
    const slotStartMs = Date.now();
    const slot = population.slots[evolverSlot];
    const evolverWeights = slot.weights;

    // vs baseline
    const vsBaselineSeed = randomGameSeed();
    const winRateVsBaseline = playMatchup(
      cardDb, deck, magiOrder, region,
      evolverWeights, baselineWeights,
      vsBaselineSeed, championGames, caps
    );

    // vs peers (all other non-baseline, non-self slots)
    let peerWins = 0;
    let peerGamesTotal = 0;
    for (let opponentSlot = 0; opponentSlot < population.slots.length; opponentSlot += 1) {
      if (opponentSlot === SLOT_BASELINE) continue;
      if (opponentSlot === evolverSlot) continue;
      const opponentWeights = population.slots[opponentSlot].weights;
      const seed = randomGameSeed();
      const rate = playMatchup(cardDb, deck, magiOrder, region, evolverWeights, opponentWeights, seed, peerGames, caps);
      peerWins += rate * peerGames;
      peerGamesTotal += peerGames;
    }
    const winRateVsPeers = peerGamesTotal > 0 ? peerWins / peerGamesTotal : 0.5;

    slot.fitness = combineFitness(winRateVsBaseline, winRateVsPeers);
    // Stash on the slot for the report -- see buildGenerationRow's championWinRateVsBaseline.
    slot._winRateVsBaseline = winRateVsBaseline;
    const slotElapsed = ((Date.now() - slotStartMs) / 1000).toFixed(1);
    process.stdout.write(`  gen ${generation} region ${region} slot ${evolverSlot}: fitness ${slot.fitness.toFixed(3)} (${slotElapsed}s)\n`);
  }
}

// ============================================================================
// Main loop
// ============================================================================

const _selectedRegions = selectedRegions();
const _skippedFromDefault = INCLUDE_EXCLUDED || REGIONS_FILTER
  ? []
  : [...DEFAULT_EXCLUDED_REGIONS].filter((r) => EVOLVING_REGIONS.includes(r));
const _matchupDescription = MATCHUP_GAMES_OVERRIDE_PARSED
  ? `${MATCHUP_GAMES_OVERRIDE_PARSED.championGames}/${MATCHUP_GAMES_OVERRIDE_PARSED.peerGames} (fixed via --matchupGames)`
  : `adaptive: gens 1-3 use 4/2, gens 4-8 use 6/3, gen 9+ uses 8/4`;
const _maxTurnsDescription = MAX_TURNS_CAP.perRegion.size > 0
  ? `default ${MAX_TURNS_CAP.default}${[...MAX_TURNS_CAP.perRegion].map(([r, n]) => `, ${r}=${n}`).join('')}`
  : `${MAX_TURNS_CAP.default} (all regions)`;

console.log('=== Evolution runner ===');
console.log(`Generations:       ${GENERATIONS}`);
console.log(`Regions:           ${_selectedRegions.join(', ')} (${_selectedRegions.length} total)`);
if (_skippedFromDefault.length > 0) {
  console.log(`Excluded by default: ${_skippedFromDefault.join(', ')} (use --includeExcluded or explicit --regions to opt back in)`);
}
console.log(`Population size:   ${POPULATION_SIZE_OVERRIDE ?? 'default (8)'}`);
console.log(`Matchup games:     ${_matchupDescription}`);
console.log(`Max turns/game:    ${_maxTurnsDescription}`);
console.log(`Game timeout:      ${GAME_TIMEOUT_SEC > 0 ? `${GAME_TIMEOUT_SEC}s` : 'disabled'}`);
console.log(`Workers:           ${WORKERS}${WORKERS === 1 ? ' (single-process, no pool)' : ''}`);
console.log(`Output dir:        ${OUT_DIR}`);
console.log(`Weights JSON:      ${WEIGHTS_JSON_PATH}`);
console.log('');

if (existsSync(STOP_FLAG)) unlinkSync(STOP_FLAG);

const cardData = JSON.parse(readFileSync(CARDS_PATH, 'utf-8'));
const cardDb = buildCardDatabase(cardData);
const decksByRegion = loadRegionalDecks();
const weightsFile = loadWeightsFile(WEIGHTS_JSON_PATH);

const startedAt = Date.now();
const startedAtIso = new Date(startedAt).toISOString();

// Rotate any existing JSONL to a timestamped archive before starting fresh (C2.6, 2026-08-18).
// Prior C2 behavior truncated in place -- every runner restart wiped history. Rotation
// preserves prior runs' data at evolution-generations-<startTime>.jsonl.
const archivedJsonl = rotateJsonlFile(JSONL_PATH, startedAtIso);
if (archivedJsonl) {
  console.log(`Rotated previous JSONL to: ${archivedJsonl}`);
  console.log('');
}
initJsonlFile(JSONL_PATH);
// Also init the errors JSONL so a fresh run starts with an empty file (rather than appending
// to potentially-stale data from a prior run).
writeFileSync(ERRORS_JSONL_PATH, '', 'utf-8');
const latestByRegion = new Map();
let totalGames = 0;
let generationsCompleted = 0;

// Per-region population state, held across generations.
// Default population size is 8 (owner decision, 2026-08-18, C2.6): quadratic savings on peer
// matchups vs POPULATION_SIZE_PER_REGION's original 10 (peer count grows as slot_count^2).
// Callers can override via --populationSize.
const DEFAULT_RUNNER_POPULATION_SIZE = 8;
const populationSize = POPULATION_SIZE_OVERRIDE !== null ? Number(POPULATION_SIZE_OVERRIDE) : DEFAULT_RUNNER_POPULATION_SIZE;

// Warm-start: if bot-weights.json has a champion for a region, seed the population from that
// champion (elite + mutations of it) rather than from all-baseline. Without this, every runner
// restart wipes accumulated learning -- see warmStartRegionPopulation's own doc comment for
// the shape. Runs unconditionally per region; if the region has no prior champion in the file,
// warmStartRegionPopulation transparently falls back to seedRegionPopulation.
const populations = new Map();
let warmStartedRegions = 0;
for (const region of _selectedRegions) {
  const prior = weightsFile.regions[region] ?? null;
  // Seed RNG for warm-start mutations deterministically: same bot-weights.json + same runner
  // args -> same starting population. Uses region index + a distinctive suffix so mutation
  // draws don't collide with the selection RNG's schedule (see selectionRngFor below).
  const warmStartRng = createSeededRng(seedFor(0, EVOLVING_REGIONS.indexOf(region), 88, 88, 88));
  const pop = warmStartRegionPopulation(region, prior, populationSize, warmStartRng, mutateWeights);
  populations.set(region, pop);
  if (prior !== null) warmStartedRegions += 1;
}
if (warmStartedRegions > 0) {
  console.log(`Warm-started ${warmStartedRegions} region(s) from bot-weights.json's saved champions.`);
  console.log(`Remaining ${_selectedRegions.length - warmStartedRegions} seeded fresh from baseline.`);
  console.log('');
}

// Selection RNG per (region, generation) -- shared across tournamentPickParents +
// crossoverWeights + mutateWeights calls within that generation's next-gen construction. A
// fresh stream per generation so a Ctrl-C between generations doesn't accidentally rewind
// randomness on resume.
function selectionRngFor(regionIndex, generation) {
  return createSeededRng(seedFor(generation, regionIndex, 99, 99, 99));
}

// Three stop modes (Checkpoint 2.6):
//   'soft'     -- finish the currently-running region, then exit (no more region-generations).
//                 Most useful during long runs; nothing already-computed is wasted, the region
//                 that was running gets its row appended cleanly.
//   'standard' -- cancel queued matchups, let in-flight games finish naturally. Same behavior
//                 as the pre-C2.6 STOP flag. Skips the row for the interrupted region (its
//                 fitness data is garbage from the cancellations).
//   'hard'     -- send SIGKILL to workers immediately. Loses whatever was in-flight. Only
//                 sensible when you're abandoning the current region's data ANYWAY (e.g.
//                 debugging a bug where every game hangs).
let stopped = false;
let stopMode = null; // 'soft' | 'standard' | 'hard'
function requestStopEverywhere(reason, mode) {
  if (stopped) return;
  stopped = true;
  stopMode = mode;
  const msg = {
    soft: `${reason} (soft) -- current region will finish cleanly, then exiting.`,
    standard: `${reason} (standard) -- cancelling queued matchups; in-flight games finish naturally, then exiting.`,
    hard: `${reason} (hard) -- SIGKILL'ing workers immediately; in-flight games abandoned.`,
  }[mode] ?? `${reason} -- stopping.`;
  console.log(`\n${msg}`);
  if (pool) {
    if (mode === 'hard') {
      // Drain FIRST so Promise.all resolves. Workers can't send results back after SIGKILL,
      // so pending in-flight jobs would otherwise hang forever.
      pool.drainAllPendingAsStuck();
      for (const w of pool.workers) {
        if (!w.killed) w.kill('SIGKILL');
      }
    } else if (mode === 'standard') {
      pool.requestStop();
    }
    // Soft mode: DON'T touch the pool. The current region completes, then the outer loop's
    // `if (stopped) break outer;` check between regions exits gracefully.
  }
}
process.on('SIGINT', () => requestStopEverywhere('SIGINT', 'standard'));

// Spin up the worker pool (Checkpoint 2.5). At WORKERS=1 we still use the sequential path
// -- avoids the process-fork overhead + IPC round-trip when the caller explicitly asked for
// no parallelism, and keeps evolve.mjs's pre-2.5 behavior byte-identical when someone runs
// without --workers.
const workerScriptPath = path.join(here, 'evolve-worker.mjs');
const pool = WORKERS > 1 ? new WorkerPool(WORKERS, workerScriptPath, CARDS_PATH) : null;
if (pool) {
  console.log(`Spinning up ${WORKERS} worker(s), waiting for card DB load...`);
  await pool.ready();
  console.log('Workers ready.');
  console.log('');
}

// Poll the STOP flag every 5 seconds so STOP EVOLUTION*.bat is responsive MID-region, not just
// between regions. Previous C2.5 shape only checked the flag between region-generations, so a
// slow region (Cald hit 8000s in the first real run) could ignore a STOP request for hours.
// 5s is a fair tradeoff between file-stat cost (trivial) and STOP responsiveness. Flag file
// content (2026-08-18 C2.6) determines mode: 'soft' finishes current region, 'hard' SIGKILLs
// workers, anything else (including empty) is standard.
const stopPoller = setInterval(() => {
  if (existsSync(STOP_FLAG)) {
    let mode = 'standard';
    try {
      const contents = readFileSync(STOP_FLAG, 'utf-8').trim().toLowerCase();
      if (contents.includes('soft')) mode = 'soft';
      else if (contents.includes('hard')) mode = 'hard';
    } catch { /* ignore read error, treat as standard */ }
    unlinkSync(STOP_FLAG);
    requestStopEverywhere('STOP flag detected', mode);
  }
}, 5000);

outer:
for (let gen = 1; gen <= GENERATIONS; gen += 1) {
  for (const region of _selectedRegions) {
    const regionIndex = EVOLVING_REGIONS.indexOf(region);
    const population = populations.get(region);
    const { deckCardKeys, magiOrder } = decksByRegion.get(region);

    const regionStartMs = Date.now();
    if (pool) {
      await evaluatePopulationParallel(pool, population, region, regionIndex, deckCardKeys, magiOrder, gen);
    } else {
      evaluatePopulation(cardDb, population, region, regionIndex, deckCardKeys, magiOrder, gen);
    }

    // If a STANDARD or HARD stop was requested mid-region, evaluatePopulationParallel resolved
    // early with 'stuck' outcomes for every cancelled matchup -- the resulting fitness numbers
    // are garbage. Do NOT append a JSONL row or persist the "champion" for that region; just
    // break out. Previously-completed regions this generation are already safely written.
    //
    // SOFT stop is different -- the region got to finish naturally, so its data is LEGIT and
    // deserves to be recorded. Let the loop body finish (append + persist + summary + next
    // gen), THEN break at the bottom.
    if (stopped && stopMode !== 'soft') break outer;

    // Broken-region skip (see evaluatePopulationParallel's own comment): a region that
    // had >50% 'stuck' outcomes gets skipped for this generation. Population state is
    // NOT advanced (no next-gen construction), so the SAME population re-runs next
    // generation -- if the underlying bug is fixed by then, the region recovers. Its
    // previous champion (if any) in bot-weights.json stays untouched.
    if (population._skipThisGen) {
      delete population._skipThisGen;
      console.log(`gen ${gen}  region ${region}  SKIPPED (broken)`);
      continue;
    }

    // Count games this region ran this generation, for the summary. Uses the same schedule
    // logic the evaluator itself used (adaptive OR --matchupGames override) so the count
    // matches reality regardless of which schedule fired.
    const _games = MATCHUP_GAMES_OVERRIDE_PARSED ?? adaptiveMatchupGames(gen);
    const perSlotGames = _games.championGames + _games.peerGames * (population.slots.length - 2);
    const gamesThisRegion = perSlotGames * population.slots.length;
    totalGames += gamesThisRegion;

    const champion = pickChampion(population);
    const championWinRateVsBaseline = champion._winRateVsBaseline ?? 0;
    const row = buildGenerationRow(gen, population, champion, championWinRateVsBaseline);
    appendGenerationRow(JSONL_PATH, row);
    latestByRegion.set(region, row);

    // Update the on-disk bot-weights.json with this region's new champion. Written atomically
    // per region so a Ctrl-C mid-generation still leaves a valid file (last-completed region's
    // champion persists; in-flight ones fall back to baseline on the next load).
    weightsFile.regions[region] = { ...champion.weights };
    writeWeightsFile(WEIGHTS_JSON_PATH, weightsFile);

    writeSummary(SUMMARY_PATH, startedAt, gen, totalGames, latestByRegion);

    const elapsedRegion = ((Date.now() - regionStartMs) / 1000).toFixed(1);
    console.log(`gen ${gen}  region ${region}  champ-fitness ${champion.fitness.toFixed(3)}  vs-baseline ${(championWinRateVsBaseline * 100).toFixed(0)}%  (${elapsedRegion}s, ${gamesThisRegion} games)`);

    // Build next generation's population NOW (still while gen counter equals `gen` for seed
    // purposes -- the newly-built pop is what gen+1's evaluation reads from).
    const selectionRng = selectionRngFor(regionIndex, gen);
    const nextPopulation = buildNextGeneration(population, champion, selectionRng);
    populations.set(region, nextPopulation);

    // Soft-stop check (see the stopMode setup near the pool init): if soft stop was requested
    // MID-region, the region-in-flight completed cleanly above (data appended, champion
    // persisted, next-gen built). Now exit cleanly at region boundary.
    if (stopped) break outer;
  }
  generationsCompleted = gen;
}

clearInterval(stopPoller);

const totalElapsedMin = ((Date.now() - startedAt) / 60000).toFixed(1);
console.log('');
console.log(`=== Done. ${generationsCompleted} generations across ${_selectedRegions.length} region(s), ${totalGames} total games, ${totalElapsedMin} minutes. ===`);
console.log(`Summary: ${SUMMARY_PATH}`);
console.log(`JSONL:   ${JSONL_PATH}`);
console.log(`Weights: ${WEIGHTS_JSON_PATH}`);

if (pool) await pool.shutdown();
