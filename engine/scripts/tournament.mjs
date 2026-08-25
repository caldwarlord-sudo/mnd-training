// AUTO-SYNCED from private repo. Do not edit here -- edits are overwritten on the next sync.
// Source of record: engine/scripts/tournament.mjs in the private repo.

// Cross-region tournament runner (Checkpoint 4, 2026-08-19). Each region pilots its OWN deck
// with its OWN evolved champion weights (loaded from bot-weights.json). Runs full round-robin:
// every ordered (p1Region, p2Region) pair plays N games with p1 in the first seat and p2 in
// the second. Outputs leaderboard + full matchup matrix + JSONL of every pair's aggregate
// stats. Underneath included with baseline weights by default (no evolved weights for it, but
// its self-mirror problem doesn't apply in cross-region play).
//
// Usage from engine/ :
//   npm run build && node scripts/tournament.mjs [OPTIONS]
//
// Options:
//   --workers N               Parallel worker processes. Default 6.
//   --gamesPerDirection N     Games per ORDERED matchup pair (so pair total = 2N, since
//                             each unordered pair is played in both directions). Default 8.
//   --regions "a,b,c"         Regions to include. Default: all 13.
//   --excludeUnderneath       Skip Underneath from the tournament (default: include, using
//                             baseline weights since Underneath has no evolved champion).
//   --maxTurns N              Per-game turn cap. Default 200.
//   --maxActions N            Per-game action cap. Default 20000.
//   --gameTimeoutSec N        Per-game wall-clock cap in seconds. Default 300 (5 min).
//                             0 disables.
//   --weights FILE            Path to bot-weights.json. Default engine/data/bot-weights.json.
//   --out DIR                 Output directory. Default engine/.
//   --decks DIR               Default deck directory. Default app/resources/default-decks.

import { readFileSync, existsSync, unlinkSync, writeFileSync, appendFileSync, mkdirSync, readdirSync, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve } from 'node:path';
import { fork } from 'node:child_process';

// Cards + weight-loading helpers -- same as evolve.mjs uses.
import '../engine.bundle.js';
import { EVOLVING_REGIONS, loadWeightsFile, resolveWeightsForRegion } from '../engine.bundle.js';

const here = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(here, '..', '..');

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : process.argv[i + 1];
}

const WORKERS = Number(arg('workers', 6));
const GAMES_PER_DIRECTION = Number(arg('gamesPerDirection', 8));
const REGIONS_FILTER = arg('regions', null);
const EXCLUDE_UNDERNEATH = process.argv.includes('--excludeUnderneath');
const MAX_TURNS = Number(arg('maxTurns', 200));
const MAX_ACTIONS = Number(arg('maxActions', 20000));
const GAME_TIMEOUT_SEC = Number(arg('gameTimeoutSec', 300));
const WEIGHTS_PATH = resolve(arg('weights', join(REPO_ROOT, 'engine', 'data', 'bot-weights.json')));
const OUT_DIR = resolve(arg('out', join(here, '..')));
const DECKS_DIR = resolve(arg('decks', join(REPO_ROOT, 'app', 'resources', 'default-decks')));
// Sharding (2026-08-20, GitHub Actions matrix parallelism -- see
// docs/plans/training-compute-scaling-plan.md and mnd-training/.github/workflows/tournament.yml).
// A "shard" is a subset of the job list. With SHARD_COUNT > 1, this script runs only jobs whose
// index mod SHARD_COUNT equals SHARD_INDEX, and writes its per-pair JSONL to a shard-suffixed
// filename so downloaded shard artifacts don't collide when the aggregator reads them.
// AGGREGATE_SHARDS_DIR flips this script into aggregator mode: read every shard JSONL under the
// given directory, reconstruct pairStats by summing, and produce the final report. No games run
// in aggregator mode.
const SHARD_INDEX = Number(arg('shardIndex', 0));
const SHARD_COUNT = Number(arg('shardCount', 1));
const AGGREGATE_SHARDS_DIR = arg('aggregateShards', null);
if (SHARD_INDEX < 0 || SHARD_INDEX >= SHARD_COUNT) {
  console.error(`Invalid shardIndex ${SHARD_INDEX} for shardCount ${SHARD_COUNT} (must be 0 <= index < count).`);
  process.exit(1);
}
// Weighted sharding (2026-08-20, owner ask): naive `i % shardCount` gives the shard that pulls
// slow-region jobs a long-tail wall-clock while other shards sit idle. Instead we tag each job
// with a weight (heavy if either side's region is in the "slow" set) and greedy-bin-pack jobs
// into shards longest-first so total weight per shard is roughly balanced. Deterministic:
// same jobs + same shardCount + same slow-region set => same assignment.
// Default slow regions come from last night's overnight run's per-pair timings -- Underneath,
// Weave, and Paradwyn same-region mirrors and any cross-region pair touching them dominated.
// Override with `--slowRegions "A,B,C"`; pass an empty string for uniform weight.
const SLOW_REGIONS_ARG = arg('slowRegions', 'Underneath,Weave,Paradwyn');
const SLOW_REGIONS = new Set(SLOW_REGIONS_ARG.split(',').map((s) => s.trim()).filter((s) => s));
const HEAVY_WEIGHT = 5; // 5x is a rough eyeball from the round-robin timings; not load-bearing.

const CARDS_PATH = join(REPO_ROOT, 'data-pipeline', 'output', 'cards_final.json');
const RESULTS_PATH = join(OUT_DIR, 'tournament-results.txt');
// In shard mode, each shard's JSONL is suffixed so multiple shard artifacts unpacked into the
// same aggregator directory don't clobber each other. In non-shard mode (default), the unsuffixed
// filename preserves backward compat with existing tooling.
const MATCHES_JSONL_PATH = join(
  OUT_DIR,
  SHARD_COUNT > 1 ? `tournament-matches-shard-${SHARD_INDEX}.jsonl` : 'tournament-matches.jsonl',
);
const ERRORS_JSONL_PATH = join(
  OUT_DIR,
  SHARD_COUNT > 1 ? `tournament-errors-shard-${SHARD_INDEX}.jsonl` : 'tournament-errors.jsonl',
);
const STOP_FLAG = join(OUT_DIR, 'tournament-STOP.txt');

const DECK_ID_TO_REGION = {
  'default-arderial': 'Arderial', 'default-bograth': 'Bograth', 'default-cald': 'Cald',
  'default-core': 'Core', 'default-dresh': "d'Resh", 'default-kybars-teeth': "Kybar's Teeth",
  'default-nar': 'Nar', 'default-naroom': 'Naroom', 'default-orothe': 'Orothe',
  'default-paradwyn': 'Paradwyn', 'default-underneath': 'Underneath',
  'default-universal': 'Universal', 'default-weave': 'Weave',
};

function expandDeck(deckJson) {
  const deckCardKeys = [];
  for (const [key, count] of deckJson.cardCounts) {
    for (let i = 0; i < count; i += 1) deckCardKeys.push(key);
  }
  return { deckCardKeys, magiOrder: [...deckJson.magiKeys] };
}

function loadRegionalDecks() {
  const byRegion = new Map();
  for (const [deckId, region] of Object.entries(DECK_ID_TO_REGION)) {
    const filePath = join(DECKS_DIR, `${deckId}.json`);
    if (!existsSync(filePath)) {
      console.error(`Missing deck file: ${filePath}`);
      process.exit(1);
    }
    byRegion.set(region, expandDeck(JSON.parse(readFileSync(filePath, 'utf-8'))));
  }
  return byRegion;
}

function selectedRegions() {
  let regions = EVOLVING_REGIONS;
  if (REGIONS_FILTER) {
    const wanted = new Set(REGIONS_FILTER.split(',').map((s) => s.trim()));
    regions = regions.filter((r) => wanted.has(r));
    if (regions.length === 0) {
      console.error(`--regions "${REGIONS_FILTER}" matched none of EVOLVING_REGIONS: ${EVOLVING_REGIONS.join(', ')}`);
      process.exit(1);
    }
  }
  if (EXCLUDE_UNDERNEATH) regions = regions.filter((r) => r !== 'Underneath');
  return regions;
}

/** Seed per game (2026-08-25 change): genuinely-random uint32 via `Math.random()`. HISTORICAL:
 *  prior formula `(p1RegionIdx*10000 + p2RegionIdx*100 + gameIdx) ^ 0x3a5f7d21` was
 *  deterministic per region-pair position, which meant every tournament run dealt the same
 *  hands for a given region matchup. See roundrobin.mjs's `randomGameSeed` for the full
 *  rationale on why per-game random seeding replaces per-index deterministic seeding. */
function randomGameSeed() {
  return (Math.floor(Math.random() * 4294967296)) >>> 0;
}

// ============================================================================
// Worker pool (duplicated from evolve.mjs to avoid modifying it during a live run)
// ============================================================================

class WorkerPool {
  constructor(size, workerScriptPath, cardsPath) {
    this.workers = [];
    this.freeWorkers = [];
    this.queue = [];
    this.pending = new Map();
    this.nextJobId = 1;
    this.stopRequested = false;
    this.errorCounts = new Map();
    this._onJobCompleteHook = null;
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
        const seen = this.errorCounts.get(msg.error);
        if (!seen) {
          this.errorCounts.set(msg.error, 1);
          console.error(`  [first occurrence] game threw: ${msg.error}`);
        } else {
          this.errorCounts.set(msg.error, seen + 1);
          if ((seen + 1) % 25 === 0) {
            console.error(`  [${seen + 1} occurrences] "${msg.error.slice(0, 80)}${msg.error.length > 80 ? '...' : ''}"`);
          }
        }
        // Persist for post-run diagnosis. Append-only, one row per occurrence.
        try {
          appendFileSync(ERRORS_JSONL_PATH, JSON.stringify({ timestamp: new Date().toISOString(), signature: msg.error }) + '\n', 'utf-8');
        } catch { /* file-write failure isn't fatal to the tournament */ }
      }
      p.resolve(msg.result);
      this.freeWorkers.push(worker);
      if (this._onJobCompleteHook) this._onJobCompleteHook();
      this._drain();
    }
  }

  _drain() {
    if (this.stopRequested) {
      const stuckResult = {
        outcome: 'stuck', turnsPlayed: 0, actionsApplied: 0,
        advancePhaseCount: 0, declareAttackCount: 0, playCardCount: 0, usePowerCount: 0,
      };
      while (this.queue.length > 0) {
        const { jobId } = this.queue.shift();
        const p = this.pending.get(jobId);
        if (p) { this.pending.delete(jobId); p.resolve(stuckResult); }
      }
      return;
    }
    while (this.queue.length > 0 && this.freeWorkers.length > 0) {
      const worker = this.freeWorkers.shift();
      const { jobId, payload } = this.queue.shift();
      worker.send({ type: 'job', jobId, ...payload });
    }
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

  async ready() { return this.readyPromise; }

  requestStop() { this.stopRequested = true; this._drain(); }

  async shutdown() {
    for (const w of this.workers) {
      try { w.send({ type: 'shutdown' }); } catch { /* dead worker */ }
    }
    await new Promise((r) => setTimeout(r, 200));
    for (const w of this.workers) { if (!w.killed) w.kill(); }
  }
}

// ============================================================================
// Aggregation + reporting
// ============================================================================

/** Empty matchup stats -- one of these per (p1Region, p2Region) ordered pair. */
function emptyPairStats() {
  return {
    games: 0,
    outcomes: { p1: 0, p2: 0, p1ByTiebreak: 0, p2ByTiebreak: 0, drawByDefeat: 0, drawByCap: 0, stuck: 0 },
    turnsSum: 0, turnsMax: 0,
  };
}

function pointsFor(outcome, side) {
  if (outcome === 'drawByDefeat' || outcome === 'drawByCap' || outcome === 'stuck') return 0.5;
  const winner = (outcome === 'p1' || outcome === 'p1ByTiebreak') ? 'p1' : 'p2';
  return winner === side ? 1 : 0;
}

function computeLeaderboard(regions, pairStats) {
  // Per-region aggregate across both directions (as p1 and as p2).
  const perRegion = new Map();
  for (const r of regions) {
    perRegion.set(r, { region: r, wins: 0, losses: 0, draws: 0, points: 0, gamesPlayed: 0 });
  }
  for (const [p1Region, byOpponent] of pairStats.entries()) {
    for (const [p2Region, stats] of byOpponent.entries()) {
      // Iterate per-outcome to credit both sides.
      for (const [outcome, count] of Object.entries(stats.outcomes)) {
        for (let i = 0; i < count; i += 1) {
          const p1Points = pointsFor(outcome, 'p1');
          const p2Points = pointsFor(outcome, 'p2');
          const p1Rec = perRegion.get(p1Region);
          const p2Rec = perRegion.get(p2Region);
          p1Rec.points += p1Points; p1Rec.gamesPlayed += 1;
          p2Rec.points += p2Points; p2Rec.gamesPlayed += 1;
          if (p1Points === 1) { p1Rec.wins += 1; p2Rec.losses += 1; }
          else if (p2Points === 1) { p2Rec.wins += 1; p1Rec.losses += 1; }
          else { p1Rec.draws += 1; p2Rec.draws += 1; }
        }
      }
    }
  }
  const sorted = [...perRegion.values()].sort((a, b) => b.points - a.points || b.wins - a.wins || a.region.localeCompare(b.region));
  return sorted;
}

/** Total wins for region A when it played AS EITHER SIDE against region B. Used by the
 *  matchup matrix, which shows both directions combined per unordered pair. */
function totalHeadToHead(pairStats, regionA, regionB) {
  let aWins = 0, bWins = 0, draws = 0, games = 0;
  // A as p1 vs B as p2
  const asP1 = pairStats.get(regionA)?.get(regionB);
  if (asP1) {
    aWins += asP1.outcomes.p1 + asP1.outcomes.p1ByTiebreak;
    bWins += asP1.outcomes.p2 + asP1.outcomes.p2ByTiebreak;
    draws += asP1.outcomes.drawByDefeat + asP1.outcomes.drawByCap + asP1.outcomes.stuck;
    games += asP1.games;
  }
  // B as p1 vs A as p2
  const asP2 = pairStats.get(regionB)?.get(regionA);
  if (asP2) {
    bWins += asP2.outcomes.p1 + asP2.outcomes.p1ByTiebreak;
    aWins += asP2.outcomes.p2 + asP2.outcomes.p2ByTiebreak;
    draws += asP2.outcomes.drawByDefeat + asP2.outcomes.drawByCap + asP2.outcomes.stuck;
    games += asP2.games;
  }
  return { aWins, bWins, draws, games };
}

function writeResults(regions, pairStats, startedAt, elapsedMs, totalGames, aggregatedFromShards) {
  const lines = [];
  lines.push('=== Cross-region tournament results ===');
  // Header shape differs between a live run (Started + Elapsed timing tells you when the run
  // happened and how long it took) and an aggregator run (which stitches shard JSONLs together;
  // the shards ran on other machines with their own start/elapsed values, so the aggregator's
  // own timestamps would be misleading -- see other-session review 2026-08-20). In aggregator
  // mode we replace the timing lines with a "Aggregated from N shards" line + the current time.
  if (aggregatedFromShards) {
    lines.push(`Aggregated from ${aggregatedFromShards} shard artifact(s) at ${new Date().toISOString()}`);
  } else {
    lines.push(`Started: ${new Date(startedAt).toISOString()}`);
    lines.push(`Elapsed: ${(elapsedMs / 60000).toFixed(1)} minutes`);
  }
  lines.push(`Total games: ${totalGames}`);
  lines.push(`Games per direction: ${GAMES_PER_DIRECTION} (per unordered pair: ${GAMES_PER_DIRECTION * 2})`);
  lines.push(`Regions: ${regions.join(', ')} (${regions.length} total)`);
  lines.push('');
  lines.push('--- Leaderboard ---');
  lines.push('Rank  Region                W    L    D    Points    Games   Win%');
  const board = computeLeaderboard(regions, pairStats);
  board.forEach((rec, i) => {
    const winPct = rec.gamesPlayed > 0 ? (rec.points / rec.gamesPlayed * 100).toFixed(1) : '-';
    lines.push(`${String(i + 1).padStart(2)}    ${rec.region.padEnd(18)} ${String(rec.wins).padStart(4)} ${String(rec.losses).padStart(4)} ${String(rec.draws).padStart(4)}  ${rec.points.toFixed(1).padStart(6)}    ${String(rec.gamesPlayed).padStart(5)}  ${winPct.padStart(5)}%`);
  });
  lines.push('');
  lines.push('--- Head-to-head matchup matrix (row region\'s wins - opponent\'s wins - draws) ---');
  const colWidth = 11;
  const header = ' '.repeat(18) + regions.map((r) => r.slice(0, colWidth - 1).padStart(colWidth)).join(' ');
  lines.push(header);
  for (const rowRegion of regions) {
    const cells = regions.map((colRegion) => {
      if (colRegion === rowRegion) return '        -  ';
      const { aWins, bWins, draws } = totalHeadToHead(pairStats, rowRegion, colRegion);
      const label = draws > 0 ? `${aWins}-${bWins}-${draws}` : `${aWins}-${bWins}`;
      return label.padStart(colWidth);
    });
    lines.push(rowRegion.padEnd(18) + cells.join(' '));
  }
  lines.push('');
  lines.push('Matchup cells are (this-region-wins)-(opponent-wins)[-(draws)], summing both directions.');
  lines.push('So "Cald" row × "Naroom" column shows total Cald wins in every game between them.');
  const dir = dirname(RESULTS_PATH);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  writeFileSync(RESULTS_PATH, lines.join('\n'), 'utf-8');
}

function initJsonl(path) {
  const dir = dirname(path);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  writeFileSync(path, '', 'utf-8');
}

function appendPairJsonl(path, p1Region, p2Region, stats) {
  const row = {
    p1Region, p2Region,
    games: stats.games,
    outcomes: stats.outcomes,
    meanTurns: stats.games > 0 ? stats.turnsSum / stats.games : 0,
    maxTurns: stats.turnsMax,
  };
  appendFileSync(path, JSON.stringify(row) + '\n', 'utf-8');
}

/** Aggregator mode helper (2026-08-20): reads every `tournament-matches*.jsonl` file under `dir`
 *  (recursively -- artifact downloads unpack into per-artifact subdirectories), merges their rows
 *  by (p1Region, p2Region), and returns a pairStats-shaped Map ready to feed to `writeResults`.
 *  Each JSONL row is self-contained (see `appendPairJsonl` above), so aggregation is a straight
 *  sum of counts and a weighted merge of the turn stats. */
function findJsonlFilesRecursive(dir) {
  const results = [];
  for (const entry of readdirSync(dir)) {
    const abs = join(dir, entry);
    const s = statSync(abs);
    if (s.isDirectory()) {
      for (const f of findJsonlFilesRecursive(abs)) results.push(f);
    } else if (/^tournament-matches(-shard-\d+)?\.jsonl$/.test(entry)) {
      results.push(abs);
    }
  }
  return results;
}

function aggregateShardJsonls(dir) {
  const files = findJsonlFilesRecursive(dir);
  if (files.length === 0) {
    console.error(`aggregator: no tournament-matches JSONL files found under ${dir}`);
    process.exit(1);
  }
  console.log(`Aggregator: reading ${files.length} shard JSONL file(s):`);
  for (const f of files) console.log(`  ${f}`);
  console.log('');

  const pairStats = new Map();
  const regionsSeen = new Set();
  let rowsRead = 0;
  for (const file of files) {
    const lines = readFileSync(file, 'utf-8').split('\n').filter((l) => l.trim());
    for (const line of lines) {
      const row = JSON.parse(line);
      rowsRead += 1;
      regionsSeen.add(row.p1Region);
      regionsSeen.add(row.p2Region);
      if (!pairStats.has(row.p1Region)) pairStats.set(row.p1Region, new Map());
      const byOpp = pairStats.get(row.p1Region);
      let stats = byOpp.get(row.p2Region);
      if (!stats) { stats = emptyPairStats(); byOpp.set(row.p2Region, stats); }
      // Merge: shard rows are non-overlapping per (p1Region, p2Region) because the shard slice
      // is on the flat job index; different shards touch different games of the same pair, so
      // sums are the correct combination. Turn stats sum via games * meanTurns to preserve the
      // weighted mean when writeResults recomputes it.
      stats.games += row.games;
      for (const [k, v] of Object.entries(row.outcomes)) {
        stats.outcomes[k] = (stats.outcomes[k] ?? 0) + v;
      }
      stats.turnsSum += (row.meanTurns ?? 0) * row.games;
      if ((row.maxTurns ?? 0) > stats.turnsMax) stats.turnsMax = row.maxTurns;
    }
  }
  console.log(`Aggregator: merged ${rowsRead} pair rows covering ${regionsSeen.size} regions.`);
  console.log('');
  return { pairStats, regions: [...regionsSeen].sort(), shardFileCount: files.length };
}

// ============================================================================
// Main loop
// ============================================================================

// Aggregator mode short-circuits the whole "spin up workers, run games" flow. Reads shard JSONLs
// from the given directory, produces the final report, exits. Used by the aggregator job of the
// GitHub Actions matrix workflow -- see mnd-training/.github/workflows/tournament.yml.
if (AGGREGATE_SHARDS_DIR) {
  const absDir = resolve(AGGREGATE_SHARDS_DIR);
  if (!existsSync(absDir)) {
    console.error(`--aggregateShards directory not found: ${absDir}`);
    process.exit(1);
  }
  console.log('=== Cross-region tournament AGGREGATOR ===');
  console.log(`Shard artifacts dir: ${absDir}`);
  console.log(`Output dir:          ${OUT_DIR}`);
  console.log('');
  const { pairStats, regions, shardFileCount } = aggregateShardJsonls(absDir);
  // Total games = sum of `stats.games` across all pairs.
  let totalGames = 0;
  for (const byOpp of pairStats.values()) for (const s of byOpp.values()) totalGames += s.games;
  // `aggregatedFromShards` (last arg) flips writeResults' header to name this as an aggregation
  // rather than printing a bogus "Elapsed: 0.0 minutes" line -- other-session review 2026-08-20.
  writeResults(regions, pairStats, Date.now(), 0, totalGames, shardFileCount);
  console.log(`Leaderboard + matchup matrix written to: ${RESULTS_PATH}`);
  process.exit(0);
}

const _regions = selectedRegions();
const weightsFile = loadWeightsFile(WEIGHTS_PATH);
const decksByRegion = loadRegionalDecks();

console.log('=== Cross-region tournament runner (Checkpoint 4) ===');
console.log(`Workers:              ${WORKERS}`);
console.log(`Games per direction:  ${GAMES_PER_DIRECTION} (per pair: ${GAMES_PER_DIRECTION * 2})`);
console.log(`Regions:              ${_regions.join(', ')} (${_regions.length} total)`);
console.log(`Weights source:       ${WEIGHTS_PATH}`);
console.log(`Max turns / game:     ${MAX_TURNS}`);
console.log(`Game timeout:         ${GAME_TIMEOUT_SEC > 0 ? `${GAME_TIMEOUT_SEC}s` : 'disabled'}`);
console.log(`Output dir:           ${OUT_DIR}`);
console.log('');

// Report which regions have evolved weights vs which fall back to baseline.
const evolvedRegions = _regions.filter((r) => weightsFile.regions[r] !== undefined);
const baselineRegions = _regions.filter((r) => weightsFile.regions[r] === undefined);
console.log(`Evolved weights (${evolvedRegions.length}): ${evolvedRegions.join(', ')}`);
if (baselineRegions.length > 0) {
  console.log(`Baseline weights (${baselineRegions.length}, no evolved champion): ${baselineRegions.join(', ')}`);
}
console.log('');

if (existsSync(STOP_FLAG)) unlinkSync(STOP_FLAG);
initJsonl(MATCHES_JSONL_PATH);
initJsonl(ERRORS_JSONL_PATH);

const startedAt = Date.now();
const workerScriptPath = join(here, 'tournament-worker.mjs');
const pool = new WorkerPool(WORKERS, workerScriptPath, CARDS_PATH);
console.log(`Spinning up ${WORKERS} worker(s), waiting for card DB load...`);
await pool.ready();
console.log('Workers ready.');
console.log('');

let stopped = false;
process.on('SIGINT', () => {
  console.log('\nSIGINT -- cancelling queued games, waiting for in-flight to finish.');
  stopped = true;
  pool.requestStop();
});
const stopPoller = setInterval(() => {
  if (existsSync(STOP_FLAG)) {
    unlinkSync(STOP_FLAG);
    console.log('\nSTOP flag detected -- cancelling queued games.');
    stopped = true;
    pool.requestStop();
  }
}, 5000);

// Build the full job list: every ordered (p1Region, p2Region) pair * GAMES_PER_DIRECTION.
// Each job carries every region's deck / weights inline (workers don't share state), so
// this loop's memory is O(regions^2 * games) but only for job descriptors -- trivial.
const jobs = [];
for (let p1Idx = 0; p1Idx < _regions.length; p1Idx += 1) {
  for (let p2Idx = 0; p2Idx < _regions.length; p2Idx += 1) {
    if (p1Idx === p2Idx) continue; // no self-mirror
    const p1Region = _regions[p1Idx];
    const p2Region = _regions[p2Idx];
    const p1Deck = decksByRegion.get(p1Region);
    const p2Deck = decksByRegion.get(p2Region);
    const p1Weights = resolveWeightsForRegion(weightsFile, p1Region);
    const p2Weights = resolveWeightsForRegion(weightsFile, p2Region);
    for (let g = 0; g < GAMES_PER_DIRECTION; g += 1) {
      jobs.push({
        p1Region, p2Region,
        payload: {
          p1Region, p1Deck: p1Deck.deckCardKeys, p1MagiOrder: p1Deck.magiOrder, p1Weights,
          p2Region, p2Deck: p2Deck.deckCardKeys, p2MagiOrder: p2Deck.magiOrder, p2Weights,
          seed: randomGameSeed(),
          maxTurns: MAX_TURNS,
          maxActions: MAX_ACTIONS,
          timeoutMs: GAME_TIMEOUT_SEC > 0 ? GAME_TIMEOUT_SEC * 1000 : undefined,
        },
      });
    }
  }
}
console.log(`Total games to run: ${jobs.length}`);

// Sharding: greedy bin-packing by job weight. Heavy jobs (either side's region in SLOW_REGIONS)
// weigh HEAVY_WEIGHT, others 1. Sort jobs by weight descending, then assign each to the
// currently-least-loaded shard -- gives roughly equal total weight per shard so no shard sits
// as the bottleneck. Deterministic order (sort is stable on original index within same weight),
// so shard N always gets the same jobs across runs with the same (regions, gamesPerDirection,
// shardCount, slowRegions) inputs.
if (SHARD_COUNT > 1) {
  const originalCount = jobs.length;
  const indexed = jobs.map((j, i) => ({
    i,
    weight: (SLOW_REGIONS.has(j.p1Region) || SLOW_REGIONS.has(j.p2Region)) ? HEAVY_WEIGHT : 1,
  }));
  // Stable sort: same weight => original index order, so the greedy allocator is deterministic.
  indexed.sort((a, b) => b.weight - a.weight || a.i - b.i);
  const shardLoads = new Array(SHARD_COUNT).fill(0);
  const assignedShard = new Array(jobs.length);
  for (const { i, weight } of indexed) {
    let minShard = 0;
    for (let s = 1; s < SHARD_COUNT; s += 1) {
      if (shardLoads[s] < shardLoads[minShard]) minShard = s;
    }
    assignedShard[i] = minShard;
    shardLoads[minShard] += weight;
  }
  // Log the allocation summary before filtering so operators can see the balance.
  const jobsPerShard = new Array(SHARD_COUNT).fill(0);
  for (const s of assignedShard) jobsPerShard[s] += 1;
  console.log(`Shard weight balance: ${shardLoads.map((w, s) => `#${s}=${w} (${jobsPerShard[s]} games)`).join(', ')}`);
  // Filter jobs down to just this shard's slice. Mutates the const array (const forbids
  // reassignment, allows mutation).
  for (let i = jobs.length - 1; i >= 0; i -= 1) {
    if (assignedShard[i] !== SHARD_INDEX) jobs.splice(i, 1);
  }
  console.log(`Shard ${SHARD_INDEX}/${SHARD_COUNT}: running ${jobs.length} of ${originalCount} games (weight ${shardLoads[SHARD_INDEX]}).`);
}
console.log('');

// Progress reporter -- log every ~50 completions or every ~90s (whichever first).
let completedJobs = 0;
let lastLogAtMs = Date.now();
let lastLogAtCount = 0;
const LOG_EVERY_JOBS = Math.max(50, Math.floor(jobs.length / 30));
const LOG_EVERY_MS = 90_000;
pool._onJobCompleteHook = () => {
  completedJobs += 1;
  const nowMs = Date.now();
  const shouldLog = (completedJobs - lastLogAtCount) >= LOG_EVERY_JOBS || (nowMs - lastLogAtMs) >= LOG_EVERY_MS;
  if (shouldLog && completedJobs < jobs.length) {
    lastLogAtCount = completedJobs;
    lastLogAtMs = nowMs;
    const pct = (completedJobs / jobs.length * 100).toFixed(0);
    const elapsedS = ((nowMs - startedAt) / 1000).toFixed(0);
    const remainingS = completedJobs > 0
      ? (((nowMs - startedAt) / completedJobs) * (jobs.length - completedJobs) / 1000).toFixed(0)
      : '?';
    process.stdout.write(`  ${completedJobs}/${jobs.length} games (${pct}%), ${elapsedS}s elapsed, ~${remainingS}s remaining\n`);
  }
};

// Dispatch and await all.
const results = await Promise.all(jobs.map((j) => pool.runJob(j.payload)));

pool._onJobCompleteHook = null;

// Aggregate per (p1Region, p2Region) ordered pair.
const pairStats = new Map(); // p1Region -> Map<p2Region, stats>
for (const r of _regions) pairStats.set(r, new Map());

for (let i = 0; i < jobs.length; i += 1) {
  const j = jobs[i];
  const r = results[i];
  let byOpp = pairStats.get(j.p1Region);
  let stats = byOpp.get(j.p2Region);
  if (!stats) { stats = emptyPairStats(); byOpp.set(j.p2Region, stats); }
  stats.games += 1;
  stats.outcomes[r.outcome] += 1;
  stats.turnsSum += r.turnsPlayed;
  if (r.turnsPlayed > stats.turnsMax) stats.turnsMax = r.turnsPlayed;
}

// Write per-pair JSONL.
for (const [p1Region, byOpp] of pairStats.entries()) {
  for (const [p2Region, stats] of byOpp.entries()) {
    appendPairJsonl(MATCHES_JSONL_PATH, p1Region, p2Region, stats);
  }
}

const elapsedMs = Date.now() - startedAt;
// In shard mode, skip writeResults -- the shard only has a slice of the pairs, so its
// "leaderboard" would be misleading. The aggregator job downloads every shard's JSONL and
// produces the final report via `--aggregateShards <dir>`.
if (SHARD_COUNT > 1) {
  console.log('');
  console.log(`=== Shard ${SHARD_INDEX}/${SHARD_COUNT} done. ${jobs.length} games in ${(elapsedMs / 60000).toFixed(1)} minutes. ===`);
  console.log(`Per-pair JSONL: ${MATCHES_JSONL_PATH}`);
  console.log('(Full leaderboard produced by the aggregator job, not this shard.)');
} else {
  writeResults(_regions, pairStats, startedAt, elapsedMs, jobs.length);
  console.log('');
  console.log(`=== Done. ${jobs.length} games in ${(elapsedMs / 60000).toFixed(1)} minutes. ===`);
  console.log(`Leaderboard + matchup matrix: ${RESULTS_PATH}`);
  console.log(`Per-pair JSONL:               ${MATCHES_JSONL_PATH}`);
}

clearInterval(stopPoller);
await pool.shutdown();
