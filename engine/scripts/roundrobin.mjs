// AUTO-SYNCED from private repo. Do not edit here -- edits are overwritten on the next sync.
// Source of record: engine/scripts/roundrobin.mjs in the private repo.

// Full-field cross-region round-robin tournament runner (2026-08-22).
// Complementary to swiss-tournament.mjs: where Swiss uses 7 rounds and ~10-14 games per
// entrant (which leaves individual rankings noisy at 52-entrant scale), this script plays
// every entrant against every other entrant in a Bo4 series with 2 games as each side. The
// result: each entrant plays ~200 games instead of ~12, and rankings become variance-robust
// enough to interpret directly rather than through cross-Swiss aggregates.
//
// Field composition works the same as swiss-tournament.mjs:
//   - Baselines auto-loaded per region (unless --skipBaseline)
//   - Champions loaded from a JSONL of historical rows (--jsonl, defaults to
//     engine/evolution-generations.jsonl). --shapeCOnly filters to Shape-C rows.
//   - Optional --extraEntrantsFile for injecting custom entrants.
//   - --deckOverride Region=path (repeatable) substitutes a non-default deck for a region.
//
// Series semantics (owner-approved 2026-08-22): each pair of entrants plays exactly 4 games,
// 2 with A as p1 and 2 with B as p1 for perfect side balance. Series scoring:
//   4-0 or 3-1 game wins for A  =>  A wins series (1 series-pt for A, 0 for B)
//   2-2 (draw)                   =>  0.5 series-pt each
//   1-3 or 0-4                   =>  B wins series (0 for A, 1 for B)
// Draws are allowed and expected -- they're informative signal that two entrants are truly
// evenly matched, not noise to be dispelled.
//
// Sharding (matrix parallelism) mirrors tournament.mjs: --shardIndex N --shardCount M splits
// the flat job list across N parallel matrix jobs, and --aggregateShards <dir> merges shard
// JSONLs into the final leaderboard. Weighted bin-packing prioritizes slow-region pairs
// (Underneath / Weave / Paradwyn by default) so no shard has an outsized wall-clock share.
//
// Usage from engine/ :
//   npm run build && node scripts/roundrobin.mjs [OPTIONS]
//
// Options:
//   --workers N               Parallel worker processes. Default 6.
//   --gamesPerPair N          Games per pair. Default 4 (2 each side).
//   --regions "a,b,c"         Regions to include as baselines. Default: EVOLVING_REGIONS.
//   --jsonl FILE              Champion JSONL. Default engine/evolution-generations.jsonl.
//   --shapeCOnly              Only include GH-Actions-ShapeC-betaOn rows from JSONL.
//   --skipBaseline            Skip auto-loading baseline entrants.
//   --latestGenOnly           Keep only each region's latest champion + baseline.
//   --extraEntrantsFile FILE  Optional custom entrants (same shape as swiss's).
//   --deckOverride Region=PATH   REPEATABLE. Substitute a non-default deck for one region.
//   --slowRegions "A,B,C"     Regions whose pairs get 5x weight for shard balancing.
//                             Default "Underneath,Weave,Paradwyn". Empty = uniform.
//   --focusRegion NAME        Optional. Only play pairs where at least one entrant's region is
//                             NAME. Used by focused per-generation evolution runs where the
//                             other regions are "opponent fodder" for a single region -- no
//                             point wasting compute on opponent-vs-opponent pairs. Compatible
//                             with sharding (the filtered pair list gets sharded normally).
//   --shardIndex N            0-based shard index. Default 0.
//   --shardCount N            Total shards. Default 1 (no sharding).
//   --aggregateShards DIR     AGGREGATOR MODE. Merges shard JSONLs into final report.
//   --maxTurns N              Per-game turn cap. Default 200.
//   --gameTimeoutSec N        Per-game wall-clock cap. Default 300.
//   --out DIR                 Output directory. Default engine/.
//   --decks DIR               Default-deck directory. Default app/resources/default-decks.
//   --weights FILE            Path to bot-weights.json (for baseline reference, ignored in
//                             practice since baselines come from BASELINE_WEIGHTS constant).

import { readFileSync, existsSync, writeFileSync, appendFileSync, mkdirSync, readdirSync, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve } from 'node:path';
import { fork } from 'node:child_process';

import '../engine.bundle.js';
import { BASELINE_WEIGHTS } from '../engine.bundle.js';
import { EVOLVING_REGIONS, loadWeightsFile, resolveWeightsForRegion, resolveWeightsForBot, getBotNamesForRegion } from '../engine.bundle.js';

const here = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(here, '..', '..');

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : process.argv[i + 1];
}

const WORKERS = Number(arg('workers', 6));
const GAMES_PER_PAIR = Number(arg('gamesPerPair', 4));
if (GAMES_PER_PAIR % 2 !== 0) {
  console.error(`--gamesPerPair must be even (got ${GAMES_PER_PAIR}) so half can be played each side`);
  process.exit(1);
}
const HALF_GAMES = GAMES_PER_PAIR / 2;
const REGIONS_FILTER = arg('regions', null);
const JSONL_PATH = resolve(arg('jsonl', join(REPO_ROOT, 'engine', 'evolution-generations.jsonl')));
const WEIGHTS_PATH = resolve(arg('weights', join(REPO_ROOT, 'engine', 'data', 'bot-weights.json')));
const DECKS_DIR = resolve(arg('decks', join(REPO_ROOT, 'app', 'resources', 'default-decks')));
const MAX_TURNS = Number(arg('maxTurns', 200));
const GAME_TIMEOUT_SEC = Number(arg('gameTimeoutSec', 300));
const OUT_DIR = resolve(arg('out', join(here, '..')));
const EXTRA_ENTRANTS_FILE = arg('extraEntrantsFile', null);
const SKIP_BASELINE = process.argv.includes('--skipBaseline');
const LATEST_GEN_ONLY = process.argv.includes('--latestGenOnly');
const SHAPE_C_ONLY = process.argv.includes('--shapeCOnly');
const SHARD_INDEX = Number(arg('shardIndex', 0));
const SHARD_COUNT = Number(arg('shardCount', 1));
const AGGREGATE_SHARDS_DIR = arg('aggregateShards', null);
if (SHARD_INDEX < 0 || SHARD_INDEX >= SHARD_COUNT) {
  console.error(`Invalid shardIndex ${SHARD_INDEX} for shardCount ${SHARD_COUNT}`);
  process.exit(1);
}

const SLOW_REGIONS_ARG = arg('slowRegions', 'Underneath,Weave,Paradwyn');
const SLOW_REGIONS = new Set(SLOW_REGIONS_ARG.split(',').map((s) => s.trim()).filter((s) => s));
const HEAVY_WEIGHT = 5;
const FOCUS_REGION = arg('focusRegion', null);

const CARDS_PATH = join(REPO_ROOT, 'data-pipeline', 'output', 'cards_final.json');
const RESULTS_PATH = join(OUT_DIR, 'roundrobin-results.txt');
const MATCHES_JSONL_PATH = join(
  OUT_DIR,
  SHARD_COUNT > 1 ? `roundrobin-matches-shard-${SHARD_INDEX}.jsonl` : 'roundrobin-matches.jsonl',
);
const ERRORS_JSONL_PATH = join(
  OUT_DIR,
  SHARD_COUNT > 1 ? `roundrobin-errors-shard-${SHARD_INDEX}.jsonl` : 'roundrobin-errors.jsonl',
);

const DECK_ID_TO_REGION = {
  'default-arderial': 'Arderial', 'default-bograth': 'Bograth', 'default-cald': 'Cald',
  'default-core': 'Core', 'default-dresh': "d'Resh", 'default-kybars-teeth': "Kybar's Teeth",
  'default-nar': 'Nar', 'default-naroom': 'Naroom', 'default-orothe': 'Orothe',
  'default-paradwyn': 'Paradwyn', 'default-underneath': 'Underneath',
  'default-universal': 'Universal', 'default-weave': 'Weave',
};

// Deck overrides (2026-08-21, mirrors swiss-tournament.mjs's flag).
const DECK_OVERRIDES = new Map();
for (let i = 0; i < process.argv.length; i += 1) {
  if (process.argv[i] === '--deckOverride' && i + 1 < process.argv.length) {
    const raw = process.argv[i + 1];
    const eq = raw.indexOf('=');
    if (eq < 0) { console.error(`Invalid --deckOverride "${raw}"`); process.exit(1); }
    const region = raw.slice(0, eq).trim();
    const p = raw.slice(eq + 1).trim();
    const absPath = resolve(REPO_ROOT, p);
    if (!existsSync(absPath)) { console.error(`--deckOverride path missing: ${absPath}`); process.exit(1); }
    DECK_OVERRIDES.set(region, absPath);
  }
}

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
    const override = DECK_OVERRIDES.get(region);
    const filePath = override ?? join(DECKS_DIR, `${deckId}.json`);
    if (!existsSync(filePath)) { console.error(`Missing deck file: ${filePath}`); process.exit(1); }
    if (override) console.log(`  [deck-override] ${region} using ${filePath}`);
    byRegion.set(region, expandDeck(JSON.parse(readFileSync(filePath, 'utf-8'))));
  }
  return byRegion;
}

/** Loads Shape-C (or all) champions grouped by region -- same shape swiss-tournament uses. */
function loadHistoricalChampions(jsonlPath) {
  const byRegion = new Map();
  if (!existsSync(jsonlPath)) {
    console.warn(`JSONL not found at ${jsonlPath} -- only baseline entrants will run.`);
    return byRegion;
  }
  let skipped = 0;
  const lines = readFileSync(jsonlPath, 'utf-8').split('\n').filter((l) => l.trim());
  for (const line of lines) {
    const row = JSON.parse(line);
    if (SHAPE_C_ONLY && row.methodology !== 'GH-Actions-ShapeC-betaOn') { skipped += 1; continue; }
    const arr = byRegion.get(row.region) ?? [];
    arr.push({ gen: row.gen, championWeights: row.championWeights, championFitness: row.championFitness });
    byRegion.set(row.region, arr);
  }
  for (const arr of byRegion.values()) arr.sort((a, b) => a.gen - b.gen);
  if (SHAPE_C_ONLY && skipped > 0) console.log(`  [shapeCOnly] filtered out ${skipped} non-Shape-C rows`);
  return byRegion;
}

function loadExtraEntrantWeights(weightsMode, historicalChampions, weightsFile) {
  if (!weightsMode || weightsMode === 'baseline') return { ...BASELINE_WEIGHTS };
  const m = weightsMode.match(/^jsonl:(.+):(\d+)$/);
  if (m) {
    const [, region, genStr] = m;
    const champs = historicalChampions.get(region) ?? [];
    const found = champs.find((c) => c.gen === Number(genStr));
    if (!found) { console.error(`Extra entrant "${weightsMode}" not found in JSONL`); process.exit(1); }
    return found.championWeights;
  }
  const bw = weightsMode.match(/^botweights:([^:]+):(.+)$/);
  if (bw) {
    const [, region, botName] = bw;
    if (!weightsFile) { console.error(`weightsMode "${weightsMode}" needs bot-weights.json but none loaded`); process.exit(1); }
    const names = getBotNamesForRegion(weightsFile, region);
    if (names.length > 0 && !names.includes(botName)) {
      console.error(`bot "${botName}" not found in region "${region}" (available: ${names.join(', ')})`); process.exit(1);
    }
    return resolveWeightsForBot(weightsFile, region, botName);
  }
  console.error(`Unknown weightsMode "${weightsMode}"`); process.exit(1);
}

function loadExtraEntrants(filePath, historicalChampions, weightsFile) {
  if (!filePath) return [];
  const absPath = resolve(filePath);
  if (!existsSync(absPath)) { console.error(`--extraEntrantsFile "${absPath}" not found`); process.exit(1); }
  const raw = JSON.parse(readFileSync(absPath, 'utf-8'));
  if (!Array.isArray(raw)) { console.error('extras file must be JSON array'); process.exit(1); }
  const extras = [];
  for (const entry of raw) {
    if (!entry.label || !entry.region || !entry.deckPath) {
      console.error(`Extra entrant missing field: ${JSON.stringify(entry)}`); process.exit(1);
    }
    const deckAbsPath = resolve(entry.deckPath);
    if (!existsSync(deckAbsPath)) { console.error(`Extra deckPath missing: ${deckAbsPath}`); process.exit(1); }
    const deckJson = JSON.parse(readFileSync(deckAbsPath, 'utf-8'));
    const { deckCardKeys, magiOrder } = expandDeck(deckJson);
    const weights = loadExtraEntrantWeights(entry.weightsMode, historicalChampions, weightsFile);
    extras.push({ label: entry.label, region: entry.region, deck: deckCardKeys, magiOrder, weights, gen: -1 });
  }
  return extras;
}

function buildEntrants(regionalDecks, historicalChampions, regionsFilter) {
  const entrants = [];
  const regionsToUse = regionsFilter ?? [...regionalDecks.keys()];
  for (const region of regionsToUse) {
    const deck = regionalDecks.get(region);
    if (!deck) { console.warn(`Region "${region}" has no deck file, skipping.`); continue; }
    if (!SKIP_BASELINE) {
      entrants.push({
        label: `${region}-baseline`, region,
        deck: deck.deckCardKeys, magiOrder: deck.magiOrder,
        weights: { ...BASELINE_WEIGHTS }, gen: 0,
      });
    }
    const champs = historicalChampions.get(region) ?? [];
    const gensToUse = LATEST_GEN_ONLY && champs.length > 0 ? [champs[champs.length - 1]] : champs;
    for (const c of gensToUse) {
      entrants.push({
        label: `${region}-gen${c.gen}`, region,
        deck: deck.deckCardKeys, magiOrder: deck.magiOrder,
        weights: c.championWeights, gen: c.gen,
      });
    }
  }
  return entrants;
}

/** Seed per game (2026-08-25 change): genuinely-random uint32 per game, sourced from
 *  `Math.random()`. Every game gets a fresh shuffle + fresh opening hands regardless of pair
 *  position or run history. HISTORICAL: prior formula `(aIdx*100000 + bIdx*100 + gameIdx) ^
 *  0x2f8b5d17` was deterministic per pair-index, which meant the SAME 4 hands were dealt for a
 *  given pair position across every run. Weights that memorized how to play those specific 4
 *  hands looked good in per-gen training but collapsed in the final RR where field composition
 *  shifted the pair indices. Fixed to expose bots to the full card-draw distribution -- fitness
 *  is now measured on deck-wide skill, not hand-specific pattern-matching. The random seed IS
 *  captured in each game's outcome record (see `gameOutcomes` push below) so any specific game
 *  can still be replayed for debugging by feeding the recorded seed back into the worker. */
function randomGameSeed() {
  return (Math.floor(Math.random() * 4294967296)) >>> 0;
}

// ============================================================================
// Worker pool (mirrors tournament.mjs / swiss-tournament.mjs's shape)
// ============================================================================

class WorkerPool {
  constructor(size, workerScriptPath, cardsPath) {
    this.workers = []; this.freeWorkers = []; this.queue = []; this.pending = new Map();
    this.nextJobId = 1; this.stopRequested = false; this.errorCounts = new Map();
    this._onJobCompleteHook = null;
    this.readyPromise = new Promise((resolveReady) => {
      let readyCount = 0;
      for (let i = 0; i < size; i += 1) {
        const workerEnv = { ...process.env };
        const w = fork(workerScriptPath, [cardsPath], { stdio: 'inherit', env: workerEnv });
        w.on('message', (msg) => this._onMessage(w, msg));
        w.on('exit', (code, signal) => { if (code !== 0 && code !== null) console.error(`worker ${w.pid} exited: ${code} ${signal}`); });
        w.on('error', (err) => console.error(`worker ${w.pid} error:`, err));
        this.workers.push(w);
        w._onReady = () => { readyCount += 1; this.freeWorkers.push(w); this._drain(); if (readyCount === size) resolveReady(); };
      }
    });
  }
  _onMessage(worker, msg) {
    if (msg.type === 'ready') { if (typeof worker._onReady === 'function') worker._onReady(); }
    else if (msg.type === 'result') {
      const p = this.pending.get(msg.jobId);
      if (!p) return;
      this.pending.delete(msg.jobId);
      if (msg.error) {
        const seen = this.errorCounts.get(msg.error);
        if (!seen) { this.errorCounts.set(msg.error, 1); console.error(`  [first occurrence] game threw: ${msg.error}`); }
        else { this.errorCounts.set(msg.error, seen + 1); if ((seen + 1) % 25 === 0) console.error(`  [${seen + 1}x] "${msg.error.slice(0, 80)}..."`); }
        try { appendFileSync(ERRORS_JSONL_PATH, JSON.stringify({ timestamp: new Date().toISOString(), signature: msg.error }) + '\n', 'utf-8'); } catch {}
      }
      p.resolve(msg.result);
      this.freeWorkers.push(worker);
      if (this._onJobCompleteHook) this._onJobCompleteHook();
      this._drain();
    }
  }
  _drain() {
    while (this.queue.length > 0 && this.freeWorkers.length > 0) {
      const worker = this.freeWorkers.shift();
      const { jobId, payload } = this.queue.shift();
      worker.send({ type: 'job', jobId, ...payload });
    }
  }
  runJob(payload) {
    const jobId = this.nextJobId; this.nextJobId += 1;
    return new Promise((resolveJob) => { this.pending.set(jobId, { resolve: resolveJob }); this.queue.push({ jobId, payload }); this._drain(); });
  }
  async ready() { return this.readyPromise; }
  async shutdown() {
    for (const w of this.workers) { try { w.send({ type: 'shutdown' }); } catch {} }
    await new Promise((r) => setTimeout(r, 200));
    for (const w of this.workers) if (!w.killed) w.kill();
  }
}

// ============================================================================
// Series semantics + aggregation
// ============================================================================

function pointsForOutcomeSide(outcome, side) {
  if (outcome === 'drawByDefeat' || outcome === 'drawByCap' || outcome === 'stuck') return 0.5;
  const winner = (outcome === 'p1' || outcome === 'p1ByTiebreak') ? 'p1' : 'p2';
  return winner === side ? 1 : 0;
}

function initJsonl(path) {
  const dir = dirname(path);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  writeFileSync(path, '', 'utf-8');
}

/** Append a per-pair series row (one line per pair) to the JSONL. Aggregator reads these
 *  back. Recording game-level breakdown so the aggregator can recompute if it wants. */
function appendPairJsonl(path, aLabel, bLabel, aRegion, bRegion, aGamesWon, bGamesWon, draws, gameOutcomes) {
  const row = { aLabel, bLabel, aRegion, bRegion, aGamesWon, bGamesWon, draws, totalGames: aGamesWon + bGamesWon + draws, gameOutcomes };
  appendFileSync(path, JSON.stringify(row) + '\n', 'utf-8');
}

// ============================================================================
// Aggregator mode
// ============================================================================

function findShardJsonls(dir) {
  const results = [];
  for (const entry of readdirSync(dir)) {
    const abs = join(dir, entry);
    const s = statSync(abs);
    if (s.isDirectory()) { for (const f of findShardJsonls(abs)) results.push(f); }
    else if (/^roundrobin-matches(-shard-\d+)?\.jsonl$/.test(entry)) results.push(abs);
  }
  return results;
}

/** Aggregator: reads shard JSONLs, tallies per-entrant series-W/L/D and game-W/L/D, produces
 *  final leaderboard. Series scoring: A-games > B-games => series win for A (1 pt), 2-2 => draw
 *  (0.5 pt each), etc. Uses game-level outcomes for the game-record columns. */
if (AGGREGATE_SHARDS_DIR) {
  const absDir = resolve(AGGREGATE_SHARDS_DIR);
  if (!existsSync(absDir)) { console.error(`--aggregateShards not found: ${absDir}`); process.exit(1); }
  console.log('=== roundrobin AGGREGATOR ===');
  console.log(`Shard dir: ${absDir}`);
  const files = findShardJsonls(absDir);
  if (files.length === 0) { console.error(`No shard JSONLs under ${absDir}`); process.exit(1); }
  console.log(`Found ${files.length} shard file(s):`);
  for (const f of files) console.log(`  ${f}`);
  console.log('');

  const entrants = new Map(); // label -> { region, seriesW, seriesL, seriesD, gameW, gameL, gameD }
  const matchups = new Map(); // "labelA|labelB" (sorted) -> row
  let rowCount = 0;
  // Detect actual games-per-pair from the FIRST pair row (all should agree). Falls back to the
  // process-level GAMES_PER_PAIR constant if no rows have totalGames set (shouldn't happen).
  let actualGamesPerPair = null;

  function ensureEntrant(label, region) {
    if (!entrants.has(label)) entrants.set(label, { label, region, seriesW: 0, seriesL: 0, seriesD: 0, gameW: 0, gameL: 0, gameD: 0 });
    return entrants.get(label);
  }

  for (const file of files) {
    const lines = readFileSync(file, 'utf-8').split('\n').filter((l) => l.trim());
    for (const line of lines) {
      const row = JSON.parse(line);
      rowCount += 1;
      if (actualGamesPerPair === null && typeof row.totalGames === 'number') actualGamesPerPair = row.totalGames;
      const a = ensureEntrant(row.aLabel, row.aRegion);
      const b = ensureEntrant(row.bLabel, row.bRegion);
      // Game-level tallies
      a.gameW += row.aGamesWon; a.gameL += row.bGamesWon; a.gameD += row.draws;
      b.gameW += row.bGamesWon; b.gameL += row.aGamesWon; b.gameD += row.draws;
      // Series result
      if (row.aGamesWon > row.bGamesWon) { a.seriesW += 1; b.seriesL += 1; }
      else if (row.bGamesWon > row.aGamesWon) { b.seriesW += 1; a.seriesL += 1; }
      else { a.seriesD += 1; b.seriesD += 1; }
      // Matchup matrix (unordered key)
      const [k1, k2] = [row.aLabel, row.bLabel].sort();
      matchups.set(`${k1}|${k2}`, { aLabel: row.aLabel, bLabel: row.bLabel, aGamesWon: row.aGamesWon, bGamesWon: row.bGamesWon, draws: row.draws });
    }
  }
  console.log(`Aggregator: merged ${rowCount} pair rows across ${entrants.size} entrants.`);
  console.log('');

  // Compute points and sort leaderboard
  const board = [...entrants.values()].map((e) => ({
    ...e,
    seriesPoints: e.seriesW + e.seriesD * 0.5,
    seriesTotal: e.seriesW + e.seriesL + e.seriesD,
    gameTotal: e.gameW + e.gameL + e.gameD,
  })).sort((a, b) => b.seriesPoints - a.seriesPoints || b.gameW - a.gameW || a.label.localeCompare(b.label));

  // Write results
  const lines = [];
  lines.push('=== Cross-region full-field round-robin results ===');
  lines.push(`Aggregated from ${files.length} shard file(s) at ${new Date().toISOString()}`);
  lines.push(`Total entrants: ${entrants.size}`);
  lines.push(`Total pair series: ${matchups.size}`);
  lines.push(`Games per pair: ${actualGamesPerPair ?? GAMES_PER_PAIR} (half as each side)`);
  lines.push('');
  lines.push('--- Full leaderboard ---');
  lines.push('Rank  Label                            SeriesW  SeriesL  SeriesD  Points   Games(W-L-D)');
  board.forEach((e, i) => {
    lines.push(`${String(i + 1).padStart(2)}    ${e.label.padEnd(30)} ${String(e.seriesW).padStart(6)}   ${String(e.seriesL).padStart(6)}   ${String(e.seriesD).padStart(6)}   ${e.seriesPoints.toFixed(1).padStart(6)}   ${e.gameW}-${e.gameL}-${e.gameD}`);
  });
  lines.push('');
  lines.push(`Each entrant played ${entrants.size - 1} series (once vs every other entrant), each series = ${actualGamesPerPair ?? GAMES_PER_PAIR} games.`);
  lines.push('SeriesW/L/D = pair series won / lost / drawn (2-2 = draw when games/pair = 4).');
  lines.push('Points = SeriesW + SeriesD * 0.5. Games(W-L-D) = total game-level record across all series.');
  const dir = dirname(RESULTS_PATH);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  writeFileSync(RESULTS_PATH, lines.join('\n'), 'utf-8');
  console.log(`Leaderboard written to: ${RESULTS_PATH}`);
  process.exit(0);
}

// ============================================================================
// Main tournament flow (non-aggregator mode)
// ============================================================================

const decksByRegion = loadRegionalDecks();
const weightsFile = existsSync(WEIGHTS_PATH) ? loadWeightsFile(WEIGHTS_PATH) : null;
const historicalChampions = loadHistoricalChampions(JSONL_PATH);
const regionsToUse = REGIONS_FILTER ? REGIONS_FILTER.split(',').map((s) => s.trim()) : EVOLVING_REGIONS;
const autoEntrants = buildEntrants(decksByRegion, historicalChampions, regionsToUse);
const extraEntrants = loadExtraEntrants(EXTRA_ENTRANTS_FILE, historicalChampions, weightsFile);
const entrants = [...autoEntrants, ...extraEntrants];
if (entrants.length < 2) { console.error(`Too few entrants (${entrants.length})`); process.exit(1); }

console.log('=== Cross-region full-field round-robin ===');
console.log(`Workers:              ${WORKERS}`);
console.log(`Games per pair:       ${GAMES_PER_PAIR} (${HALF_GAMES} as each side)`);
console.log(`Entrants:             ${entrants.length}`);
console.log(`Total unique pairs:   ${entrants.length * (entrants.length - 1) / 2}`);
console.log(`Total games planned:  ${entrants.length * (entrants.length - 1) / 2 * GAMES_PER_PAIR}`);
console.log(`Max turns / game:     ${MAX_TURNS}`);
console.log(`Game timeout:         ${GAME_TIMEOUT_SEC > 0 ? `${GAME_TIMEOUT_SEC}s` : 'disabled'}`);
console.log(`Sharding:             shard ${SHARD_INDEX} of ${SHARD_COUNT}`);
console.log(`Slow regions:         ${[...SLOW_REGIONS].join(', ') || '(none)'}`);
console.log(`Output dir:           ${OUT_DIR}`);
console.log('');
console.log('Entrant list:');
for (const e of entrants) console.log(`  ${e.label}`);
console.log('');

initJsonl(MATCHES_JSONL_PATH);
initJsonl(ERRORS_JSONL_PATH);

// Build the pair list: unique unordered pairs (i, j) with i < j. If --focusRegion is set,
// keep only pairs where at least one endpoint's region matches (opponent-vs-opponent pairs
// are discarded -- they're compute waste in focused per-generation runs).
const pairs = [];
for (let i = 0; i < entrants.length; i += 1) {
  for (let j = i + 1; j < entrants.length; j += 1) {
    if (FOCUS_REGION && entrants[i].region !== FOCUS_REGION && entrants[j].region !== FOCUS_REGION) continue;
    pairs.push({ aIdx: i, bIdx: j });
  }
}
if (FOCUS_REGION) {
  const fullCount = entrants.length * (entrants.length - 1) / 2;
  console.log(`Focus-region filter: keeping ${pairs.length}/${fullCount} pairs (only pairs involving "${FOCUS_REGION}")`);
}

// Apply weighted bin-packing sharding: heavy pairs (either endpoint in SLOW_REGIONS) get
// HEAVY_WEIGHT, others 1. Greedy longest-first assignment keeps per-shard total weight balanced.
if (SHARD_COUNT > 1) {
  const originalCount = pairs.length;
  const indexed = pairs.map((p, i) => ({
    i,
    weight: (SLOW_REGIONS.has(entrants[p.aIdx].region) || SLOW_REGIONS.has(entrants[p.bIdx].region)) ? HEAVY_WEIGHT : 1,
  }));
  indexed.sort((a, b) => b.weight - a.weight || a.i - b.i);
  const shardLoads = new Array(SHARD_COUNT).fill(0);
  const assignedShard = new Array(pairs.length);
  for (const { i, weight } of indexed) {
    let minShard = 0;
    for (let s = 1; s < SHARD_COUNT; s += 1) { if (shardLoads[s] < shardLoads[minShard]) minShard = s; }
    assignedShard[i] = minShard;
    shardLoads[minShard] += weight;
  }
  const pairsPerShard = new Array(SHARD_COUNT).fill(0);
  for (const s of assignedShard) pairsPerShard[s] += 1;
  console.log(`Shard weight balance: ${shardLoads.map((w, s) => `#${s}=${w} (${pairsPerShard[s]} pairs)`).join(', ')}`);
  for (let i = pairs.length - 1; i >= 0; i -= 1) {
    if (assignedShard[i] !== SHARD_INDEX) pairs.splice(i, 1);
  }
  console.log(`Shard ${SHARD_INDEX}/${SHARD_COUNT}: running ${pairs.length} of ${originalCount} pairs (${pairs.length * GAMES_PER_PAIR} games, weight ${shardLoads[SHARD_INDEX]}).`);
  console.log('');
}

const startedAt = Date.now();
const workerScriptPath = join(here, 'tournament-worker.mjs');
const pool = new WorkerPool(WORKERS, workerScriptPath, CARDS_PATH);
console.log(`Spinning up ${WORKERS} worker(s), waiting for card DB load...`);
await pool.ready();
console.log('Workers ready.');
console.log('');

// Progress reporter (every ~50 completions or ~90s, whichever first).
const totalGames = pairs.length * GAMES_PER_PAIR;
let completedGames = 0;
let lastLogAtMs = Date.now();
let lastLogAtCount = 0;
const LOG_EVERY_GAMES = Math.max(50, Math.floor(totalGames / 30));
const LOG_EVERY_MS = 90_000;
pool._onJobCompleteHook = () => {
  completedGames += 1;
  const nowMs = Date.now();
  const shouldLog = (completedGames - lastLogAtCount) >= LOG_EVERY_GAMES || (nowMs - lastLogAtMs) >= LOG_EVERY_MS;
  if (shouldLog && completedGames < totalGames) {
    lastLogAtCount = completedGames; lastLogAtMs = nowMs;
    const pct = (completedGames / totalGames * 100).toFixed(0);
    const elapsedS = ((nowMs - startedAt) / 1000).toFixed(0);
    const remainingS = completedGames > 0 ? (((nowMs - startedAt) / completedGames) * (totalGames - completedGames) / 1000).toFixed(0) : '?';
    process.stdout.write(`  ${completedGames}/${totalGames} games (${pct}%), ${elapsedS}s elapsed, ~${remainingS}s remaining\n`);
  }
};

// For each pair: run GAMES_PER_PAIR games (HALF_GAMES with A as p1, HALF_GAMES with B as p1).
// Fire all games in parallel via the pool; aggregate per-pair after Promise.all.
const pairJobPromises = pairs.map(async (pair) => {
  const entrantA = entrants[pair.aIdx];
  const entrantB = entrants[pair.bIdx];
  const gamePromises = [];
  for (let g = 0; g < GAMES_PER_PAIR; g += 1) {
    const aIsP1 = g < HALF_GAMES;
    const p1Entrant = aIsP1 ? entrantA : entrantB;
    const p2Entrant = aIsP1 ? entrantB : entrantA;
    const seed = randomGameSeed();
    const payload = {
      seed,
      p1Region: p1Entrant.region, p1Deck: p1Entrant.deck, p1MagiOrder: p1Entrant.magiOrder, p1Weights: p1Entrant.weights,
      p2Region: p2Entrant.region, p2Deck: p2Entrant.deck, p2MagiOrder: p2Entrant.magiOrder, p2Weights: p2Entrant.weights,
      maxTurns: MAX_TURNS, maxActions: 20000,
      timeoutMs: GAME_TIMEOUT_SEC > 0 ? GAME_TIMEOUT_SEC * 1000 : undefined,
    };
    gamePromises.push(pool.runJob(payload).then((r) => ({ r, aIsP1, seed })));
  }
  const gameResults = await Promise.all(gamePromises);
  let aWon = 0, bWon = 0, draws = 0;
  const gameOutcomes = [];
  for (const { r, aIsP1, seed } of gameResults) {
    const aPts = aIsP1 ? pointsForOutcomeSide(r.outcome, 'p1') : pointsForOutcomeSide(r.outcome, 'p2');
    const bPts = aIsP1 ? pointsForOutcomeSide(r.outcome, 'p2') : pointsForOutcomeSide(r.outcome, 'p1');
    if (aPts === 1) aWon += 1; else if (bPts === 1) bWon += 1; else draws += 1;
    // Seed captured for debug: individual games can be replayed by feeding the recorded seed
    // back into tournament-worker.mjs (identical bot decisions given identical weights + seed).
    gameOutcomes.push({ outcome: r.outcome, aIsP1, seed });
  }
  appendPairJsonl(MATCHES_JSONL_PATH, entrantA.label, entrantB.label, entrantA.region, entrantB.region, aWon, bWon, draws, gameOutcomes);
  return { pair, aWon, bWon, draws };
});

await Promise.all(pairJobPromises);
pool._onJobCompleteHook = null;

const elapsedMs = Date.now() - startedAt;
console.log('');
if (SHARD_COUNT > 1) {
  console.log(`=== Shard ${SHARD_INDEX}/${SHARD_COUNT} done. ${pairs.length} pairs (${totalGames} games) in ${(elapsedMs / 60000).toFixed(1)} min. ===`);
  console.log(`Per-pair JSONL: ${MATCHES_JSONL_PATH}`);
  console.log('(Full leaderboard produced by aggregator job, not this shard.)');
} else {
  // Non-shard mode: run the aggregator inline over the just-written JSONL so we get results.
  console.log(`=== Done. ${pairs.length} pairs (${totalGames} games) in ${(elapsedMs / 60000).toFixed(1)} min. ===`);
  console.log('Producing final leaderboard...');
  console.log('');
  // Reuse aggregator logic by spawning a subprocess would be complex; instead, replicate briefly.
  const entrantStats = new Map();
  for (const e of entrants) entrantStats.set(e.label, { label: e.label, region: e.region, seriesW: 0, seriesL: 0, seriesD: 0, gameW: 0, gameL: 0, gameD: 0 });
  const lines = readFileSync(MATCHES_JSONL_PATH, 'utf-8').split('\n').filter((l) => l.trim());
  for (const line of lines) {
    const row = JSON.parse(line);
    const a = entrantStats.get(row.aLabel), b = entrantStats.get(row.bLabel);
    a.gameW += row.aGamesWon; a.gameL += row.bGamesWon; a.gameD += row.draws;
    b.gameW += row.bGamesWon; b.gameL += row.aGamesWon; b.gameD += row.draws;
    if (row.aGamesWon > row.bGamesWon) { a.seriesW += 1; b.seriesL += 1; }
    else if (row.bGamesWon > row.aGamesWon) { b.seriesW += 1; a.seriesL += 1; }
    else { a.seriesD += 1; b.seriesD += 1; }
  }
  const board = [...entrantStats.values()].map((e) => ({ ...e, seriesPoints: e.seriesW + e.seriesD * 0.5 }))
    .sort((a, b) => b.seriesPoints - a.seriesPoints || b.gameW - a.gameW || a.label.localeCompare(b.label));
  const out = [];
  out.push('=== Cross-region full-field round-robin results ===');
  out.push(`Started: ${new Date(startedAt).toISOString()}`);
  out.push(`Elapsed: ${(elapsedMs / 60000).toFixed(1)} minutes`);
  out.push(`Entrants: ${entrants.length}   Pairs: ${pairs.length}   Games: ${totalGames}   Games/pair: ${GAMES_PER_PAIR}`);
  out.push('');
  out.push('--- Full leaderboard ---');
  out.push('Rank  Label                            SeriesW  SeriesL  SeriesD  Points   Games(W-L-D)');
  board.forEach((e, i) => {
    out.push(`${String(i + 1).padStart(2)}    ${e.label.padEnd(30)} ${String(e.seriesW).padStart(6)}   ${String(e.seriesL).padStart(6)}   ${String(e.seriesD).padStart(6)}   ${e.seriesPoints.toFixed(1).padStart(6)}   ${e.gameW}-${e.gameL}-${e.gameD}`);
  });
  writeFileSync(RESULTS_PATH, out.join('\n'), 'utf-8');
  console.log(`Leaderboard: ${RESULTS_PATH}`);
  console.log(`Per-pair JSONL: ${MATCHES_JSONL_PATH}`);
}

await pool.shutdown();
