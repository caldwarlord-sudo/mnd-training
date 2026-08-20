// AUTO-SYNCED from private repo. Do not edit here -- edits are overwritten on the next sync.
// Source of record: engine/scripts/evolve-generation.mjs in the private repo.

// Single-generation, single-region evolution runner for the GitHub Actions matrix
// (docs/plans/training-compute-scaling-plan.md, 2026-08-20).
//
// Differs from the local-desktop `evolve.mjs` in three important ways:
//   1. ONE generation per invocation (no outer generations loop). The workflow session drives
//      the multi-generation loop by triggering N runs of this script back-to-back.
//   2. ONE region per invocation (matrix job = 1 region). Each region job runs independently
//      and produces its own artifact; the aggregator merges them into an updated bot-weights.json.
//   3. Shape C hybrid fitness (owner-approved 2026-08-20): each candidate is evaluated by
//      MIRROR games vs its region's baseline PLUS CROSS-REGION games vs a rotating set of other
//      regions' baseline weights. Fitness combines the two: `0.25 * mirror + 0.75 * cross_region`.
//      Trained under beta ON, matching the shipping app's default search structure.
//
// The existing evolve.mjs (which does mirror-only, multi-gen, adaptive matchup counts) stays
// untouched -- it's still the local-machine tool, and the two use cases are different enough
// (long overnight local grind vs cloud-fan-out one-shot) that sharing code would obscure both.
// Mutation / population helpers ARE shared via engine/src/evolution/*.
//
// Usage from engine/ :
//   npm run build && node scripts/evolve-generation.mjs [OPTIONS]
//
// Options:
//   --region NAME             REQUIRED. Region to evolve this run (e.g. "Cald").
//   --generation N            REQUIRED. Generation number (>= 1). Used in seed derivation.
//   --populationSize N        Candidates to evaluate. Default 8.
//   --mirrorGames N           Games per candidate vs region baseline. Default 6.
//   --crossRegionOpponents N  How many OTHER regions each candidate plays. Default 3.
//   --gamesPerCrossOpponent N Games per candidate per cross-region opponent. Default 4.
//   --workers N               Parallel worker processes. Default 2.
//   --maxTurns N              Per-game turn cap. Default 200.
//   --gameTimeoutSec N        Per-game wall-clock cap. Default 300.
//   --seed N                  Base seed for population + game seeds. Default 42.
//   --weights PATH            bot-weights.json path. Default engine/data/bot-weights.json.
//   --decks DIR               Default-decks dir. Default app/resources/default-decks.
//   --out DIR                 Output directory. Default engine/.
//   --aggregate DIR           AGGREGATOR MODE: read every region-generation JSON from DIR
//                             (recursively), merge into an updated bot-weights.json plus a
//                             generation summary, and exit. No games run in this mode. Used
//                             by the workflow's aggregator job.
//
// Beta mode (OPPONENT_TURN_CROSSINGS=1) is expected to be set via env var by the caller
// (workflow does `OPPONENT_TURN_CROSSINGS: '1'`) -- tournament-worker.mjs already reads that
// env var at startup. Not a script option here to avoid two sources of truth.

import { readFileSync, existsSync, unlinkSync, writeFileSync, appendFileSync, mkdirSync, readdirSync, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve, basename } from 'node:path';
import { fork } from 'node:child_process';

import '../engine.bundle.js';
import { BASELINE_WEIGHTS } from '../engine.bundle.js';
import { EVOLVING_REGIONS, loadWeightsFile, resolveWeightsForRegion } from '../engine.bundle.js';
import { mutateWeights, crossoverWeights } from '../engine.bundle.js';
import { createSeededRng } from '../engine.bundle.js';

const here = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(here, '..', '..');

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : process.argv[i + 1];
}

const REGION = arg('region', null);
const GENERATION = Number(arg('generation', 0));
const POPULATION_SIZE = Number(arg('populationSize', 8));
const MIRROR_GAMES = Number(arg('mirrorGames', 6));
const CROSS_REGION_OPPONENTS = Number(arg('crossRegionOpponents', 3));
const GAMES_PER_CROSS_OPPONENT = Number(arg('gamesPerCrossOpponent', 4));
const WORKERS = Number(arg('workers', 2));
const MAX_TURNS = Number(arg('maxTurns', 200));
const GAME_TIMEOUT_SEC = Number(arg('gameTimeoutSec', 300));
const BASE_SEED = Number(arg('seed', 42));
const WEIGHTS_PATH = resolve(arg('weights', join(REPO_ROOT, 'engine', 'data', 'bot-weights.json')));
const DECKS_DIR = resolve(arg('decks', join(REPO_ROOT, 'app', 'resources', 'default-decks')));
const OUT_DIR = resolve(arg('out', join(here, '..')));
const AGGREGATE_DIR = arg('aggregate', null);

const CARDS_PATH = join(REPO_ROOT, 'data-pipeline', 'output', 'cards_final.json');

// Fitness blend: cross-region is weighted higher because cross-region play is what the shipped
// app actually does -- the mirror-match sanity check just confirms the vector can pilot its own
// deck at all. Owner-approved 2026-08-20.
const MIRROR_WEIGHT = 0.25;
const CROSS_REGION_WEIGHT = 0.75;

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

/** Deterministic seed per game -- unique per (generation, region, candidate, matchup, gameIdx).
 *  XOR constant differs from evolve.mjs's seedFor and tournament seeds so evolution-generation
 *  runs can never coincidentally reproduce another run's game states. */
function evolveGenSeedFor(generation, regionIdx, candidateIdx, matchupIdx, gameIdx) {
  const h = (generation * 1_000_000 + regionIdx * 10_000 + candidateIdx * 100 + matchupIdx * 10 + gameIdx) >>> 0;
  return ((h ^ 0x9b1c4e37) + BASE_SEED) >>> 0;
}

/** Cross-region opponent rotation: for a given (generation, region), pick N opponents from the
 *  other 12 regions such that (1) the same region+gen picks the same opponents (deterministic),
 *  and (2) over ~4-5 generations, every other region gets sampled at least once. Uses a simple
 *  offset walk: start position depends on region index and generation, then take N consecutive.
 *  Excludes self. */
function crossRegionOpponentsFor(generation, regionIdx, allRegions, n) {
  const others = allRegions.filter((_, i) => i !== regionIdx);
  if (n >= others.length) return others;
  const start = ((regionIdx * 3) + (generation * n)) % others.length;
  const picks = [];
  for (let i = 0; i < n; i += 1) picks.push(others[(start + i) % others.length]);
  return picks;
}

// ============================================================================
// Worker pool (mirrors tournament.mjs's shape, one file to keep script self-contained)
// ============================================================================

class WorkerPool {
  constructor(size, workerScriptPath, cardsPath) {
    this.workers = [];
    this.freeWorkers = [];
    this.queue = [];
    this.pending = new Map();
    this.nextJobId = 1;
    this.errorCounts = new Map();
    this._onJobCompleteHook = null;
    this.readyPromise = new Promise((resolveReady) => {
      let readyCount = 0;
      for (let i = 0; i < size; i += 1) {
        const workerEnv = { ...process.env };
        const w = fork(workerScriptPath, [cardsPath], { stdio: 'inherit', env: workerEnv });
        w.on('message', (msg) => this._onMessage(w, msg));
        w.on('exit', (code, signal) => {
          if (code !== 0 && code !== null) console.error(`worker ${w.pid} exited with code ${code} (signal ${signal})`);
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
    if (msg.type === 'ready') { if (typeof worker._onReady === 'function') worker._onReady(); }
    else if (msg.type === 'result') {
      const p = this.pending.get(msg.jobId);
      if (!p) return;
      this.pending.delete(msg.jobId);
      if (msg.error) {
        const seen = this.errorCounts.get(msg.error);
        if (!seen) { this.errorCounts.set(msg.error, 1); console.error(`  [first occurrence] game threw: ${msg.error}`); }
        else this.errorCounts.set(msg.error, seen + 1);
        try {
          appendFileSync(ERRORS_JSONL_PATH, JSON.stringify({ timestamp: new Date().toISOString(), signature: msg.error }) + '\n', 'utf-8');
        } catch { /* file write failure is non-fatal */ }
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
    return new Promise((resolveJob) => {
      this.pending.set(jobId, { resolve: resolveJob });
      this.queue.push({ jobId, payload });
      this._drain();
    });
  }
  async ready() { return this.readyPromise; }
  async shutdown() {
    for (const w of this.workers) { try { w.send({ type: 'shutdown' }); } catch {} }
    await new Promise((r) => setTimeout(r, 200));
    for (const w of this.workers) if (!w.killed) w.kill();
  }
}

// ============================================================================
// Fitness helpers
// ============================================================================

function pointsForOutcome(outcome, side) {
  if (outcome === 'drawByDefeat' || outcome === 'drawByCap' || outcome === 'stuck') return 0.5;
  const winner = (outcome === 'p1' || outcome === 'p1ByTiebreak') ? 'p1' : 'p2';
  return winner === side ? 1 : 0;
}

function combineFitness(mirrorRate, crossRegionRate) {
  return MIRROR_WEIGHT * mirrorRate + CROSS_REGION_WEIGHT * crossRegionRate;
}

// ============================================================================
// Aggregator mode -- merge per-region outputs into an updated bot-weights.json
// ============================================================================

function findRegionOutputs(dir) {
  const results = [];
  for (const entry of readdirSync(dir)) {
    const abs = join(dir, entry);
    const s = statSync(abs);
    if (s.isDirectory()) {
      for (const f of findRegionOutputs(abs)) results.push(f);
    } else if (/^evolve-region-.+-gen-\d+\.json$/.test(entry)) {
      results.push(abs);
    }
  }
  return results;
}

if (AGGREGATE_DIR) {
  const absDir = resolve(AGGREGATE_DIR);
  if (!existsSync(absDir)) {
    console.error(`--aggregate directory not found: ${absDir}`);
    process.exit(1);
  }
  console.log('=== evolve-generation AGGREGATOR ===');
  console.log(`Region-output dir: ${absDir}`);
  console.log(`Output dir:        ${OUT_DIR}`);

  const files = findRegionOutputs(absDir);
  if (files.length === 0) {
    console.error(`Aggregator: no evolve-region-*-gen-*.json files found under ${absDir}`);
    process.exit(1);
  }
  console.log(`Aggregator: found ${files.length} region-output file(s):`);
  for (const f of files) console.log(`  ${f}`);
  console.log('');

  // Start from the on-disk baseline weights file to preserve any regions not in this gen.
  const baseline = existsSync(WEIGHTS_PATH) ? loadWeightsFile(WEIGHTS_PATH) : { baseline: { ...BASELINE_WEIGHTS }, regions: {} };
  const summary = { generation: null, regions: [], startedAt: null, finishedAt: new Date().toISOString() };

  for (const file of files) {
    const data = JSON.parse(readFileSync(file, 'utf-8'));
    if (summary.generation === null) summary.generation = data.generation;
    else if (summary.generation !== data.generation) {
      console.error(`Aggregator: region files disagree on generation (${summary.generation} vs ${data.generation}). Refusing to merge.`);
      process.exit(1);
    }
    baseline.regions[data.region] = data.champion.weights;
    summary.regions.push({
      region: data.region,
      championFitness: data.champion.fitness,
      championMirrorWinRate: data.champion.mirrorWinRate,
      championCrossRegionWinRate: data.champion.crossRegionWinRate,
      populationSize: data.population.length,
      candidates: data.population.map((c) => ({
        idx: c.idx,
        fitness: c.fitness,
        mirrorWinRate: c.mirrorWinRate,
        crossRegionWinRate: c.crossRegionWinRate,
        origin: c.origin,
      })),
      crossRegionOpponents: data.crossRegionOpponents,
      regionElapsedSec: data.regionElapsedSec,
    });
  }

  // Write updated bot-weights.json + generation summary.
  const outWeightsPath = join(OUT_DIR, 'bot-weights.json');
  mkdirSync(OUT_DIR, { recursive: true });
  writeFileSync(outWeightsPath, JSON.stringify(baseline, null, 2) + '\n', 'utf-8');
  const summaryPath = join(OUT_DIR, `evolution-generation-${summary.generation}-summary.json`);
  writeFileSync(summaryPath, JSON.stringify(summary, null, 2) + '\n', 'utf-8');

  // Human-readable digest too, easier to eyeball in `cat` output.
  const lines = [];
  lines.push(`=== Evolution generation ${summary.generation} aggregated summary ===`);
  lines.push(`Finished: ${summary.finishedAt}`);
  lines.push(`Regions evaluated: ${summary.regions.length}`);
  lines.push('');
  lines.push('Champion per region (mirror-win% | cross-region-win% | fitness):');
  for (const r of summary.regions.sort((a, b) => b.championFitness - a.championFitness)) {
    const m = (r.championMirrorWinRate * 100).toFixed(1).padStart(5);
    const c = (r.championCrossRegionWinRate * 100).toFixed(1).padStart(5);
    const f = r.championFitness.toFixed(3);
    lines.push(`  ${r.region.padEnd(18)} mirror ${m}%  cross ${c}%  fitness ${f}   (${r.populationSize} candidates, opponents: ${r.crossRegionOpponents.join(', ')})`);
  }
  const digestPath = join(OUT_DIR, `evolution-generation-${summary.generation}-summary.txt`);
  writeFileSync(digestPath, lines.join('\n') + '\n', 'utf-8');

  console.log(`Aggregator: wrote updated ${outWeightsPath}`);
  console.log(`Aggregator: wrote ${summaryPath}`);
  console.log(`Aggregator: wrote ${digestPath}`);
  process.exit(0);
}

// ============================================================================
// Region-run mode -- evaluate one region's population for one generation
// ============================================================================

if (!REGION) {
  console.error('Missing required --region');
  process.exit(1);
}
if (!GENERATION || GENERATION < 1) {
  console.error('Missing required --generation (must be >= 1)');
  process.exit(1);
}
if (!EVOLVING_REGIONS.includes(REGION)) {
  console.error(`Region "${REGION}" not in EVOLVING_REGIONS: ${EVOLVING_REGIONS.join(', ')}`);
  process.exit(1);
}

const regionIdx = EVOLVING_REGIONS.indexOf(REGION);
const decksByRegion = loadRegionalDecks();
const weightsFile = loadWeightsFile(WEIGHTS_PATH);
const regionDeck = decksByRegion.get(REGION);
const regionBaselineWeights = { ...BASELINE_WEIGHTS };
// Warm start: current champion for this region (or baseline if no evolved champion yet).
const seedChampionWeights = resolveWeightsForRegion(weightsFile, REGION);

// Output filenames (single region, single generation).
const OUTPUT_JSON_PATH = join(OUT_DIR, `evolve-region-${REGION.replace(/[^A-Za-z0-9]/g, '_')}-gen-${GENERATION}.json`);
const ERRORS_JSONL_PATH = join(OUT_DIR, `evolve-region-${REGION.replace(/[^A-Za-z0-9]/g, '_')}-gen-${GENERATION}-errors.jsonl`);

console.log('=== evolve-generation (single region, single gen) ===');
console.log(`Region:                       ${REGION}`);
console.log(`Generation:                   ${GENERATION}`);
console.log(`Population size:              ${POPULATION_SIZE}`);
console.log(`Mirror games per candidate:   ${MIRROR_GAMES}`);
console.log(`Cross-region opponents:       ${CROSS_REGION_OPPONENTS}`);
console.log(`Games per cross-region opp:   ${GAMES_PER_CROSS_OPPONENT}`);
console.log(`Total games per candidate:    ${MIRROR_GAMES + CROSS_REGION_OPPONENTS * GAMES_PER_CROSS_OPPONENT}`);
console.log(`Total games this run:         ${POPULATION_SIZE * (MIRROR_GAMES + CROSS_REGION_OPPONENTS * GAMES_PER_CROSS_OPPONENT)}`);
console.log(`Workers:                      ${WORKERS}`);
console.log(`Max turns / game:             ${MAX_TURNS}`);
console.log(`Game timeout:                 ${GAME_TIMEOUT_SEC > 0 ? `${GAME_TIMEOUT_SEC}s` : 'disabled'}`);
console.log(`Beta mode env var:            OPPONENT_TURN_CROSSINGS=${process.env.OPPONENT_TURN_CROSSINGS ?? '0'}`);
console.log(`Fitness blend:                mirror ${MIRROR_WEIGHT} + cross-region ${CROSS_REGION_WEIGHT}`);
console.log(`Output JSON:                  ${OUTPUT_JSON_PATH}`);
console.log('');

// Cross-region opponent selection.
const crossRegionOpponents = crossRegionOpponentsFor(GENERATION, regionIdx, EVOLVING_REGIONS, CROSS_REGION_OPPONENTS);
console.log(`Cross-region opponents this run: ${crossRegionOpponents.join(', ')}`);
console.log('');

// Reset errors JSONL on fresh run.
mkdirSync(OUT_DIR, { recursive: true });
writeFileSync(ERRORS_JSONL_PATH, '', 'utf-8');

// ============================================================================
// Build population: [champion (slot 0)] + [mutations + crossovers]
// ============================================================================

// Deterministic RNG for mutation (separate seed stream from game RNG so game seeds change
// without shifting the population, and vice versa).
const mutationRng = createSeededRng(evolveGenSeedFor(GENERATION, regionIdx, 0, 0, 0));

const population = [];
// Slot 0: the current champion, unchanged. Serves as a "must not lose ground" anchor.
population.push({ idx: 0, weights: { ...seedChampionWeights }, origin: 'champion' });
// Slots 1..POPULATION_SIZE-1: alternate between mutations of champion and crossovers of two
// random prior population members. Alternating keeps population diverse rather than all-mutations.
for (let i = 1; i < POPULATION_SIZE; i += 1) {
  if (i % 2 === 1) {
    // Mutation of champion
    population.push({ idx: i, weights: mutateWeights(seedChampionWeights, mutationRng), origin: `mutate-champion` });
  } else {
    // Crossover of two random prior members
    const aIdx = Math.floor(mutationRng() * i);
    const bIdx = Math.floor(mutationRng() * i);
    const parent1 = population[aIdx].weights;
    const parent2 = population[bIdx].weights;
    population.push({ idx: i, weights: crossoverWeights(parent1, parent2, mutationRng), origin: `crossover-${aIdx}-${bIdx}` });
  }
}

// ============================================================================
// Build the job list
// ============================================================================

const jobs = [];
// Mirror games: each candidate plays MIRROR_GAMES vs region's baseline weights on region's deck.
for (let candIdx = 0; candIdx < POPULATION_SIZE; candIdx += 1) {
  const cand = population[candIdx];
  for (let g = 0; g < MIRROR_GAMES; g += 1) {
    jobs.push({
      candIdx,
      matchupType: 'mirror',
      opponentRegion: REGION,
      payload: {
        p1Region: REGION, p1Deck: regionDeck.deckCardKeys, p1MagiOrder: regionDeck.magiOrder, p1Weights: cand.weights,
        p2Region: REGION, p2Deck: regionDeck.deckCardKeys, p2MagiOrder: regionDeck.magiOrder, p2Weights: regionBaselineWeights,
        seed: evolveGenSeedFor(GENERATION, regionIdx, candIdx, 0, g),
        maxTurns: MAX_TURNS,
        maxActions: 20000,
        timeoutMs: GAME_TIMEOUT_SEC > 0 ? GAME_TIMEOUT_SEC * 1000 : undefined,
      },
    });
  }
}
// Cross-region games: each candidate plays GAMES_PER_CROSS_OPPONENT vs each opponent's baseline.
// Candidate always pilots the REGION's deck; opponent pilots opponent's deck with baseline weights.
for (let candIdx = 0; candIdx < POPULATION_SIZE; candIdx += 1) {
  const cand = population[candIdx];
  for (let oppIdx = 0; oppIdx < crossRegionOpponents.length; oppIdx += 1) {
    const oppRegion = crossRegionOpponents[oppIdx];
    const oppDeck = decksByRegion.get(oppRegion);
    for (let g = 0; g < GAMES_PER_CROSS_OPPONENT; g += 1) {
      jobs.push({
        candIdx,
        matchupType: 'crossRegion',
        opponentRegion: oppRegion,
        payload: {
          p1Region: REGION, p1Deck: regionDeck.deckCardKeys, p1MagiOrder: regionDeck.magiOrder, p1Weights: cand.weights,
          p2Region: oppRegion, p2Deck: oppDeck.deckCardKeys, p2MagiOrder: oppDeck.magiOrder, p2Weights: { ...BASELINE_WEIGHTS },
          seed: evolveGenSeedFor(GENERATION, regionIdx, candIdx, oppIdx + 1, g),
          maxTurns: MAX_TURNS,
          maxActions: 20000,
          timeoutMs: GAME_TIMEOUT_SEC > 0 ? GAME_TIMEOUT_SEC * 1000 : undefined,
        },
      });
    }
  }
}

console.log(`Total games to run: ${jobs.length}`);
console.log('');

// ============================================================================
// Run games
// ============================================================================

const startedAt = Date.now();
const workerScriptPath = join(here, 'tournament-worker.mjs');
const pool = new WorkerPool(WORKERS, workerScriptPath, CARDS_PATH);
console.log(`Spinning up ${WORKERS} worker(s), waiting for card DB load...`);
await pool.ready();
console.log('Workers ready.');
console.log('');

let completedJobs = 0;
let lastLogAtMs = Date.now();
let lastLogAtCount = 0;
const LOG_EVERY_JOBS = Math.max(20, Math.floor(jobs.length / 20));
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
    process.stdout.write(`  ${REGION} gen ${GENERATION}: ${completedJobs}/${jobs.length} games (${pct}%), ${elapsedS}s elapsed, ~${remainingS}s remaining\n`);
  }
};

const results = await Promise.all(jobs.map((j) => pool.runJob(j.payload)));
pool._onJobCompleteHook = null;

// ============================================================================
// Compute fitness per candidate
// ============================================================================

const perCandidate = new Map(); // candIdx -> { mirrorWins, mirrorGames, crossWins, crossGames }
for (let i = 0; i < jobs.length; i += 1) {
  const j = jobs[i];
  const r = results[i];
  const points = pointsForOutcome(r.outcome, 'p1'); // candidate is always p1 here
  let e = perCandidate.get(j.candIdx);
  if (!e) { e = { mirrorWins: 0, mirrorGames: 0, crossWins: 0, crossGames: 0 }; perCandidate.set(j.candIdx, e); }
  if (j.matchupType === 'mirror') { e.mirrorWins += points; e.mirrorGames += 1; }
  else { e.crossWins += points; e.crossGames += 1; }
}

const scoredCandidates = [];
for (let i = 0; i < POPULATION_SIZE; i += 1) {
  const cand = population[i];
  const e = perCandidate.get(i) ?? { mirrorWins: 0, mirrorGames: 1, crossWins: 0, crossGames: 1 };
  const mirrorRate = e.mirrorGames > 0 ? e.mirrorWins / e.mirrorGames : 0.5;
  const crossRate = e.crossGames > 0 ? e.crossWins / e.crossGames : 0.5;
  const fitness = combineFitness(mirrorRate, crossRate);
  scoredCandidates.push({
    idx: cand.idx,
    origin: cand.origin,
    weights: cand.weights,
    mirrorWinRate: mirrorRate,
    crossRegionWinRate: crossRate,
    fitness,
  });
}

// Pick champion: highest fitness. Tiebreak by higher cross-region rate (matches ship priority).
scoredCandidates.sort((a, b) => b.fitness - a.fitness || b.crossRegionWinRate - a.crossRegionWinRate);
const champion = scoredCandidates[0];

const elapsedMs = Date.now() - startedAt;

console.log('');
console.log(`=== ${REGION} gen ${GENERATION} done. ${jobs.length} games in ${(elapsedMs / 60000).toFixed(1)} minutes. ===`);
console.log(`Champion: slot ${champion.idx} (${champion.origin})`);
console.log(`  mirror win rate:       ${(champion.mirrorWinRate * 100).toFixed(1)}%`);
console.log(`  cross-region win rate: ${(champion.crossRegionWinRate * 100).toFixed(1)}%`);
console.log(`  combined fitness:      ${champion.fitness.toFixed(3)}`);
console.log('');
console.log('Full population ranking:');
for (const c of scoredCandidates) {
  const marker = c.idx === champion.idx ? ' <- champion' : '';
  console.log(`  slot ${c.idx} (${c.origin.padEnd(20)}): fitness ${c.fitness.toFixed(3)} (mirror ${(c.mirrorWinRate * 100).toFixed(0)}%, cross ${(c.crossRegionWinRate * 100).toFixed(0)}%)${marker}`);
}

// ============================================================================
// Write output JSON
// ============================================================================

const output = {
  region: REGION,
  generation: GENERATION,
  regionElapsedSec: Math.round(elapsedMs / 1000),
  crossRegionOpponents,
  seedChampionOrigin: 'bot-weights.json (or baseline if no evolved champion yet)',
  champion: {
    idx: champion.idx,
    origin: champion.origin,
    weights: champion.weights,
    mirrorWinRate: champion.mirrorWinRate,
    crossRegionWinRate: champion.crossRegionWinRate,
    fitness: champion.fitness,
  },
  population: scoredCandidates,
  config: {
    populationSize: POPULATION_SIZE,
    mirrorGames: MIRROR_GAMES,
    crossRegionOpponents: CROSS_REGION_OPPONENTS,
    gamesPerCrossOpponent: GAMES_PER_CROSS_OPPONENT,
    maxTurns: MAX_TURNS,
    gameTimeoutSec: GAME_TIMEOUT_SEC,
    baseSeed: BASE_SEED,
    mirrorWeight: MIRROR_WEIGHT,
    crossRegionWeight: CROSS_REGION_WEIGHT,
    betaMode: process.env.OPPONENT_TURN_CROSSINGS === '1',
  },
};
writeFileSync(OUTPUT_JSON_PATH, JSON.stringify(output, null, 2) + '\n', 'utf-8');
console.log(`Wrote: ${OUTPUT_JSON_PATH}`);

await pool.shutdown();
