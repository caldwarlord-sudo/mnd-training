// AUTO-SYNCED from private repo. Do not edit here -- edits are overwritten on the next sync.
// Source of record: engine/scripts/evolve-underneath-focused.mjs in the private repo.

// Focused per-region evolution runner (2026-08-22, initially built for Underneath v2 deck).
//
// Owner-approved shape (2026-08-22, after the round-robin proved Swiss-style sampling is too
// noisy for reliable fitness signal at small population sizes):
//
//   Per generation, the field is:
//     - baseline (stable anchor)
//     - best-so-far champion (persistent anchor, only replaced when a mutation cleanly beats it)
//     - 4 mutations of best-so-far
//     - 12 opponents = every OTHER evolving region's current shipping bot (from bot-weights.json;
//       absent entry = baseline)
//   = 18 entrants per generation.
//
//   Every pair-with-focus-region plays a Bo4 series (2 games each side, 2-2 draws allowed) --
//   exactly the same shape as the successful round-robin. Opponent-vs-opponent pairs are skipped
//   via roundrobin.mjs's --focusRegion flag (they'd be compute waste).
//
//   After the generation completes, the leaderboard's top Underneath entrant is compared to
//   best-so-far: if the mutation cleanly outperformed best-so-far in series points, it becomes
//   the new best-so-far. Otherwise best-so-far persists to next generation. Baseline being the
//   winner is a "no mutation improved; hold" outcome that also keeps best-so-far.
//
// Why this exists alongside evolve-generation.mjs:
//   evolve-generation.mjs uses a wider population (~8-30) with only 3 rotating cross-region
//   opponents per candidate -- great for exploring the weight space, but the per-candidate
//   sample is small enough that fitness scores swing generation-to-generation. This wrapper
//   trades population size for per-candidate signal quality: 4 mutations, but each one gets
//   the full round-robin treatment against all 12 real opponents. Best-so-far as an anchor
//   also gives us a "never regress" guarantee -- a bad generation doesn't dethrone a good
//   champion just because the mutations happened to draw well.
//
// Sharded execution modes (2026-09-05, added to unblock Cald v1 gen 4 hitting the GH 6h cap):
//   Default invocation (no --planOnly / --aggregateOnly) runs the full sequential pipeline
//   generation-by-generation, exactly as before -- backward-compat for local runs and the
//   single-runner fallback if sharding is ever unavailable.
//
//   Sharded workflow splits each gen into three jobs:
//     plan   : `--planOnly <planPath>`                        -- reads genlog, computes
//                                                                best-so-far, generates mutations,
//                                                                writes field files + plan JSON.
//                                                                No roundrobin, no genlog append.
//     shard  : `roundrobin.mjs --shardIndex N --shardCount M` -- invoked directly, in parallel.
//     aggregate: `--aggregateOnly <planPath> --resultsDir <d>` -- reads plan + merged leaderboard
//                                                                (already produced by
//                                                                `roundrobin.mjs --aggregateShards`),
//                                                                runs champion selection, appends
//                                                                genlog row.
//
//   plan.json persists the mutation vectors + best-so-far snapshot across the plan->aggregate
//   process boundary. Regenerating mutations in aggregate (rather than reading plan.json) would
//   be deterministic today but fragile -- a mutateWeights or seed-derivation change would
//   silently desync the fields.
//
//   generations>1 is REJECTED in --planOnly / --aggregateOnly modes: each gen depends on the
//   previous gen's genlog row, so a matrix-sharded workflow can only do one gen per invocation.
//   Full sequential mode still supports >1 gens for local batches.
//
// Usage from engine/ :
//   npm run build && node scripts/evolve-underneath-focused.mjs [OPTIONS]
//
// Options:
//   --region NAME             REQUIRED. Region to evolve (default: Underneath).
//   --deckPath PATH           REQUIRED. Deck file for the region (e.g. engine/decks-experimental/
//                             underneath-test-v2.json).
//   --generations N           How many generations to run in this invocation. Default 1.
//                             MUST be 1 when --planOnly or --aggregateOnly is set.
//   --startGeneration N       Generation number to start at. Default = max(existing gen)+1 or 1.
//   --populationSize N        Mutations per generation (in addition to baseline + best-so-far).
//                             Default 4.
//   --mutationSigma N         Optional. Override mutateWeights's default sigma-relative (0.15).
//   --gamesPerPair N          Games per pair series. Default 4 (2 each side).
//   --workers N               Parallel worker processes. Default 2.
//   --maxTurns N              Per-game turn cap. Default 200.
//   --gameTimeoutSec N        Per-game wall-clock cap. Default 300.
//   --seed N                  Base seed. Default 42.
//   --weights PATH            bot-weights.json path. Default engine/data/bot-weights.json.
//   --genLog PATH             evolution-generations.jsonl path. Default engine/evolution-generations.jsonl.
//   --out DIR                 Output directory. Default engine/.
//   --methodology TAG         Tag stored in genLog rows so this run's champions are distinguishable
//                             from Shape-C rows. Default focused-underneath-v2.
//
// Sharded-mode options:
//   --planOnly PATH           Write plan JSON to PATH, generate field files, then exit. Skips
//                             roundrobin invocation and genlog append. See "Sharded execution
//                             modes" above.
//   --aggregateOnly PATH      Read plan JSON from PATH plus --resultsDir's merged leaderboard,
//                             run champion selection, append genlog row, then exit.
//   --resultsDir DIR          For --aggregateOnly: directory containing roundrobin-results.txt
//                             (produced by `roundrobin.mjs --aggregateShards`). Required with
//                             --aggregateOnly.
//   --failedShards CSV        For --aggregateOnly: comma-separated shard indices that failed
//                             (e.g. "1,3"). Empty = all shards succeeded. When non-empty, the
//                             appended genlog row carries a `partialShards` field so downstream
//                             analysis can caveat conclusions drawn from partial-data gens.
//   --totalShards N           For --aggregateOnly: total shard count for the run (used with
//                             --failedShards to record partialShards.totalShards). Required
//                             when --failedShards is non-empty.
//
// Methodology tag: rows appended to evolution-generations.jsonl carry methodology =
// "focused-underneath-v2" by default so a future run's best-so-far lookup can filter to only
// its own history (owner might want to run several rounds of this same wrapper on different
// decks in the future -- each with its own tag).
//
// Beta mode: OPPONENT_TURN_CROSSINGS=1 must be set via env var by the caller (same convention
// as evolve-generation.mjs). The workflow does this; local invocations should too.

import { readFileSync, existsSync, appendFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

import '../engine.bundle.js';
import { BASELINE_WEIGHTS } from '../engine.bundle.js';
import { EVOLVING_REGIONS, loadWeightsFile, resolveWeightsForRegion } from '../engine.bundle.js';
import { mutateWeights } from '../engine.bundle.js';
import { createSeededRng } from '../engine.bundle.js';

const here = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(here, '..', '..');

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : process.argv[i + 1];
}

const REGION = arg('region', 'Underneath');
const DECK_PATH_RAW = arg('deckPath', null);
if (!DECK_PATH_RAW) {
  console.error('--deckPath is required (e.g. engine/decks-experimental/underneath-test-v2.json)');
  process.exit(1);
}
const DECK_PATH = resolve(REPO_ROOT, DECK_PATH_RAW);
if (!existsSync(DECK_PATH)) { console.error(`Deck file missing: ${DECK_PATH}`); process.exit(1); }
const GENERATIONS_TO_RUN = Number(arg('generations', 1));
const START_GENERATION_OVERRIDE = arg('startGeneration', null);
const POPULATION_SIZE = Number(arg('populationSize', 4));
const MUTATION_SIGMA = arg('mutationSigma', null);
const GAMES_PER_PAIR = Number(arg('gamesPerPair', 4));
const WORKERS = Number(arg('workers', 2));
const MAX_TURNS = Number(arg('maxTurns', 200));
const GAME_TIMEOUT_SEC = Number(arg('gameTimeoutSec', 300));
const BASE_SEED = Number(arg('seed', 42));
const WEIGHTS_PATH = resolve(arg('weights', join(REPO_ROOT, 'engine', 'data', 'bot-weights.json')));
const GEN_LOG_PATH = resolve(arg('genLog', join(REPO_ROOT, 'engine', 'evolution-generations.jsonl')));
const OUT_DIR = resolve(arg('out', join(here, '..')));
const METHODOLOGY = arg('methodology', 'focused-underneath-v2');

// Sharded-mode flags. All three modes are mutually exclusive; enforcement below.
const PLAN_ONLY_PATH = arg('planOnly', null);
const AGGREGATE_ONLY_PATH = arg('aggregateOnly', null);
const RESULTS_DIR = arg('resultsDir', null);
const FAILED_SHARDS_RAW = arg('failedShards', '');
const TOTAL_SHARDS_RAW = arg('totalShards', null);

if (PLAN_ONLY_PATH && AGGREGATE_ONLY_PATH) {
  console.error('--planOnly and --aggregateOnly are mutually exclusive');
  process.exit(1);
}
const MODE = PLAN_ONLY_PATH ? 'planOnly' : (AGGREGATE_ONLY_PATH ? 'aggregateOnly' : 'full');
if (MODE !== 'full' && GENERATIONS_TO_RUN !== 1) {
  console.error(
    `--generations must be 1 when --${MODE} is set (workflow enforces one gen per invocation; ` +
    'see docs/plans/per-region-training-methodology.md §1 for the session-driven per-gen pattern).'
  );
  process.exit(1);
}
if (MODE === 'aggregateOnly' && !RESULTS_DIR) {
  console.error('--aggregateOnly requires --resultsDir DIR (containing roundrobin-results.txt from the merged shards)');
  process.exit(1);
}
const FAILED_SHARD_INDICES = FAILED_SHARDS_RAW
  ? FAILED_SHARDS_RAW.split(',').map((s) => Number(s.trim())).filter((n) => Number.isInteger(n))
  : [];
if (FAILED_SHARD_INDICES.length > 0 && !TOTAL_SHARDS_RAW) {
  console.error('--failedShards requires --totalShards N (so the genlog row records the denominator)');
  process.exit(1);
}
const TOTAL_SHARDS = TOTAL_SHARDS_RAW ? Number(TOTAL_SHARDS_RAW) : null;

if (!EVOLVING_REGIONS.includes(REGION)) {
  console.error(`Region "${REGION}" is not in EVOLVING_REGIONS. Valid: ${EVOLVING_REGIONS.join(', ')}`);
  process.exit(1);
}
const OPPONENT_REGIONS = EVOLVING_REGIONS.filter((r) => r !== REGION);

if (!existsSync(OUT_DIR)) mkdirSync(OUT_DIR, { recursive: true });

// ---------------------------------------------------------------------------
// Existing genlog + best-so-far lookup
// ---------------------------------------------------------------------------

/** Read the genlog and return rows tagged with our methodology, sorted by gen ascending. */
function readMethodologyRows() {
  if (!existsSync(GEN_LOG_PATH)) return [];
  const lines = readFileSync(GEN_LOG_PATH, 'utf-8').split(/\r?\n/).filter(Boolean);
  const rows = [];
  for (const l of lines) {
    let row;
    try { row = JSON.parse(l); } catch { continue; }
    if (row.methodology === METHODOLOGY && row.region === REGION) rows.push(row);
  }
  rows.sort((a, b) => a.gen - b.gen);
  return rows;
}

/** Best-so-far = the current champion in the mutation lineage.
 *
 *  Fixed 2026-09-04 (Cald v1 gen 3 exposed the bug): PRIOR implementation picked by highest
 *  raw `focusPoints` across all rows. That's meaningless cross-gen because per-gen field
 *  composition varies (different sibling mutations, different seeds, different anchor scores).
 *  The Cald case: gen 1 Baseline scored 11 pts vs weak-mutation siblings; gen 2 Mut3 scored 10
 *  pts vs baseline + stronger-mutation siblings. Mut3 BEAT its in-gen anchor (10 vs 7.5 pts)
 *  by +2.5 -- real progress -- but max-focusPoints logic kept gen 1 Baseline as anchor
 *  because 11 > 10, ignoring the within-gen improvement signal. Result: gen 3 mutations
 *  spawned from Baseline instead of chaining from Mut3, so evolution effectively restarted
 *  every gen. Same bug affected Core v1's whole 3-gen campaign.
 *
 *  CORRECTED semantics: walk rows chronologically, only update the anchor when the wrapper's
 *  own `bestSoFarUpdated` flag was true for that gen (i.e. a mutation genuinely beat its
 *  in-gen anchor). This matches the plateau-rule semantics fixed the same day: within-gen
 *  comparison is authoritative, cross-gen focusPoints comparison is confounded.
 *
 *  Returns { gen, weights, points, origin } or null if no rows exist yet. `origin` is the
 *  row's `championOrigin` string (e.g. "baseline", "mutation 1", "best-so-far-*") so callers
 *  can tell if the anchor IS baseline and skip a redundant best-so-far slot in the field. */
function bestSoFarFrom(rows) {
  if (rows.length === 0) return null;
  // Walk chronologically. Anchor updates only when the wrapper flagged bestSoFarUpdated=true
  // for that gen. Missing/false = anchor unchanged that gen (championOrigin=baseline case,
  // or no mutation beat the in-gen anchor).
  const sorted = [...rows].sort((a, b) => (a.gen ?? 0) - (b.gen ?? 0));
  let anchor = null;
  for (const r of sorted) {
    if (r.bestSoFarUpdated === true) {
      anchor = r;
    }
  }
  if (anchor === null) return null;
  return {
    gen: anchor.gen,
    weights: anchor.championWeights,
    points: anchor.focusPoints ?? null,
    origin: anchor.championOrigin ?? null,
  };
}

// ---------------------------------------------------------------------------
// Field construction: build the per-gen curated JSONL + extras file for the
// roundrobin.mjs invocation. Field is 6 target-region entrants + 12 opponents = 18.
// ---------------------------------------------------------------------------

/** Look up an opponent region's shipping weights.
 *  Absent from bot-weights.json.regions => baseline. Present => that trained gen's weights. */
function opponentWeightsFor(region, weightsFile) {
  return resolveWeightsForRegion(weightsFile, region);
}

/** Deck-id used by roundrobin's DECK_ID_TO_REGION table -- inferred by lowercasing region names
 *  and mapping the two special cases (d'Resh, Kybar's Teeth) to their deck-id slugs. Only used
 *  when we need to reference an opponent's default deck path here. */
function defaultDeckPathFor(region) {
  const REGION_TO_SLUG = {
    'Arderial': 'default-arderial', 'Bograth': 'default-bograth', 'Cald': 'default-cald',
    'Core': 'default-core', "d'Resh": 'default-dresh', "Kybar's Teeth": 'default-kybars-teeth',
    'Nar': 'default-nar', 'Naroom': 'default-naroom', 'Orothe': 'default-orothe',
    'Paradwyn': 'default-paradwyn', 'Underneath': 'default-underneath',
    'Universal': 'default-universal', 'Weave': 'default-weave',
  };
  const slug = REGION_TO_SLUG[region];
  if (!slug) throw new Error(`No deck slug for region "${region}"`);
  return `app/resources/default-decks/${slug}.json`;
}

/** Build per-gen curated JSONL + extras. Field assembly split cleanly by mechanism:
 *  - Curated JSONL (one file, --jsonl input to roundrobin) contains:
 *      * 5 focus rows (fake region "<REGION>-Focus", gens 0=best-so-far + 1..N=mutations)
 *        -- ignored by auto-loader because the fake region isn't in DECK_ID_TO_REGION, but
 *        findable by the extras loader via `jsonl:<REGION>-Focus:<n>`
 *      * K opponent-retirement rows (REAL region names, gen 0, retirement weights from
 *        bot-weights.json) -- these ARE picked up by roundrobin's auto-loader, which
 *        materializes them as `<Region>-gen0` entrants piloting the region's default deck
 *  - Extras JSON contains:
 *      * 6 target entrants: baseline + best-so-far + 4 mutations (all region=<REGION>, deck=our
 *        experimental deck)
 *      * (12 - K) opponents whose retirement is baseline (no bot-weights entry) -- they need
 *        an extras entry to appear in the field at all
 *
 *  Add --skipBaseline to the roundrobin call so the auto-loader doesn't ALSO add
 *  per-region baseline entrants (which would duplicate what extras adds and inflate the field).
 */
function writeFieldFiles(gen, bestSoFar, mutations, scratchDir, weightsFile) {
  const focusJsonlPath = join(scratchDir, `gen-${gen}-focus-jsonl.jsonl`);
  const extrasPath = join(scratchDir, `gen-${gen}-extras.json`);

  const fakeRegion = `${REGION}-Focus`;
  const jsonlRows = [];

  // If the anchor IS baseline (or there's no anchor yet), skip a dedicated best-so-far slot --
  // baseline is already in the field. Field becomes: baseline + N mutations = N+1 target
  // entrants. Otherwise: baseline + best-so-far + N mutations = N+2. This keeps mutation-lineage
  // anchors distinct from baseline while avoiding a duplicate baseline entrant when best-so-far
  // has landed back on baseline.
  const includeBestSoFarSlot = bestSoFar != null && bestSoFar.origin !== 'baseline';

  // Focus row for best-so-far (only if it's distinct from baseline).
  if (includeBestSoFarSlot) {
    jsonlRows.push({
      gen: 0, region: fakeRegion,
      championWeights: bestSoFar.weights,
      methodology: `${METHODOLOGY}-focus-slot`,
      appendedAt: new Date().toISOString(),
      note: `best-so-far from gen ${bestSoFar.gen} (origin=${bestSoFar.origin})`,
    });
  }
  mutations.forEach((mut, i) => {
    jsonlRows.push({
      gen: i + 1, region: fakeRegion,
      championWeights: mut,
      methodology: `${METHODOLOGY}-focus-slot`,
      appendedAt: new Date().toISOString(),
      note: `mutation ${i + 1} of ${mutations.length} for gen ${gen}`,
    });
  });

  const extras = [];

  // Target: 1 baseline + 1 best-so-far + N mutations (all region=<REGION>, deck=experimental).
  extras.push({
    label: `${REGION}-Baseline`,
    region: REGION,
    deckPath: DECK_PATH_RAW,
    weightsMode: 'baseline',
  });
  if (includeBestSoFarSlot) {
    extras.push({
      label: `${REGION}-BestSoFar-Gen${bestSoFar.gen}`,
      region: REGION,
      deckPath: DECK_PATH_RAW,
      weightsMode: `jsonl:${fakeRegion}:0`,
    });
  }
  mutations.forEach((_, i) => {
    extras.push({
      label: `${REGION}-Gen${gen}-Mut${i + 1}`,
      region: REGION,
      deckPath: DECK_PATH_RAW,
      weightsMode: `jsonl:${fakeRegion}:${i + 1}`,
    });
  });

  // Opponents. One representative per region cloned as a JSONL row under the REAL region name so
  // the auto-loader materializes an entrant piloting the region's default deck. The weight vector
  // comes from `resolveWeightsForRegion` which handles BOTH the single-vector legacy shape and
  // the multi-bot object shape (returns the alphabetically-first bot for multi-bot regions --
  // deterministic + scales as more regions complete their own focused-evo campaigns). Returns
  // baseline for regions absent from bot-weights.json entirely, so the "no entry at all" case is
  // handled inside the resolver rather than needing its own extras fallback here.
  //
  // Precedent for opponent selection during per-gen focused-evo -- see
  // docs/plans/multi-bot-per-region-plan.md: "Per-gen: opponent slot = alphabetically-first bot
  // per region (from bot-weights.json)." Fits compute regardless of how many regions become
  // multi-bot. Full-fidelity "all bots per region" opponent testing is reserved for the final
  // round-robin verification at campaign completion (once, not every gen).
  //
  // 2026-08-24 fix: prior code used `weightsFile.regions[region]` directly, which returned the
  // multi-bot OBJECT for regions like Underneath after the 2026-08-24 multi-bot rollout and
  // would have been passed through as `championWeights` -- an object where a WeightVector was
  // expected. Would have produced nonsense scoring for opponent slots. resolveWeightsForRegion
  // always returns a WeightVector; safe on both shapes.
  for (const region of OPPONENT_REGIONS) {
    if (!weightsFile) {
      // Defensive: if bot-weights.json didn't load at all, opponent falls back to baseline
      // via extras. Doesn't happen in practice (WEIGHTS_PATH is always present + readable) but
      // keeps the field valid in the pathological case.
      extras.push({
        label: `${region}-Baseline`,
        region,
        deckPath: defaultDeckPathFor(region),
        weightsMode: 'baseline',
      });
      continue;
    }
    const opponentWeights = resolveWeightsForRegion(weightsFile, region);
    jsonlRows.push({
      gen: 0, region,
      championWeights: opponentWeights,
      methodology: `${METHODOLOGY}-opponent-slot`,
      appendedAt: new Date().toISOString(),
      note: `opponent representative for ${region} (via resolveWeightsForRegion; alphabetically-first bot for multi-bot regions per docs/plans/multi-bot-per-region-plan.md)`,
    });
    // Auto-loader materializes this entrant as `${region}-gen0` with the region's default deck.
  }

  writeFileSync(focusJsonlPath, jsonlRows.map((r) => JSON.stringify(r)).join('\n') + '\n', 'utf-8');
  writeFileSync(extrasPath, JSON.stringify(extras, null, 2) + '\n', 'utf-8');
  return { focusJsonlPath, extrasPath };
}

// ---------------------------------------------------------------------------
// Roundrobin invocation + leaderboard parsing
// ---------------------------------------------------------------------------

function invokeRoundRobin(gen, focusJsonlPath, extrasPath, genOutDir) {
  if (!existsSync(genOutDir)) mkdirSync(genOutDir, { recursive: true });
  const roundrobinScript = join(here, 'roundrobin.mjs');
  const args = [
    roundrobinScript,
    '--jsonl', focusJsonlPath,
    '--extraEntrantsFile', extrasPath,
    '--gamesPerPair', String(GAMES_PER_PAIR),
    '--workers', String(WORKERS),
    '--maxTurns', String(MAX_TURNS),
    '--gameTimeoutSec', String(GAME_TIMEOUT_SEC),
    '--out', genOutDir,
    '--focusRegion', REGION,
    '--skipBaseline',
  ];
  console.log(`\n[gen ${gen}] Invoking roundrobin.mjs...`);
  console.log(`  ${args.slice(1).join(' ')}`);
  const result = spawnSync(process.execPath, args, { stdio: 'inherit', env: process.env });
  if (result.status !== 0) {
    console.error(`\n[gen ${gen}] roundrobin.mjs exited with code ${result.status}`);
    process.exit(result.status ?? 1);
  }
}

/** Parse the leaderboard txt into an array of { rank, label, seriesW, seriesL, seriesD, points }.
 *  Works for BOTH the non-aggregator writer (roundrobin.mjs main flow) and the aggregator writer
 *  (`--aggregateShards`) -- both emit the same "Rank Label W L D Points Games(W-L-D)" columns. */
function parseLeaderboard(resultsPath) {
  const text = readFileSync(resultsPath, 'utf-8');
  const lines = text.split(/\r?\n/);
  const board = [];
  for (const line of lines) {
    const m = line.match(/^\s*(\d+)\s+(.+?)\s{2,}(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)\s/);
    if (!m) continue;
    board.push({
      rank: Number(m[1]),
      label: m[2].trim(),
      seriesW: Number(m[3]),
      seriesL: Number(m[4]),
      seriesD: Number(m[5]),
      points: Number(m[6]),
    });
  }
  return board;
}

// ---------------------------------------------------------------------------
// Phase helpers -- structured so plan-only, aggregate-only, and full modes all
// go through the same code paths.
// ---------------------------------------------------------------------------

/** PLAN PHASE. Generate mutations, write field files, return the plan object.
 *  Pure -- no roundrobin invocation, no genlog append. Same code called in full and planOnly
 *  modes so the sharded pipeline produces bit-identical field files to the sequential one. */
function runPlan(gen, prevBestSoFar, scratchDir, weightsFile) {
  // Mutate: parent = best-so-far weights (or baseline if no best-so-far yet).
  const parent = prevBestSoFar?.weights ?? { ...BASELINE_WEIGHTS };
  const rng = createSeededRng((BASE_SEED ^ (gen * 2654435761)) >>> 0);
  const mutOpts = MUTATION_SIGMA != null ? { sigmaRelative: Number(MUTATION_SIGMA) } : undefined;
  const mutations = [];
  for (let i = 0; i < POPULATION_SIZE; i += 1) mutations.push(mutateWeights(parent, rng, mutOpts));

  const { focusJsonlPath, extrasPath } = writeFieldFiles(gen, prevBestSoFar, mutations, scratchDir, weightsFile);
  console.log(`[gen ${gen}] Wrote focus JSONL: ${focusJsonlPath}`);
  console.log(`[gen ${gen}] Wrote extras:      ${extrasPath}`);

  return {
    gen,
    region: REGION,
    deckPath: DECK_PATH_RAW,
    methodology: METHODOLOGY,
    populationSize: POPULATION_SIZE,
    baseSeed: BASE_SEED,
    mutationSigma: MUTATION_SIGMA != null ? Number(MUTATION_SIGMA) : null,
    bestSoFar: prevBestSoFar,
    mutations,
    focusJsonlPath,
    extrasPath,
    generatedAt: new Date().toISOString(),
  };
}

/** AGGREGATE PHASE. Read the merged leaderboard from `resultsDir`, run champion selection using
 *  the mutations + bestSoFar from `plan`, append a genlog row. Returns { genRow, newBestSoFar }.
 *
 *  `failedShardIndices` / `totalShards` are only meaningful in sharded mode; when non-empty they
 *  cause a `partialShards` field to appear on the genlog row so downstream analysis can flag
 *  gens that ran on partial data. Full-mode callers pass [] and null respectively. */
function runAggregate(plan, resultsDir, failedShardIndices, totalShards) {
  const { gen, mutations, bestSoFar } = plan;

  const resultsPath = join(resultsDir, 'roundrobin-results.txt');
  if (!existsSync(resultsPath)) {
    console.error(`[gen ${gen}] Expected results file not found: ${resultsPath}`);
    process.exit(1);
  }
  const board = parseLeaderboard(resultsPath);
  const targetLabelPrefix = `${REGION}-`;
  const targetEntries = board.filter((e) => e.label.startsWith(targetLabelPrefix));
  targetEntries.sort((a, b) => b.points - a.points || b.seriesW - a.seriesW || a.rank - b.rank);
  if (targetEntries.length === 0) {
    console.error(`[gen ${gen}] No target-region entries found in leaderboard`);
    process.exit(1);
  }
  const topTarget = targetEntries[0];
  console.log(`\n[gen ${gen}] Target-region leaderboard:`);
  for (const e of targetEntries) console.log(`  #${e.rank}  ${e.label}  ${e.points.toFixed(1)}pts  W${e.seriesW}/L${e.seriesL}/D${e.seriesD}`);

  // Identify the winning weights: map top-target's label back to either baseline, best-so-far,
  // or one of the mutations.
  let winningWeights, winningNote;
  if (topTarget.label.endsWith('-Baseline')) {
    winningWeights = { ...BASELINE_WEIGHTS };
    winningNote = 'baseline';
  } else if (topTarget.label.startsWith(`${REGION}-BestSoFar`)) {
    winningWeights = bestSoFar?.weights ?? { ...BASELINE_WEIGHTS };
    winningNote = `best-so-far${bestSoFar ? `-gen${bestSoFar.gen}` : '-baseline'}`;
  } else {
    // Mutation: label shape "REGION-Gen<gen>-Mut<i>"
    const m = topTarget.label.match(/-Mut(\d+)$/);
    if (!m) { console.error(`[gen ${gen}] Can't decode mutation label "${topTarget.label}"`); process.exit(1); }
    const mutIdx = Number(m[1]) - 1;
    winningWeights = mutations[mutIdx];
    winningNote = `mutation ${mutIdx + 1}`;
  }

  // Decide if best-so-far updates. Any winner (baseline, mutation, or the best-so-far slot
  // itself) can become the new anchor -- baseline is a valid ceiling. Update when the winner
  // has strictly higher points than the previous anchor's score this gen. If the winner IS
  // the previous anchor (it topped the leaderboard again), no update needed -- it already IS
  // the anchor.
  //
  // Two anchor-carrier labels depending on whether we included a best-so-far slot this gen:
  //   - No slot (anchor == baseline): the baseline entrant IS the anchor's carrier
  //   - Slot present (anchor != baseline): the -BestSoFar entrant is the anchor's carrier
  const anchorIsBaseline = bestSoFar == null || bestSoFar.origin === 'baseline';
  const anchorCarrierLabel = anchorIsBaseline ? `${REGION}-Baseline` : `${REGION}-BestSoFar`;
  const anchorEntryThisGen = targetEntries.find((e) => e.label.startsWith(anchorCarrierLabel));
  const anchorPointsThisGen = anchorEntryThisGen?.points ?? null;
  const winnerIsAnchor = anchorEntryThisGen != null && topTarget.label === anchorEntryThisGen.label;
  const cleanlyBeatsAnchor = anchorPointsThisGen != null && topTarget.points > anchorPointsThisGen;
  const shouldUpdateBestSoFar = !winnerIsAnchor && cleanlyBeatsAnchor;

  console.log(`\n[gen ${gen}] Winner: ${topTarget.label} (${winningNote}, ${topTarget.points.toFixed(1)}pts)`);
  console.log(`[gen ${gen}]   Previous anchor was ${bestSoFar ? `gen ${bestSoFar.gen} ${bestSoFar.origin}` : 'BASELINE (no prior)'} -- scored ${anchorPointsThisGen?.toFixed(1) ?? '?'}pts this gen`);
  if (winnerIsAnchor) {
    console.log(`[gen ${gen}]   Anchor HELD (previous anchor topped the leaderboard again)`);
  } else if (shouldUpdateBestSoFar) {
    console.log(`[gen ${gen}]   ANCHOR UPDATED -- new best-so-far is ${winningNote}`);
  } else {
    console.log(`[gen ${gen}]   Anchor HELD (winner did not strictly beat the anchor's score)`);
  }

  // Append to genlog. Always append -- one row per generation, regardless of whether best-so-far
  // updated. Field convention:
  //   championWeights: this gen's WINNER (may equal current best-so-far)
  //   focusPoints:     top-target's points this gen (canonical fitness for best-so-far logic)
  //   bestSoFarUpdated: bool (did the anchor change?)
  //   allTargetPoints: full target-entries leaderboard, for later analysis
  //   partialShards:   OPTIONAL. Only present when the workflow reported failed shards; carries
  //                    { failedShardIndices, totalShards } so downstream analysis can caveat any
  //                    conclusions drawn from partial-data gens. Absent = all shards succeeded
  //                    (or the run wasn't sharded).
  const genRow = {
    gen, region: REGION,
    championWeights: winningWeights,
    focusPoints: topTarget.points,
    focusSeriesW: topTarget.seriesW,
    focusSeriesL: topTarget.seriesL,
    focusSeriesD: topTarget.seriesD,
    championOrigin: winningNote,
    bestSoFarUpdated: shouldUpdateBestSoFar,
    priorBestSoFarGen: bestSoFar?.gen ?? null,
    priorBestSoFarPoints: anchorPointsThisGen,
    allTargetPoints: targetEntries.map((e) => ({ label: e.label, rank: e.rank, points: e.points })),
    deckPath: DECK_PATH_RAW,
    methodology: METHODOLOGY,
    appendedAt: new Date().toISOString(),
  };
  if (failedShardIndices && failedShardIndices.length > 0) {
    genRow.partialShards = {
      failedShardIndices: [...failedShardIndices].sort((a, b) => a - b),
      totalShards,
    };
    console.log(`[gen ${gen}]   PARTIAL-SHARD run: ${failedShardIndices.length}/${totalShards} shards failed (indices: ${genRow.partialShards.failedShardIndices.join(', ')})`);
  }
  appendFileSync(GEN_LOG_PATH, JSON.stringify(genRow) + '\n', 'utf-8');
  console.log(`[gen ${gen}] Appended row to ${GEN_LOG_PATH}`);

  const newBestSoFar = shouldUpdateBestSoFar
    ? { gen, weights: winningWeights, points: topTarget.points, origin: winningNote }
    : bestSoFar;
  return { genRow, newBestSoFar };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

const weightsFile = existsSync(WEIGHTS_PATH) ? loadWeightsFile(WEIGHTS_PATH) : null;
const scratchDir = join(OUT_DIR, 'focused-evo-scratch');
if (!existsSync(scratchDir)) mkdirSync(scratchDir, { recursive: true });

const startingRows = readMethodologyRows();
const startingBest = bestSoFarFrom(startingRows);
const inferredStartGen = startingRows.length > 0 ? startingRows[startingRows.length - 1].gen + 1 : 1;
const START_GEN = START_GENERATION_OVERRIDE ? Number(START_GENERATION_OVERRIDE) : inferredStartGen;

console.log('=== Focused per-region evolution ===');
console.log(`Mode:                  ${MODE}`);
console.log(`Region:                ${REGION}`);
console.log(`Deck:                  ${DECK_PATH_RAW}`);
console.log(`Methodology tag:       ${METHODOLOGY}`);
console.log(`Existing genlog rows:  ${startingRows.length}`);
console.log(`Best-so-far seed:      ${startingBest ? `gen ${startingBest.gen} (${startingBest.points?.toFixed(1) ?? '?'} pts)` : 'BASELINE (first run)'}`);
console.log(`Starting at gen:       ${START_GEN}`);
console.log(`Running:               ${GENERATIONS_TO_RUN} generation(s)`);
console.log(`Population per gen:    ${POPULATION_SIZE} mutations + baseline + best-so-far = ${POPULATION_SIZE + 2} target entrants`);
console.log(`Opponents:             ${OPPONENT_REGIONS.length} (${OPPONENT_REGIONS.join(', ')})`);
console.log(`Games per pair:        ${GAMES_PER_PAIR}`);
console.log(`Workers:               ${WORKERS}`);
console.log(`Mutation sigma:        ${MUTATION_SIGMA ?? 'engine default (0.15)'}`);
console.log('');

if (MODE === 'planOnly') {
  // Plan-only: one gen, write plan JSON + field files, exit. No roundrobin, no genlog append.
  const gen = START_GEN;
  console.log(`\n========== Plan for generation ${gen} ==========`);
  const plan = runPlan(gen, startingBest, scratchDir, weightsFile);
  const planPathResolved = resolve(PLAN_ONLY_PATH);
  const planDir = dirname(planPathResolved);
  if (!existsSync(planDir)) mkdirSync(planDir, { recursive: true });
  writeFileSync(planPathResolved, JSON.stringify(plan, null, 2) + '\n', 'utf-8');
  console.log(`\n[gen ${gen}] Wrote plan JSON: ${planPathResolved}`);
  console.log('=== Plan phase complete ===');
  process.exit(0);
}

if (MODE === 'aggregateOnly') {
  // Aggregate-only: read plan + merged leaderboard, run champion selection, append genlog row.
  const planPathResolved = resolve(AGGREGATE_ONLY_PATH);
  if (!existsSync(planPathResolved)) {
    console.error(`--aggregateOnly plan file not found: ${planPathResolved}`);
    process.exit(1);
  }
  const plan = JSON.parse(readFileSync(planPathResolved, 'utf-8'));
  console.log(`\n========== Aggregate for generation ${plan.gen} ==========`);
  const resultsDirResolved = resolve(RESULTS_DIR);
  runAggregate(plan, resultsDirResolved, FAILED_SHARD_INDICES, TOTAL_SHARDS);
  console.log('\n=== Aggregate phase complete ===');
  process.exit(0);
}

// Full sequential mode -- unchanged behavior. Runs plan -> roundrobin -> aggregate in-process for
// each gen, threading best-so-far forward across the loop iterations.
let bestSoFar = startingBest;
for (let g = 0; g < GENERATIONS_TO_RUN; g += 1) {
  const gen = START_GEN + g;
  console.log(`\n========== Generation ${gen} (${g + 1} of ${GENERATIONS_TO_RUN}) ==========`);

  const plan = runPlan(gen, bestSoFar, scratchDir, weightsFile);

  const genOutDir = join(OUT_DIR, `focused-evo-gen-${gen}`);
  invokeRoundRobin(gen, plan.focusJsonlPath, plan.extrasPath, genOutDir);

  const { newBestSoFar } = runAggregate(plan, genOutDir, [], null);
  bestSoFar = newBestSoFar;
}

console.log('\n=== Focused-evo run complete ===');
console.log(`Final best-so-far: ${bestSoFar ? `gen ${bestSoFar.gen} (${bestSoFar.points?.toFixed(1) ?? '?'} pts)` : 'BASELINE (no mutation beat baseline)'}`);
