// AUTO-SYNCED from private repo. Do not edit here -- edits are overwritten on the next sync.
// Source of record: engine/scripts/swiss-tournament.mjs in the private repo.

// Swiss-format multi-entrant tournament (Checkpoint 4b, 2026-08-19).
//
// Loads every completed generation's champion per region (from evolution-generations.jsonl) plus
// each region's BASELINE_WEIGHTS. Total field ~46 entrants (13 baselines + varying number of
// evolved champions per region -- Underneath has only baseline, Weave/Universal have fewer gens
// than the rest, others have baseline + 3-4 champion gens).
//
// Runs 7 rounds of Swiss:
//   R1-R2:  Bo1 series, everyone paired by starting seed (R1) or current record (R2)
//   R3-R7:  Bo3 series once record differentiation matters
// Standings by cumulative match wins, tie-broken by Buchholz (sum of opponents' win rates).
//
// Answers the specific question: "is our per-region mirror evolution actually producing bots that
// perform better than baseline / earlier generations when they meet cross-region?" A Cald-gen3
// entrant that consistently loses to Cald-baseline in this tournament would tell us evolution
// went the wrong direction; the opposite would validate the whole approach.
//
// Usage from engine/ :
//   npm run build && node scripts/swiss-tournament.mjs [OPTIONS]
//
// Options:
//   --workers N               Parallel workers. Default 8 (bumped from 6 on 2026-08-20 for the
//                             overnight beta-mode validation run).
//   --rounds N                Swiss rounds. Default 7.
//   --noBetaMode              Disable beta mode (OPPONENT_TURN_CROSSINGS=0). Default is ON as of
//                             2026-08-20 -- see tournament-worker.mjs's own env-var block for
//                             the plumbing (parent forks workers with MND_BOT_BETA_MODE=1 in the
//                             env). Add this flag for a beta-OFF comparison run.
//   --postRoundRobin          After the Swiss tournament, run an ADDITIONAL same-region
//                             round-robin: for every region, every within-region pair of entrants
//                             plays --roundRobinGames mirror-match games. Answers the specific
//                             per-region question "does this evolved champion actually beat its
//                             own baseline?" -- the Swiss format answers cross-region ordering,
//                             not this. Report appended to the same swiss-tournament-results.txt.
//   --roundRobinGames N       Games per pair in the post-Swiss round-robin phase. Default 40
//                             (gives clear signal on ~60/40 splits without excessive runtime).
//   --regions "a,b,c"         Regions to include (each contributes 1 baseline + all its
//                             evolved champions). Default: every region present in the
//                             evolution-generations.jsonl or in EVOLVING_REGIONS.
//   --jsonl FILE              Evolution JSONL file to load historical champions from.
//                             Default: engine/evolution-generations.jsonl.
//   --weights FILE            bot-weights.json path (for baseline reference only; historical
//                             champions come from the JSONL). Default engine/data/bot-weights.json.
//   --maxTurns N              Per-game turn cap. Default 200.
//   --gameTimeoutSec N        Per-game wall-clock cap. Default 300. 0 disables.
//   --out DIR                 Output directory. Default engine/.
//   --decks DIR               Default deck directory. Default app/resources/default-decks.
//   --skipBaseline            Skip the baseline entrants (test only evolved champions).
//   --latestGenOnly           Skip historical generations, keep only each region's LATEST
//                             champion + baseline. Smaller field for a quick preview.
//   --shapeCOnly              Filter historical champions to only Shape-C rows (from GH Actions
//                             evolve runs, methodology='GH-Actions-ShapeC-betaOn'). Skips
//                             pre-Shape-C rows so the field doesn't have "Cald-gen1" appearing
//                             twice with different weights. Use for focused validation runs of
//                             the current training methodology.
//   --deckOverride Region=PATH   REPEATABLE. Substitute a non-default deck file for one region
//                                (e.g. `--deckOverride Underneath=engine/decks-experimental/
//                                new-underneath.json`). Path is absolute or relative to the
//                                private repo root. Applies to auto-loaded entrants only --
//                                extras from --extraEntrantsFile use their own explicit
//                                deckPath. Non-destructive -- shipped defaults are untouched.
//   --extraEntrantsFile FILE  Append custom entrants from a JSON file. File format is an
//                             array of { label, region, deckPath, weightsMode? } entries.
//                             deckPath can be absolute or relative to engine/. weightsMode
//                             defaults to 'baseline'; other options: 'jsonl:REGION:GEN' to
//                             load a specific evolved champion by region+generation from the
//                             evolution-generations.jsonl file.
//                             Use case: inject custom deck variants (e.g., a redesigned
//                             Underneath deck) into the tournament without editing the
//                             region auto-load defaults.

import { readFileSync, existsSync, unlinkSync, writeFileSync, appendFileSync, mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve } from 'node:path';
import { fork } from 'node:child_process';

import '../engine.bundle.js';
import { BASELINE_WEIGHTS } from '../engine.bundle.js';
import { EVOLVING_REGIONS } from '../engine.bundle.js';

const here = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(here, '..', '..');

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : process.argv[i + 1];
}

const WORKERS = Number(arg('workers', 8));
const ROUNDS = Number(arg('rounds', 7));
const REGIONS_FILTER = arg('regions', null);
const JSONL_PATH = resolve(arg('jsonl', join(REPO_ROOT, 'engine', 'evolution-generations.jsonl')));
const MAX_TURNS = Number(arg('maxTurns', 200));
const GAME_TIMEOUT_SEC = Number(arg('gameTimeoutSec', 300));
const OUT_DIR = resolve(arg('out', join(here, '..')));
const DECKS_DIR = resolve(arg('decks', join(REPO_ROOT, 'app', 'resources', 'default-decks')));
const SKIP_BASELINE = process.argv.includes('--skipBaseline');
const LATEST_GEN_ONLY = process.argv.includes('--latestGenOnly');
const EXTRA_ENTRANTS_FILE = arg('extraEntrantsFile', null);
// Beta mode ("opponent turn crossings") -- see tournament-worker.mjs's own top-level env-var
// block. Default ON since 2026-08-20 to match the shipped app's intended default (the Play.tsx
// beta-mode toggle is on its way to becoming the only bot search mode); pass `--noBetaMode` to
// invert for a comparison run. Written to a plain string ('1' / '0') because that's the shape
// the worker's env-var check expects.
const BETA_MODE = !process.argv.includes('--noBetaMode');
// Post-Swiss same-region round-robin phase (added 2026-08-20 for the overnight validation run).
// After the Swiss tournament finishes, plays every within-region pair of entrants against each
// other in mirror matches to see if the evolved champions still clearly beat their own
// baselines. Off by default so a plain Swiss invocation is unchanged; enable with
// `--postRoundRobin`. Games per pair configurable via `--roundRobinGames`.
const RUN_ROUND_ROBIN = process.argv.includes('--postRoundRobin');
const ROUND_ROBIN_GAMES = Number(arg('roundRobinGames', 40));

// Shape-C-only filter (2026-08-21). `evolution-generations.jsonl` accumulates rows from every
// era of training. Pre-Shape-C rows (from the old local beta-off / mirror-only runs) don't
// carry a `methodology` field; Shape-C rows are tagged `GH-Actions-ShapeC-betaOn`. With this
// flag set, only Shape-C rows are loaded as historical champions -- keeps the field focused on
// the current training methodology and avoids the "Cald-gen1 appears twice with different
// weights" label collision that happens when both eras are in the field.
const SHAPE_C_ONLY = process.argv.includes('--shapeCOnly');

// Per-region deck overrides (2026-08-21, owner ask -- mirrors evolve-generation.mjs's flag of
// the same name). By default every region uses its shipped default-<region>.json file; the
// override substitutes a different deck for one region for THIS invocation only -- shipped
// defaults are untouched. Used to run the Underneath entrants against the redesigned new deck
// (`engine/decks-experimental/new-underneath.json`) instead of the mirror-pathological default
// so every Underneath variant (baseline + all historical Shape-C champions) is directly
// comparable. Overrides apply only to auto-loaded entrants -- extras from --extraEntrantsFile
// have their own explicit deckPath and are not affected.
const DECK_OVERRIDES = new Map();
{
  for (let i = 0; i < process.argv.length; i += 1) {
    if (process.argv[i] === '--deckOverride' && i + 1 < process.argv.length) {
      const raw = process.argv[i + 1];
      const eq = raw.indexOf('=');
      if (eq < 0) {
        console.error(`Invalid --deckOverride "${raw}": expected Region=path`);
        process.exit(1);
      }
      const region = raw.slice(0, eq).trim();
      const p = raw.slice(eq + 1).trim();
      const absPath = resolve(REPO_ROOT, p);
      if (!existsSync(absPath)) {
        console.error(`--deckOverride "${region}" path does not exist: ${absPath}`);
        process.exit(1);
      }
      DECK_OVERRIDES.set(region, absPath);
    }
  }
}

const CARDS_PATH = join(REPO_ROOT, 'data-pipeline', 'output', 'cards_final.json');
const RESULTS_PATH = join(OUT_DIR, 'swiss-tournament-results.txt');
const ROUNDS_JSONL_PATH = join(OUT_DIR, 'swiss-tournament-rounds.jsonl');
const ERRORS_JSONL_PATH = join(OUT_DIR, 'swiss-tournament-errors.jsonl');
const STOP_FLAG = join(OUT_DIR, 'swiss-tournament-STOP.txt');

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
    // Deck override applies first if set for this region -- see DECK_OVERRIDES above.
    const override = DECK_OVERRIDES.get(region);
    const filePath = override ?? join(DECKS_DIR, `${deckId}.json`);
    if (!existsSync(filePath)) {
      console.error(`Missing deck file: ${filePath}`);
      process.exit(1);
    }
    if (override) {
      console.log(`  [deck-override] ${region} using ${filePath}`);
    }
    byRegion.set(region, expandDeck(JSON.parse(readFileSync(filePath, 'utf-8'))));
  }
  return byRegion;
}

/** Load every generation's champion weights from evolution-generations.jsonl, grouped by
 *  region. Returns Map<region, Array<{gen, championWeights}>>. Later generations later in
 *  the array. If the JSONL doesn't exist, returns an empty map (only baselines will run). */
function loadHistoricalChampions(jsonlPath) {
  const byRegion = new Map();
  if (!existsSync(jsonlPath)) {
    console.warn(`JSONL not found at ${jsonlPath} -- only baseline entrants will run.`);
    return byRegion;
  }
  const lines = readFileSync(jsonlPath, 'utf-8').split('\n').filter((l) => l.trim());
  let skippedNonShapeC = 0;
  for (const line of lines) {
    const row = JSON.parse(line);
    if (SHAPE_C_ONLY && row.methodology !== 'GH-Actions-ShapeC-betaOn') {
      skippedNonShapeC += 1;
      continue;
    }
    const arr = byRegion.get(row.region) ?? [];
    arr.push({ gen: row.gen, championWeights: row.championWeights, championFitness: row.championFitness });
    byRegion.set(row.region, arr);
  }
  for (const arr of byRegion.values()) arr.sort((a, b) => a.gen - b.gen);
  if (SHAPE_C_ONLY && skippedNonShapeC > 0) {
    console.log(`  [shapeCOnly] filtered out ${skippedNonShapeC} historical champion rows without methodology='GH-Actions-ShapeC-betaOn'`);
  }
  return byRegion;
}

/** Loads weights per the file's weightsMode field. 'baseline' -> BASELINE_WEIGHTS;
 *  'jsonl:REGION:GEN' -> that specific champion from evolution-generations.jsonl. Called
 *  by loadExtraEntrants. */
function loadExtraEntrantWeights(weightsMode, historicalChampions) {
  if (!weightsMode || weightsMode === 'baseline') return { ...BASELINE_WEIGHTS };
  const m = weightsMode.match(/^jsonl:(.+):(\d+)$/);
  if (m) {
    const [, region, genStr] = m;
    const gen = Number(genStr);
    const champs = historicalChampions.get(region) ?? [];
    const found = champs.find((c) => c.gen === gen);
    if (!found) {
      console.error(`Extra entrant weightsMode "${weightsMode}" -- no champion found for region "${region}" gen ${gen} in JSONL`);
      process.exit(1);
    }
    return found.championWeights;
  }
  console.error(`Unknown weightsMode "${weightsMode}" in extra entrants file. Use 'baseline' or 'jsonl:REGION:GEN'.`);
  process.exit(1);
}

/** Load custom entrants from a JSON file. Each entry: { label, region, deckPath, weightsMode? }.
 *  deckPath can be absolute or relative to engine/. Appended to the entrants list AFTER the
 *  auto-loaded region baselines + champions. Duplicate labels are not deduped -- caller
 *  responsibility to pick unique labels. */
function loadExtraEntrants(filePath, historicalChampions) {
  if (!filePath) return [];
  const absPath = resolve(filePath);
  if (!existsSync(absPath)) {
    console.error(`--extraEntrantsFile "${absPath}" not found`);
    process.exit(1);
  }
  const raw = JSON.parse(readFileSync(absPath, 'utf-8'));
  if (!Array.isArray(raw)) {
    console.error(`--extraEntrantsFile must be a JSON array; got ${typeof raw}`);
    process.exit(1);
  }
  const extras = [];
  for (const entry of raw) {
    if (!entry.label || !entry.region || !entry.deckPath) {
      console.error(`Extra entrant missing required field (label/region/deckPath): ${JSON.stringify(entry)}`);
      process.exit(1);
    }
    const deckAbsPath = resolve(entry.deckPath);
    if (!existsSync(deckAbsPath)) {
      console.error(`Extra entrant "${entry.label}" deckPath not found: ${deckAbsPath}`);
      process.exit(1);
    }
    const deckJson = JSON.parse(readFileSync(deckAbsPath, 'utf-8'));
    const { deckCardKeys, magiOrder } = expandDeck(deckJson);
    const weights = loadExtraEntrantWeights(entry.weightsMode, historicalChampions);
    extras.push({
      label: entry.label,
      region: entry.region,
      deck: deckCardKeys,
      magiOrder,
      weights,
      gen: -1, // marker: custom entrant (not auto-loaded baseline or JSONL champion)
    });
  }
  return extras;
}

/** Build the entrant list from region deck data + baseline + historical champions.
 *  Each entrant is: { label, region, deck, magiOrder, weights, gen }. `gen === 0` marks
 *  baseline; positive gens are historical champions. Skips baseline if --skipBaseline;
 *  skips non-latest champions if --latestGenOnly. */
function buildEntrants(regionalDecks, historicalChampions, regionsFilter) {
  const entrants = [];
  const regionsToUse = regionsFilter ?? [...regionalDecks.keys()];
  for (const region of regionsToUse) {
    const deck = regionalDecks.get(region);
    if (!deck) {
      console.warn(`Region "${region}" has no deck file, skipping.`);
      continue;
    }
    if (!SKIP_BASELINE) {
      entrants.push({
        label: `${region}-baseline`,
        region, deck: deck.deckCardKeys, magiOrder: deck.magiOrder,
        weights: { ...BASELINE_WEIGHTS }, gen: 0,
      });
    }
    const champions = historicalChampions.get(region) ?? [];
    const gensToUse = LATEST_GEN_ONLY && champions.length > 0 ? [champions[champions.length - 1]] : champions;
    for (const champ of gensToUse) {
      entrants.push({
        label: `${region}-gen${champ.gen}`,
        region, deck: deck.deckCardKeys, magiOrder: deck.magiOrder,
        weights: champ.championWeights, gen: champ.gen,
      });
    }
  }
  return entrants;
}

/** Deterministic seed per game -- includes round, series, and game index so every game in
 *  the tournament has a unique reproducible seed. XOR constant differs from evolve.mjs's
 *  seedFor and tournament.mjs's tournamentSeedFor to keep runs distinguishable. */
function swissSeedFor(round, seriesIdx, gameIdxInSeries) {
  const h = (round * 100000 + seriesIdx * 100 + gameIdxInSeries) >>> 0;
  return (h ^ 0x7e2b9c48) >>> 0;
}

/** Deterministic seed for the post-Swiss round-robin phase. Different XOR from `swissSeedFor`
 *  so the two phases' games can't collide on identical seeds even at pairIdx=0/gameIdx=0. */
function roundRobinSeedFor(pairIdx, gameIdx) {
  const h = (pairIdx * 1000 + gameIdx) >>> 0;
  return (h ^ 0x4bc6fa9d) >>> 0;
}

// ============================================================================
// Worker pool (duplicated from tournament.mjs to keep this script self-contained)
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
        // Pass MND_BOT_BETA_MODE through the env so each worker picks it up at startup and
        // calls `setOpponentTurnCrossingsEnabled(true)` before running any game. Merging with
        // `...process.env` preserves everything the parent inherited (PATH, HOME, etc.); the
        // beta flag is layered on top.
        const workerEnv = { ...process.env, MND_BOT_BETA_MODE: BETA_MODE ? '1' : '0' };
        const w = fork(workerScriptPath, [cardsPath], { stdio: 'inherit', env: workerEnv });
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
    if (msg.type === 'ready') { if (typeof worker._onReady === 'function') worker._onReady(); }
    else if (msg.type === 'result') {
      const p = this.pending.get(msg.jobId);
      if (!p) return;
      this.pending.delete(msg.jobId);
      if (msg.error) {
        const seen = this.errorCounts.get(msg.error);
        if (!seen) { this.errorCounts.set(msg.error, 1); console.error(`  [first occurrence] game threw: ${msg.error}`); }
        else { this.errorCounts.set(msg.error, seen + 1); if ((seen + 1) % 25 === 0) console.error(`  [${seen + 1} occurrences] "${msg.error.slice(0, 80)}${msg.error.length > 80 ? '...' : ''}"`); }
        // Persist to JSONL for post-run diagnosis. Append-only, one row per occurrence, so
        // aggregate counts can be recovered via jq/awk grouping later. Timestamps included
        // to help correlate with tournament round timings if needed.
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
      const stuckResult = { outcome: 'stuck', turnsPlayed: 0, actionsApplied: 0, advancePhaseCount: 0, declareAttackCount: 0, playCardCount: 0, usePowerCount: 0 };
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
    const jobId = this.nextJobId; this.nextJobId += 1;
    return new Promise((resolve, reject) => {
      this.pending.set(jobId, { resolve, reject });
      this.queue.push({ jobId, payload });
      this._drain();
    });
  }
  async ready() { return this.readyPromise; }
  requestStop() { this.stopRequested = true; this._drain(); }
  async shutdown() {
    for (const w of this.workers) { try { w.send({ type: 'shutdown' }); } catch {} }
    await new Promise((r) => setTimeout(r, 200));
    for (const w of this.workers) if (!w.killed) w.kill();
  }
}

// ============================================================================
// Swiss pairing engine
// ============================================================================

/** Given a list of standings (sorted best->worst) plus a set of already-played pairs,
 *  produce pairings for the next round. Uses classic Swiss: pair within same-record
 *  groups where possible, prefer non-rematches, allow rematches only when no
 *  same-record non-rematch alternative exists. Handles odd counts via a bye rotated
 *  to the lowest-standing entrant that hasn't yet had one this run. Returns
 *  { pairings: Array<[entrantIdxA, entrantIdxB]>, byeIdx: number | null }. */
function pairSwissRound(standings, playedPairs, byesReceived) {
  // Group standings by record (wins-losses tuple as key).
  const groups = new Map();
  for (const s of standings) {
    const key = `${s.wins}-${s.losses}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(s);
  }
  const groupKeys = [...groups.keys()]; // insertion order = standings order = best-to-worst

  // If odd, give bye to lowest-standing entrant who hasn't had one yet.
  const totalCount = standings.length;
  let byeIdx = null;
  if (totalCount % 2 === 1) {
    for (let i = standings.length - 1; i >= 0; i -= 1) {
      if (!byesReceived.has(standings[i].idx)) { byeIdx = standings[i].idx; break; }
    }
    if (byeIdx === null) byeIdx = standings[standings.length - 1].idx; // everyone had one -- shouldn't happen at 7 rounds / 46 players
  }

  // Flatten remaining entrants (in standings order, excluding bye).
  const remaining = standings.filter((s) => s.idx !== byeIdx).map((s) => s.idx);

  // Greedy pairing: within same record group, walk top-to-bottom pairing with
  // next-non-rematch. Falls back to next in list if all rematches, then across group
  // boundaries if a group has an odd count. This is O(N^2) worst case which is fine
  // for N=46.
  const pairings = [];
  const paired = new Set();
  for (let i = 0; i < remaining.length; i += 1) {
    const a = remaining[i];
    if (paired.has(a)) continue;
    let matched = null;
    // First pass: same record, non-rematch
    for (let j = i + 1; j < remaining.length; j += 1) {
      const b = remaining[j];
      if (paired.has(b)) continue;
      const aRec = `${standings.find((s) => s.idx === a).wins}-${standings.find((s) => s.idx === a).losses}`;
      const bRec = `${standings.find((s) => s.idx === b).wins}-${standings.find((s) => s.idx === b).losses}`;
      if (aRec !== bRec) break;
      const pairKey = pairKeyOf(a, b);
      if (playedPairs.has(pairKey)) continue;
      matched = b; break;
    }
    // Second pass: any non-rematch
    if (matched === null) {
      for (let j = i + 1; j < remaining.length; j += 1) {
        const b = remaining[j];
        if (paired.has(b)) continue;
        const pairKey = pairKeyOf(a, b);
        if (playedPairs.has(pairKey)) continue;
        matched = b; break;
      }
    }
    // Third pass: rematch (last resort)
    if (matched === null) {
      for (let j = i + 1; j < remaining.length; j += 1) {
        const b = remaining[j];
        if (paired.has(b)) continue;
        matched = b; break;
      }
    }
    if (matched !== null) {
      pairings.push([a, matched]);
      paired.add(a); paired.add(matched);
    }
  }
  return { pairings, byeIdx };
}

function pairKeyOf(a, b) { return a < b ? `${a}|${b}` : `${b}|${a}`; }

// ============================================================================
// Series semantics (Bo1 / Bo3)
// ============================================================================

function pointsForOutcome(outcome, side) {
  if (outcome === 'drawByDefeat' || outcome === 'drawByCap' || outcome === 'stuck') return 0.5;
  const winner = (outcome === 'p1' || outcome === 'p1ByTiebreak') ? 'p1' : 'p2';
  return winner === side ? 1 : 0;
}

/** Play a Bo1 or Bo3 series between two entrants; returns { winnerEntrantIdx | null,
 *  gameResults: Array<GameResult>, aWins, bWins, draws }. p1EntrantIdx plays as p1 in
 *  odd games (1, 3), swapping to p2 in even games (2), to balance going-first advantage. */
async function playSeries(pool, entrantA, entrantB, seriesFormat, round, seriesIdx, perGameCaps) {
  const targetWins = seriesFormat === 'Bo3' ? 2 : 1;
  const maxGames = seriesFormat === 'Bo3' ? 3 : 1;
  const gameResults = [];
  let aWins = 0, bWins = 0, draws = 0;
  for (let g = 0; g < maxGames; g += 1) {
    // Alternate p1/p2 per game to balance going-first bias. Odd game -> A as p1; even game -> B as p1.
    const aIsP1 = g % 2 === 0;
    const p1 = aIsP1 ? entrantA : entrantB;
    const p2 = aIsP1 ? entrantB : entrantA;
    const result = await pool.runJob({
      p1Region: p1.region, p1Deck: p1.deck, p1MagiOrder: p1.magiOrder, p1Weights: p1.weights,
      p2Region: p2.region, p2Deck: p2.deck, p2MagiOrder: p2.magiOrder, p2Weights: p2.weights,
      seed: swissSeedFor(round, seriesIdx, g),
      maxTurns: perGameCaps.maxTurns,
      maxActions: perGameCaps.maxActions,
      timeoutMs: perGameCaps.timeoutMs,
    });
    gameResults.push({ ...result, aWasP1: aIsP1 });
    const aPoints = aIsP1 ? pointsForOutcome(result.outcome, 'p1') : pointsForOutcome(result.outcome, 'p2');
    const bPoints = aIsP1 ? pointsForOutcome(result.outcome, 'p2') : pointsForOutcome(result.outcome, 'p1');
    if (aPoints === 1) aWins += 1;
    else if (bPoints === 1) bWins += 1;
    else draws += 1;
    if (aWins >= targetWins || bWins >= targetWins) break;
  }
  const winner = aWins > bWins ? 'A' : (bWins > aWins ? 'B' : null);
  return { winner, gameResults, aWins, bWins, draws };
}

/** Adaptive Bo1 vs Bo3 by round: rounds 1-2 are Bo1 (cheap, wide exploration), rounds
 *  3-7 are Bo3 (precision when record differentiation matters). */
function seriesFormatFor(round) {
  return round <= 2 ? 'Bo1' : 'Bo3';
}

// ============================================================================
// Standings + Buchholz
// ============================================================================

/** Buchholz score = sum of opponents' current win rates. Rewards beating strong opponents;
 *  standard Swiss tiebreaker. Byes count as a win but don't contribute an opponent. */
function computeBuchholz(entrants, matchLog, entrantIdx) {
  const opponents = new Set();
  for (const m of matchLog) {
    if (m.aIdx === entrantIdx) opponents.add(m.bIdx);
    else if (m.bIdx === entrantIdx) opponents.add(m.aIdx);
  }
  if (opponents.size === 0) return 0;
  let sum = 0;
  for (const oppIdx of opponents) {
    const opp = entrants[oppIdx];
    const oppGames = opp.wins + opp.losses + opp.draws;
    const oppWinRate = oppGames > 0 ? (opp.wins + opp.draws * 0.5) / oppGames : 0.5;
    sum += oppWinRate;
  }
  return sum;
}

function currentStandings(entrants) {
  return entrants.map((e, idx) => ({ idx, label: e.label, region: e.region, gen: e.gen, wins: e.wins, losses: e.losses, draws: e.draws }))
    .sort((a, b) => b.wins - a.wins || a.losses - b.losses || a.label.localeCompare(b.label));
}

// ============================================================================
// Reporting
// ============================================================================

function initJsonl(path) {
  const dir = dirname(path);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  writeFileSync(path, '', 'utf-8');
}

function appendRoundRow(path, roundNum, seriesFormat, match) {
  appendFileSync(path, JSON.stringify({ round: roundNum, seriesFormat, ...match }) + '\n', 'utf-8');
}

function writeResults(entrants, matchLog, startedAt, elapsedMs, totalGames, roundRobinReport) {
  const lines = [];
  lines.push('=== Swiss-format multi-entrant cross-region tournament ===');
  lines.push(`Started: ${new Date(startedAt).toISOString()}`);
  lines.push(`Elapsed: ${(elapsedMs / 60000).toFixed(1)} minutes`);
  lines.push(`Total games: ${totalGames}`);
  lines.push(`Total match series: ${matchLog.length}`);
  lines.push(`Entrants: ${entrants.length} (across ${new Set(entrants.map((e) => e.region)).size} regions)`);
  lines.push(`Rounds: ${ROUNDS} (R1-R2 Bo1, R3+ Bo3)`);
  lines.push(`Beta mode (OPPONENT_TURN_CROSSINGS=1): ${BETA_MODE ? 'ON' : 'OFF'}`);
  lines.push('');

  // ---- Full leaderboard ----
  const sorted = entrants.map((e, idx) => ({ idx, ...e, buchholz: computeBuchholz(entrants, matchLog, idx) }))
    .sort((a, b) => b.wins - a.wins || b.buchholz - a.buchholz || a.losses - b.losses || a.label.localeCompare(b.label));
  lines.push('--- Full leaderboard ---');
  lines.push('Rank  Label                            W    L    D    Points   Buchholz');
  sorted.forEach((e, i) => {
    const pts = (e.wins + e.draws * 0.5).toFixed(1);
    lines.push(`${String(i + 1).padStart(2)}    ${e.label.padEnd(30)} ${String(e.wins).padStart(4)} ${String(e.losses).padStart(4)} ${String(e.draws).padStart(4)}   ${pts.padStart(6)}   ${e.buchholz.toFixed(2).padStart(7)}`);
  });
  lines.push('');

  // ---- Per-region rollup: which variant of each region ranked highest? ----
  lines.push('--- Per-region rollup (which variant ranked highest per region) ---');
  const byRegion = new Map();
  for (let i = 0; i < sorted.length; i += 1) {
    const e = sorted[i];
    if (!byRegion.has(e.region)) byRegion.set(e.region, []);
    byRegion.get(e.region).push({ ...e, rank: i + 1 });
  }
  const regionRollup = [...byRegion.entries()].sort((a, b) => a[1][0].rank - b[1][0].rank);
  lines.push('Region              Best variant                  Best rank   All variants (by rank)');
  for (const [region, variants] of regionRollup) {
    const best = variants[0];
    const allText = variants.map((v) => `${v.label.replace(region + '-', '')}@#${v.rank}(${v.wins}W)`).join(', ');
    lines.push(`${region.padEnd(18)}  ${best.label.padEnd(28)}  ${String(best.rank).padStart(6)}     ${allText}`);
  }
  lines.push('');
  lines.push('The Best-variant column answers the core question per region: is baseline, an early gen, or the latest gen actually the strongest?');
  lines.push('');

  // ---- Post-Swiss round-robin phase (optional; only present if --postRoundRobin was used) ----
  if (roundRobinReport) {
    lines.push('=== Post-Swiss same-region round-robin ===');
    lines.push(`Games per pair: ${roundRobinReport.gamesPerPair}`);
    lines.push(`Total pairs: ${roundRobinReport.totalPairs}`);
    lines.push(`Total games: ${roundRobinReport.totalGames}`);
    lines.push(`Elapsed: ${(roundRobinReport.elapsedMs / 60000).toFixed(1)} minutes`);
    lines.push('');
    lines.push('Each region below: entrants ranked by round-robin points (win = 1, draw = 0.5).');
    lines.push('Head-to-head shows each pair separately -- read as "row entrant vs column entrant."');
    lines.push('');
    for (const region of roundRobinReport.regions) {
      lines.push(`--- ${region.regionName} (${region.entrants.length} entrants) ---`);
      lines.push('Rank  Label                            RR-W   RR-L   RR-D   Points');
      region.ranking.forEach((r, i) => {
        const pts = (r.wins + r.draws * 0.5).toFixed(1);
        lines.push(`${String(i + 1).padStart(2)}    ${r.label.padEnd(30)}${String(r.wins).padStart(5)}  ${String(r.losses).padStart(5)}  ${String(r.draws).padStart(5)}  ${pts.padStart(6)}`);
      });
      if (region.headToHead.length > 0) {
        lines.push('  Head-to-head:');
        for (const h2h of region.headToHead) {
          lines.push(`    ${h2h.aLabel.padEnd(28)} vs ${h2h.bLabel.padEnd(28)}: ${String(h2h.aWins).padStart(3)}-${String(h2h.bWins).padStart(3)}${h2h.draws > 0 ? `-${h2h.draws}` : ''}  (${h2h.winner === 'A' ? h2h.aLabel : h2h.winner === 'B' ? h2h.bLabel : 'TIE'})`);
        }
      }
      lines.push('');
    }
    lines.push('Round-robin interpretation: for shipping a per-region champion, look for a clear');
    lines.push('winner (>= ~60% of possible points) over baseline in the same-region column above.');
    lines.push('A near-tie or reversal means the evolved variant is not clearly better than baseline');
    lines.push('under the deeper (beta-ON) search structure and probably should not ship yet.');
    lines.push('');
  }

  const dir = dirname(RESULTS_PATH);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  writeFileSync(RESULTS_PATH, lines.join('\n'), 'utf-8');
}

// ============================================================================
// Post-Swiss same-region round-robin phase
// ============================================================================

/** Runs the post-Swiss same-region round-robin: for every region with 2+ entrants, plays every
 *  pair (C(N,2)) as a mirror match (same deck on both sides, different weight vectors), K games
 *  per pair, alternating p1/p2 to balance going-first bias. Returns a structured report the
 *  writeResults pass appends to the main results file. Reuses the WorkerPool already spun up
 *  for the Swiss tournament -- no fresh workers, no re-loading of card DB. */
async function runPostSwissRoundRobin(pool, entrants, gamesPerPair, perGameCaps) {
  const phaseStart = Date.now();
  // Group entrants by region.
  const byRegion = new Map();
  for (let idx = 0; idx < entrants.length; idx += 1) {
    const e = entrants[idx];
    if (!byRegion.has(e.region)) byRegion.set(e.region, []);
    byRegion.get(e.region).push({ idx, entrant: e });
  }
  // Build the pair list (across all regions, one flat list -- one WorkerPool dispatch batch).
  const allPairs = [];
  const regionsWithPairs = [];
  for (const [regionName, members] of byRegion.entries()) {
    if (members.length < 2) {
      console.log(`  round-robin: region "${regionName}" has only ${members.length} entrant(s), skipping.`);
      continue;
    }
    const regionPairs = [];
    for (let i = 0; i < members.length; i += 1) {
      for (let j = i + 1; j < members.length; j += 1) {
        const pair = { aIdx: members[i].idx, bIdx: members[j].idx, region: regionName };
        regionPairs.push(pair);
        allPairs.push(pair);
      }
    }
    regionsWithPairs.push({ regionName, members, pairs: regionPairs });
  }
  const totalPairs = allPairs.length;
  const totalPlannedGames = totalPairs * gamesPerPair;
  console.log(`Post-Swiss round-robin: ${totalPairs} pairs across ${regionsWithPairs.length} regions, ${gamesPerPair} games/pair -> ${totalPlannedGames} games total.`);
  console.log('');

  // Per-entrant round-robin standings (separate from Swiss wins/losses).
  const rrStandings = new Map();
  for (const e of entrants) rrStandings.set(e.label, { wins: 0, losses: 0, draws: 0 });
  const headToHead = []; // Array<{ region, aLabel, bLabel, aWins, bWins, draws, winner }>

  // Progress logging shared across all pairs (same shape as Swiss round progress).
  let completedGames = 0;
  let lastProgressLog = Date.now();
  const LOG_INTERVAL_MS = 60_000;
  const LOG_EVERY_N_GAMES = Math.max(20, Math.floor(totalPlannedGames / 20));
  let lastProgressCount = 0;
  pool._onJobCompleteHook = () => {
    completedGames += 1;
    const now = Date.now();
    if ((completedGames - lastProgressCount) >= LOG_EVERY_N_GAMES || (now - lastProgressLog) >= LOG_INTERVAL_MS) {
      lastProgressCount = completedGames;
      lastProgressLog = now;
      const elapsedS = ((now - phaseStart) / 1000).toFixed(0);
      const pct = (completedGames / totalPlannedGames * 100).toFixed(0);
      const eta = completedGames > 0
        ? (((now - phaseStart) / completedGames) * (totalPlannedGames - completedGames) / 1000).toFixed(0)
        : '?';
      process.stdout.write(`  round-robin progress: ${completedGames}/${totalPlannedGames} games (~${pct}%), ${elapsedS}s elapsed, ~${eta}s remaining\n`);
    }
  };

  // Dispatch all games as one flat batch to the pool. Each pair's K games run in parallel with
  // games from other pairs. Alternate p1/p2 per game (odd game -> A as p1, even -> B as p1) to
  // balance going-first advantage, same as the Swiss series logic.
  const pairResultPromises = allPairs.map(async (pair, pairIdx) => {
    const entrantA = entrants[pair.aIdx];
    const entrantB = entrants[pair.bIdx];
    const gamePromises = [];
    for (let g = 0; g < gamesPerPair; g += 1) {
      const aIsP1 = g % 2 === 0;
      const p1Entrant = aIsP1 ? entrantA : entrantB;
      const p2Entrant = aIsP1 ? entrantB : entrantA;
      const jobPayload = {
        seed: roundRobinSeedFor(pairIdx, g),
        p1Deck: p1Entrant.deck,
        p1MagiOrder: p1Entrant.magiOrder,
        p1Region: p1Entrant.region,
        p1Weights: p1Entrant.weights,
        p2Deck: p2Entrant.deck,
        p2MagiOrder: p2Entrant.magiOrder,
        p2Region: p2Entrant.region,
        p2Weights: p2Entrant.weights,
        maxTurns: perGameCaps.maxTurns,
        maxActions: perGameCaps.maxActions,
        timeoutMs: perGameCaps.timeoutMs,
      };
      gamePromises.push(pool.runJob(jobPayload).then((result) => ({ result, aIsP1 })));
    }
    const gameResults = await Promise.all(gamePromises);
    // Tally head-to-head.
    let aWins = 0, bWins = 0, draws = 0;
    for (const { result, aIsP1 } of gameResults) {
      const aPoints = aIsP1 ? pointsForOutcome(result.outcome, 'p1') : pointsForOutcome(result.outcome, 'p2');
      const bPoints = aIsP1 ? pointsForOutcome(result.outcome, 'p2') : pointsForOutcome(result.outcome, 'p1');
      if (aPoints === 1) aWins += 1;
      else if (bPoints === 1) bWins += 1;
      else draws += 1;
    }
    return { pair, aWins, bWins, draws };
  });
  const pairResults = await Promise.all(pairResultPromises);
  pool._onJobCompleteHook = null;

  // Aggregate: per-entrant round-robin standings, per-region head-to-head list.
  for (const { pair, aWins, bWins, draws } of pairResults) {
    const aLabel = entrants[pair.aIdx].label;
    const bLabel = entrants[pair.bIdx].label;
    rrStandings.get(aLabel).wins += aWins;
    rrStandings.get(aLabel).losses += bWins;
    rrStandings.get(aLabel).draws += draws;
    rrStandings.get(bLabel).wins += bWins;
    rrStandings.get(bLabel).losses += aWins;
    rrStandings.get(bLabel).draws += draws;
    const winner = aWins > bWins ? 'A' : (bWins > aWins ? 'B' : null);
    headToHead.push({ region: pair.region, aLabel, bLabel, aWins, bWins, draws, winner });
  }

  // Per-region report structure: entrants ranked by points, plus that region's head-to-head rows.
  const regions = regionsWithPairs.map(({ regionName, members, pairs }) => {
    const ranking = members
      .map(({ entrant }) => ({
        label: entrant.label,
        wins: rrStandings.get(entrant.label).wins,
        losses: rrStandings.get(entrant.label).losses,
        draws: rrStandings.get(entrant.label).draws,
      }))
      .sort((a, b) => {
        const aPts = a.wins + a.draws * 0.5;
        const bPts = b.wins + b.draws * 0.5;
        if (bPts !== aPts) return bPts - aPts;
        return a.label.localeCompare(b.label);
      });
    const regionH2H = headToHead.filter((h) => h.region === regionName);
    return { regionName, entrants: members, ranking, headToHead: regionH2H };
  });

  return {
    gamesPerPair,
    totalPairs,
    totalGames: totalPlannedGames,
    elapsedMs: Date.now() - phaseStart,
    regions,
  };
}

// ============================================================================
// Main
// ============================================================================

const regionalDecks = loadRegionalDecks();
const historicalChampions = loadHistoricalChampions(JSONL_PATH);
const regionsToUse = REGIONS_FILTER ? REGIONS_FILTER.split(',').map((s) => s.trim()) : EVOLVING_REGIONS;
const autoEntrants = buildEntrants(regionalDecks, historicalChampions, regionsToUse);
const extraEntrants = loadExtraEntrants(EXTRA_ENTRANTS_FILE, historicalChampions);
const entrants = [...autoEntrants, ...extraEntrants];
if (entrants.length < 2) {
  console.error(`Too few entrants (${entrants.length}). Nothing to run.`);
  process.exit(1);
}
if (extraEntrants.length > 0) {
  console.log(`Loaded ${extraEntrants.length} extra entrant(s) from ${EXTRA_ENTRANTS_FILE}:`);
  for (const e of extraEntrants) console.log(`  ${e.label} (region: ${e.region})`);
  console.log('');
}

// Initialize per-entrant standings state.
for (const e of entrants) { e.wins = 0; e.losses = 0; e.draws = 0; }

console.log('=== Swiss-format multi-entrant tournament ===');
console.log(`Workers:              ${WORKERS}`);
console.log(`Rounds:               ${ROUNDS} (R1-R2 Bo1, R3+ Bo3)`);
console.log(`Entrants:             ${entrants.length}`);
console.log(`Regions represented:  ${new Set(entrants.map((e) => e.region)).size}`);
console.log(`Max turns / game:     ${MAX_TURNS}`);
console.log(`Game timeout:         ${GAME_TIMEOUT_SEC > 0 ? `${GAME_TIMEOUT_SEC}s` : 'disabled'}`);
console.log(`Beta mode:            ${BETA_MODE ? 'ON (OPPONENT_TURN_CROSSINGS=1)' : 'OFF'}`);
console.log(`Post-Swiss round-robin: ${RUN_ROUND_ROBIN ? `ON (${ROUND_ROBIN_GAMES} games/pair)` : 'OFF'}`);
console.log(`Output dir:           ${OUT_DIR}`);
console.log('');
console.log('Entrant list:');
for (const e of entrants) console.log(`  ${e.label}`);
console.log('');

if (existsSync(STOP_FLAG)) unlinkSync(STOP_FLAG);
initJsonl(ROUNDS_JSONL_PATH);
initJsonl(ERRORS_JSONL_PATH);

const startedAt = Date.now();
const workerScriptPath = join(here, 'tournament-worker.mjs');
const pool = new WorkerPool(WORKERS, workerScriptPath, CARDS_PATH);
console.log(`Spinning up ${WORKERS} worker(s), waiting for card DB load...`);
await pool.ready();
console.log('Workers ready.\n');

let stopped = false;
process.on('SIGINT', () => { console.log('\nSIGINT -- finishing current round, then stopping.'); stopped = true; });
const stopPoller = setInterval(() => {
  if (existsSync(STOP_FLAG)) { unlinkSync(STOP_FLAG); console.log('\nSTOP flag -- finishing current round.'); stopped = true; }
}, 5000);

const playedPairs = new Set(); // pair keys already played
const byesReceived = new Set(); // entrant idx -> bye received
const matchLog = []; // Array<{ round, aIdx, bIdx, seriesFormat, aWins, bWins, draws, aWonSeries }>
let totalGames = 0;
const perGameCaps = { maxTurns: MAX_TURNS, maxActions: 20000, timeoutMs: GAME_TIMEOUT_SEC > 0 ? GAME_TIMEOUT_SEC * 1000 : undefined };

for (let round = 1; round <= ROUNDS; round += 1) {
  if (stopped) break;
  const roundStartMs = Date.now();
  const seriesFormat = seriesFormatFor(round);
  const standings = currentStandings(entrants);
  const { pairings, byeIdx } = pairSwissRound(standings, playedPairs, byesReceived);
  if (byeIdx !== null) {
    // Bye = auto-win (1 match, 0 games)
    entrants[byeIdx].wins += 1;
    byesReceived.add(byeIdx);
  }
  console.log(`--- Round ${round} (${seriesFormat}): ${pairings.length} matches${byeIdx !== null ? ` + 1 bye (${entrants[byeIdx].label})` : ''} ---`);

  // Mid-round progress hook. Bo1 rounds finish fast so a per-series line is fine; Bo3 rounds
  // can take 20-30 minutes with 20+ series, so periodic game-count updates keep the operator
  // informed. Same mechanism the evolution runner uses for mid-region progress.
  const expectedGames = pairings.length * (seriesFormat === 'Bo3' ? 3 : 1); // upper bound (Bo3 can end at 2)
  let completedGames = 0;
  let lastProgressLog = Date.now();
  const LOG_INTERVAL_MS = 60_000; // every 60s
  const LOG_EVERY_N_GAMES = Math.max(10, Math.floor(expectedGames / 15));
  let lastProgressCount = 0;
  pool._onJobCompleteHook = () => {
    completedGames += 1;
    const now = Date.now();
    if ((completedGames - lastProgressCount) >= LOG_EVERY_N_GAMES || (now - lastProgressLog) >= LOG_INTERVAL_MS) {
      lastProgressCount = completedGames;
      lastProgressLog = now;
      const elapsedS = ((now - roundStartMs) / 1000).toFixed(0);
      const pct = (completedGames / expectedGames * 100).toFixed(0);
      const eta = completedGames > 0
        ? (((now - roundStartMs) / completedGames) * (expectedGames - completedGames) / 1000).toFixed(0)
        : '?';
      process.stdout.write(`  round ${round} progress: ${completedGames}/${expectedGames} games (~${pct}%), ${elapsedS}s elapsed, ~${eta}s remaining\n`);
    }
  };

  // Play all pairings in parallel via the pool.
  const seriesPromises = pairings.map(([aIdx, bIdx], seriesIdx) => {
    return playSeries(pool, entrants[aIdx], entrants[bIdx], seriesFormat, round, seriesIdx, perGameCaps)
      .then((series) => ({ aIdx, bIdx, seriesIdx, series }));
  });
  const seriesResults = await Promise.all(seriesPromises);
  pool._onJobCompleteHook = null;

  for (const { aIdx, bIdx, series } of seriesResults) {
    const { winner, aWins, bWins, draws, gameResults } = series;
    // Update standings
    if (winner === 'A') { entrants[aIdx].wins += 1; entrants[bIdx].losses += 1; }
    else if (winner === 'B') { entrants[bIdx].wins += 1; entrants[aIdx].losses += 1; }
    else { entrants[aIdx].draws += 1; entrants[bIdx].draws += 1; }
    // Mark as played
    playedPairs.add(pairKeyOf(aIdx, bIdx));
    // Log
    matchLog.push({ round, aIdx, bIdx, seriesFormat, aWins, bWins, draws, aWonSeries: winner === 'A' });
    appendRoundRow(ROUNDS_JSONL_PATH, round, seriesFormat, {
      aLabel: entrants[aIdx].label, bLabel: entrants[bIdx].label,
      aWins, bWins, draws, winner,
      gameOutcomes: gameResults.map((g) => ({ outcome: g.outcome, turns: g.turnsPlayed, aWasP1: g.aWasP1 })),
    });
    totalGames += gameResults.length;
    console.log(`  ${entrants[aIdx].label} vs ${entrants[bIdx].label}: ${aWins}-${bWins}${draws > 0 ? `-${draws}` : ''}  (${winner === 'A' ? entrants[aIdx].label : winner === 'B' ? entrants[bIdx].label : 'DRAW'} wins series)`);
  }

  const roundElapsed = ((Date.now() - roundStartMs) / 1000).toFixed(0);
  console.log(`Round ${round} complete: ${seriesResults.length} series, ~${seriesResults.reduce((s, r) => s + r.series.gameResults.length, 0)} games, ${roundElapsed}s`);
  console.log('');

  // Write partial results after each round so a Ctrl-C mid-tournament still leaves something.
  writeResults(entrants, matchLog, startedAt, Date.now() - startedAt, totalGames, null);
}

const swissElapsedMs = Date.now() - startedAt;
console.log('');
console.log(`=== Swiss phase done. ${matchLog.length} series, ${totalGames} games, ${(swissElapsedMs / 60000).toFixed(1)} min. ===`);

// Post-Swiss round-robin phase (optional, --postRoundRobin). Skipped if the run stopped early.
let roundRobinReport = null;
if (RUN_ROUND_ROBIN && !stopped) {
  console.log('');
  console.log('Starting post-Swiss same-region round-robin phase...');
  console.log('');
  try {
    roundRobinReport = await runPostSwissRoundRobin(pool, entrants, ROUND_ROBIN_GAMES, perGameCaps);
    console.log('');
    console.log(`=== Round-robin phase done. ${roundRobinReport.totalGames} games across ${roundRobinReport.totalPairs} pairs, ${(roundRobinReport.elapsedMs / 60000).toFixed(1)} min. ===`);
  } catch (err) {
    console.error('Round-robin phase failed:', err);
  }
} else if (RUN_ROUND_ROBIN && stopped) {
  console.log('Round-robin phase SKIPPED (Swiss stopped early).');
}

// Final write: Swiss results + optional round-robin report, both in the same file.
writeResults(entrants, matchLog, startedAt, Date.now() - startedAt, totalGames, roundRobinReport);

const elapsedMs = Date.now() - startedAt;
console.log('');
console.log(`=== All phases done. Total elapsed: ${(elapsedMs / 60000).toFixed(1)} min. ===`);
console.log(`Leaderboard + rollup${roundRobinReport ? ' + round-robin' : ''}: ${RESULTS_PATH}`);
console.log(`Per-match JSONL:      ${ROUNDS_JSONL_PATH}`);

clearInterval(stopPoller);
await pool.shutdown();
