// AUTO-SYNCED from private repo. Do not edit here -- edits are overwritten on the next sync.
// Source of record: engine/scripts/evolve-worker.mjs in the private repo.

// Checkpoint 2.5 of the evolutionary-bot-training Track 1 plan
// (docs/plans/evolutionary-bot-training-track1-plan.md, 2026-08-16). One evolve.mjs worker
// process: loads the card DB once at startup, then loops on "receive job -> play one game ->
// send outcome" until the master sends 'shutdown'. Runs one game at a time (deliberately --
// each worker IS one core's worth of work); parallelism comes from having N of these processes
// running simultaneously, dispatched by evolve.mjs's master.
//
// Determinism contract: every game is a pure function of (weights, deck, seed, caps). No state
// carries between jobs within one worker beyond the (pre-loaded, read-only) card DB. Two
// invocations of the same job on any worker return identical outcomes -- so the master's
// job-to-worker assignment is irrelevant to results, only to wall-clock.

import { readFileSync } from 'node:fs';

// Same side-effect import evolve.mjs / fuzz.mjs use -- registers every card's real effect.
// Without this the evaluator scores broken (getCardEffectByName returns undefined for every
// card and no card-specific hooks fire).
import '../engine.bundle.js';
import { buildCardDatabase } from '../engine.bundle.js';
import { playBotVsBotGame } from '../engine.bundle.js';

const CARDS_PATH = process.argv[2];
if (!CARDS_PATH) {
  console.error('evolve-worker: missing cards path argv[2]');
  process.exit(1);
}

const cardData = JSON.parse(readFileSync(CARDS_PATH, 'utf-8'));
const cardDb = buildCardDatabase(cardData);

// Signal readiness -- the master's pool doesn't dispatch jobs to a worker until it's told the
// card DB is loaded (blocking dispatches on unloaded workers would silently serialize the
// startup of a large pool through the first-ready worker).
process.send({ type: 'ready', pid: process.pid });

process.on('message', (msg) => {
  if (msg.type === 'shutdown') {
    process.exit(0);
  }
  if (msg.type !== 'job') {
    console.error(`evolve-worker: unknown message type "${msg.type}"`);
    return;
  }
  // Wrap in try/catch so a single crashing game doesn't kill the whole worker (the master
  // treats a 'threw' outcome as 'stuck' for fitness purposes -- same as a mid-game exception
  // would be counted). Result is now a rich GameResult (Checkpoint 2.6 telemetry, 2026-08-18);
  // exception path fabricates a matching shape so aggregation code can treat both uniformly.
  let result;
  let error = null;
  try {
    result = playBotVsBotGame({
      cardDb,
      deck: msg.deck,
      magiOrder: msg.magiOrder,
      region: msg.region,
      p1Weights: msg.p1Weights,
      p2Weights: msg.p2Weights,
      seed: msg.seed,
      maxTurns: msg.maxTurns,
      maxActions: msg.maxActions,
      timeoutMs: msg.timeoutMs,
    });
  } catch (e) {
    error = e?.message ?? String(e);
    result = {
      outcome: 'stuck',
      turnsPlayed: 0,
      actionsApplied: 0,
      advancePhaseCount: 0,
      declareAttackCount: 0,
      playCardCount: 0,
      usePowerCount: 0,
    };
  }
  process.send({ type: 'result', jobId: msg.jobId, result, error });
});
