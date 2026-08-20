// AUTO-SYNCED from private repo. Do not edit here -- edits are overwritten on the next sync.
// Source of record: engine/scripts/tournament-worker.mjs in the private repo.

// Cross-region tournament worker (Checkpoint 4, 2026-08-19). Companion to evolve-worker.mjs
// but for CROSS-region play: each side has its OWN deck, magi order, region, and weight vector.
// evolve-worker.mjs's playBotVsBotGame requires mirror decks (same deck on both seats), so we
// can't reuse it -- this worker inlines the game loop with per-player setup instead. Everything
// else (rng streams, tiebreak, counting record for telemetry, timeout handling) matches
// fitness.ts's own playBotVsBotGame exactly, so results are directly comparable to evolution runs.
//
// Determinism: same job payload -> same GameResult on any worker on any machine. Two RNG
// streams (engine + policy) derived from the seed, exactly mirroring playRandomGame / the
// evolution runner. Instance counter reset per game.

import { readFileSync } from 'node:fs';

import '../engine.bundle.js';
import { buildCardDatabase } from '../engine.bundle.js';
import { setupGame, rollForTurnOrder } from '../engine.bundle.js';
import { takeBotAction, setOpponentTurnCrossingsEnabled } from '../engine.bundle.js';
import { createSeededRng } from '../engine.bundle.js';
import { resetInstanceCounter } from '../engine.bundle.js';
import { resolveTiebreakByMagiCount } from '../engine.bundle.js';

const CARDS_PATH = process.argv[2];
if (!CARDS_PATH) {
  console.error('tournament-worker: missing cards path argv[2]');
  process.exit(1);
}

// Beta mode ("opponent turn crossings") -- enabled per-worker via env var passed by the parent
// (swiss-tournament.mjs / tournament.mjs). When ON, the bot's lookahead crosses ONE turn
// boundary to model the opponent's likely response, matching the shipping app's own
// `betaModeEnabled` toggle in Play.tsx. All evolution + tournament runs prior to 2026-08-20 used
// the default OFF; the 2026-08-20 overnight Swiss re-run turns it ON to validate that the
// evolved champion weights hold up against the deeper search structure the shipped app actually
// uses. The env var is process-scoped, so setting once at worker startup (before any game runs)
// covers every job the worker handles.
if (process.env.MND_BOT_BETA_MODE === '1') {
  setOpponentTurnCrossingsEnabled(true);
}

const cardData = JSON.parse(readFileSync(CARDS_PATH, 'utf-8'));
const cardDb = buildCardDatabase(cardData);

process.send({ type: 'ready', pid: process.pid });

/** The cross-region equivalent of fitness.ts's playBotVsBotGame. Difference: p1 and p2 each
 *  bring their OWN deck, magi order, region tag, and weight vector. Everything else is the
 *  same: two rng streams, counting record, tiebreak on any non-decisive exit, GameResult
 *  shape matches fitness.ts's export so downstream consumers (report/aggregation) can use
 *  either interchangeably. */
function playCrossRegionGame(job) {
  resetInstanceCounter();
  const rng = createSeededRng(job.seed);
  const policyRng = createSeededRng((job.seed ^ 0x9e3779b9) >>> 0);

  const turnOrder = rollForTurnOrder(['p1', 'p2'], rng);
  const state = setupGame(
    [
      { playerId: 'p1', deckCardKeys: [...job.p1Deck], magiOrder: [...job.p1MagiOrder], botRegion: job.p1Region },
      { playerId: 'p2', deckCardKeys: [...job.p2Deck], magiOrder: [...job.p2MagiOrder], botRegion: job.p2Region },
    ],
    cardDb,
    { turnOrder, rng }
  );

  let advancePhaseCount = 0, declareAttackCount = 0, playCardCount = 0, usePowerCount = 0;
  const countingRecord = (fn) => {
    if (fn === 'advancePhase') advancePhaseCount += 1;
    else if (fn === 'declareAttack') declareAttackCount += 1;
    else if (fn === 'playCard') playCardCount += 1;
    else if (fn === 'usePower') usePowerCount += 1;
  };

  const resolvedTutorFor = new Set();
  const maxTurns = job.maxTurns ?? 200;
  const maxActions = job.maxActions ?? 20000;
  const timeoutMs = job.timeoutMs;
  const deadline = timeoutMs !== undefined ? Date.now() + timeoutMs : Infinity;
  let actionsApplied = 0;

  const buildResult = (outcome) => ({
    outcome,
    turnsPlayed: state.turnNumber,
    actionsApplied,
    advancePhaseCount,
    declareAttackCount,
    playCardCount,
    usePowerCount,
  });
  const buildResultWithTiebreak = (defaultOutcome) => {
    const tb = resolveTiebreakByMagiCount(state);
    if (tb === 'p1') return buildResult('p1ByTiebreak');
    if (tb === 'p2') return buildResult('p2ByTiebreak');
    return buildResult(defaultOutcome);
  };

  while (!state.winnerId && !state.isDraw) {
    if (state.turnNumber > maxTurns) return buildResultWithTiebreak('drawByCap');
    if (actionsApplied >= maxActions) return buildResultWithTiebreak('drawByCap');
    if (Date.now() > deadline) return buildResultWithTiebreak('stuck');
    const acted =
      takeBotAction(state, cardDb, 'p1', rng, policyRng, countingRecord, resolvedTutorFor, job.p1Weights) ||
      takeBotAction(state, cardDb, 'p2', rng, policyRng, countingRecord, resolvedTutorFor, job.p2Weights);
    if (!acted) return buildResultWithTiebreak('stuck');
    actionsApplied += 1;
  }
  if (state.isDraw) return buildResultWithTiebreak('drawByDefeat');
  return buildResult(state.winnerId === 'p1' ? 'p1' : 'p2');
}

process.on('message', (msg) => {
  if (msg.type === 'shutdown') process.exit(0);
  if (msg.type !== 'job') {
    console.error(`tournament-worker: unknown message type "${msg.type}"`);
    return;
  }
  let result;
  let error = null;
  try {
    result = playCrossRegionGame(msg);
  } catch (e) {
    error = e?.message ?? String(e);
    result = {
      outcome: 'stuck', turnsPlayed: 0, actionsApplied: 0,
      advancePhaseCount: 0, declareAttackCount: 0, playCardCount: 0, usePowerCount: 0,
    };
  }
  process.send({ type: 'result', jobId: msg.jobId, result, error });
});
