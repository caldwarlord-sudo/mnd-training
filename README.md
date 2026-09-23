# mnd-training

Bot-training harness for **Magi-Nation Duel**, a fan-project two-player card game.

This repo is the public compute half of a split project. The **game engine and the app**
live in a separate private repository. This repo contains only what's needed to run the
training pipeline on CI:

- `engine/engine.bundle.js` — the game engine as a single self-contained ESM bundle,
  auto-generated from the private repo via esbuild. Do not edit by hand; changes made
  here will be overwritten on the next sync.
- `engine/cardsets/` — card database + region deck definitions.
- `scripts/` — the training runners (evolve, tournament, swiss-tournament) and their
  workers.
- `baselines/` — currently-shipping per-region weight vectors, used as the fitness
  reference point.
- `configs/` — inputs to individual runs.
- `.github/workflows/` — GitHub Actions workflows that fan out training jobs across
  runners.

## How runs work

Training jobs are driven from a developer session via the `gh` CLI:

```bash
gh workflow run tournament.yml -f seeds=0-99 -f timeout=90
```

Each job spins up a runner, executes a slice of the seed range, and uploads results as
workflow artifacts. Results are aggregated locally via `gh run download`.

There is no long-running server, no database, no external state — every run is a pure
function of `(engine bundle, cardsets, weights, seeds, config)`.

## Runtime deps

**Zero.** The engine bundle inlines everything. GitHub Actions runners only need Node
20 (already installed on `ubuntu-latest`) — no `npm install` step required.

## License and attribution

This project's code is MIT-licensed (see `LICENSE`).

The Magi-Nation card game content it operates on -- card names, text, rules, and the card
database -- is **not** covered by that license. It belongs to the game's rights holders
and is included for non-commercial fan-project use only. See `NOTICE` for details and
sources.

This is an unofficial fan project, not affiliated with or endorsed by the rights holders.
