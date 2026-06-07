# ADR-0012: Wikibase as the canonical anchor + continuous wiki editor with anti-drift bot-sections

- Status: Accepted
- Date: 2026-06-07

## Context

The knowledge graph needs a canonical, human-auditable anchor for the entities
it extracts, and a human-readable surface that stays faithful to the graph. A
naive "LLM rewrites the article from the previous article" approach drifts:
each generation edits the prior prose and errors compound; humans and the bot
also fight over the same text.

## Decision

Two coupled decisions:
1. **Wikibase as the canonical anchor.** The `push_wikibase` activity projects
   each ingested batch's merged entities into the local Wikibase (items keyed to
   graph entities, statements from relations, via bootstrap caches of
   `:WikibaseBaseClass` / `:WikibaseProperty`). It is best-effort
   (`WIKIBASE_ENABLED=false` → skipped; any error → failed) so ingest still
   finalizes, and it detects the silent no-op (entities in, 0 items
   created/updated → marked failed).
2. **Continuous wiki editor with anti-drift bot-sections.** A `kb-wiki`
   `WikiSweepWorkflow` regenerates each `wiki_dirty` entity's article. The bot
   owns ONLY the text between `KB-BOT:START`/`KB-BOT:END` markers (human text
   outside is preserved verbatim), and the section is rewritten **from the graph
   each time** — no prior article prose is fed to the LLM. Unchanged entities
   are skipped via a subgraph hash. Opt-in via `WIKI_ENABLED`.

## Consequences

- A stable canonical anchor plus a grounded, cited, drift-free human surface;
  bot and human edits coexist by ownership markers.
- Commits us to running/maintaining a Wikibase + MediaWiki and the dirty-flag /
  subgraph-hash machinery; both features are opt-in and best-effort so ingest is
  never blocked by them.

## Alternatives considered

- **Rewrite the article from its previous prose** — compounding drift; rejected
  for the regenerate-from-graph guarantee.
- **One-shot wiki generation** — goes stale as the graph evolves; the continuous
  dirty-driven sweep keeps articles current.

## References

- `src/workflow/activities/push_wikibase.py`, `src/storage/wikibase.py`,
  `src/workflow/wiki/` (`wiki_sweep.py`, `article.py`), `src/graph/wiki_dirty.py`,
  `src/graph/wiki_context.py`
- `docs/runbook/wikibase.md`, `docs/runbook/wiki-editor.md`;
  CONCEPTS.md → "Wikibase anchor & continuous wiki editor"
