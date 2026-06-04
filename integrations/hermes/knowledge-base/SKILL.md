---
name: knowledge-base
description: >
  Use when the user asks about company/domain knowledge, internal documents, or
  entities (people, organisations, phone numbers, INN/OGRN/SNILS/email) likely
  stored in the internal knowledge base. Routes questions to the kb-llamaindex
  retrieval tools and formats grounded, cited answers.
---

# Knowledge Base

This skill drives the `kb-llamaindex` retrieval tools (exposed over MCP as
`mcp_kbtools_*` and `mcp_kbsearch_kb_search`). The knowledge base is **stateless**
— it has no memory of the conversation, so you must give each call a
self-contained query and keep the conversational state yourself.

## Tool selection (orient on the problem first)

Pick the tool by the *shape* of the question, not by habit:

- **Known exact identifier** (E.164 phone, INN, OGRN, SNILS, email) →
  `find_entity_by_id(name, entity_type=None)`. Use when the user already names a
  precise identifier.
- **Partial / fuzzy name** ("Иванов", "ООО Ромаш…", a surname only) →
  `find_entity_by_name(query, limit=10)`. Full-text, partial-name tolerant —
  use to resolve a name into canonical entities before drilling in.
- **Relationships** ("who is connected to X", "X's surroundings", "X's owner") →
  `find_neighbours(entity_name, hops=1)` for a direct walk, or
  `graph_search(query, depth=2)` when the entity isn't pinned yet.
- **Connection chains from a known entity** ("how is X transitively linked to Y",
  "trace X's network N hops out") → `graph_walk(start_entity, hops=2,
  rel_filter=None)`. Follows actual relationships outward (bounded); use when one
  hop isn't enough and you already have a starting entity.
- **Factual / semantic question** → `vector_search(query, top_k=10)`. If a hit
  needs surrounding context within its source, follow up with
  `get_chunks_by_doc_id(doc_id, limit, offset)`.
- **Need the raw full text** (tables, code, short documents that chunking splits
  badly) → `read_full_document(doc_id, max_chars=20000)`.
- **Hard multi-hop question** you cannot resolve in 2–3 atomic calls → escalate to
  `kb_search(query)`. It runs the full plan-execute-synthesize workflow and
  returns an answer with `citations` and `uncertainties`. Treat it as the
  expensive escape hatch, not the default.

**Canonical anchor:** the local Wikibase is the source of truth for entity
identity. When names conflict, trust the canonical name returned by the graph
tools over a name guessed from free text.

## Response templates (by task type)

The tools return `sources` (and `kb_search` adds `citations` + `uncertainties`).
Format the answer to the task:

- **Factual answer** — the claim, then citations as `[doc_id]` after each
  supported statement. If `kb_search` returned `uncertainties`, add a short
  "Unverified / uncertain" block listing them. Never present a `vector_search`
  hit as certain if the text only partially supports the claim.
- **Entity dossier** — canonical name, type, key attributes, relations (from
  `find_neighbours` / `graph_search`), then the source `doc_id`s. Use when the
  user asks "tell me about X".
- **"What do we know about X"** — a grouped summary across vector + graph results
  (facts, relationships, open questions), each line linked to its `doc_id`.
- **Answer language = the user's question language.** Note: `kb_search` synthesises
  in Russian; if the user wrote in another language, translate its answer.

## Memory (record and reuse `~/.hermes/`)

Use your persistent memory to make the base feel personal across sessions:

- **Record:** the user's domain/role, recurring entities and important `doc_id`s
  they return to, and their preferred answer format.
- **Reuse:** before a tool call, enrich the query with remembered context (entity
  names, typical department, prior `doc_id`s) — this compensates for the stateless
  base. Example: a bare "what's the latest?" becomes a self-contained query about
  the project the user has been asking about.

## Follow-up handling (multi-turn)

Because the base is stateless, **resolve references before every call**. Rewrite
"he / she / it / there / that one" into the concrete entity from earlier in the
conversation, and fold relevant constraints from prior turns into one
self-contained query. Never forward a raw follow-up utterance to a tool.
