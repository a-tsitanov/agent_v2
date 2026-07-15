"""LightRAG-style KG extraction prompts, ported for kb-llamaindex.

Three prompts verbatim from HKUDS/LightRAG (MIT license), adapted
to feed our universal `EntityType` taxonomy into `{entity_types}`:

  * `ENTITY_EXTRACTION_SYSTEM` + `ENTITY_EXTRACTION_USER` — single
    call producing entities (name + type + description) and
    relations (src + tgt + keywords + description) for one chunk.
  * `ENTITY_CONTINUE_EXTRACTION_USER` — gleaning follow-up that
    asks the model "what did you miss?" using the same delimiter
    scheme as the original output.
  * `SUMMARIZE_ENTITY_DESCRIPTIONS` — cross-chunk consolidation
    prompt; takes a JSON list of descriptions of one entity (or
    one relation) and returns one coherent summary.

Why an algorithmic port instead of a dependency: we keep our
multi-label LlamaIndex / PropertyGraphIndex Neo4j schema (typed
entity labels + semantic relation types) instead of LightRAG's
single-label `:base` + `:DIRECTED` model.  See
`docs/ARCHITECTURE.md` for the trade-off and the data-model
mismatch that drove this decision.

Attribution: prompts derived from
https://github.com/HKUDS/LightRAG/blob/main/lightrag/prompt.py
(MIT license).  Few-shot examples 4–6 below were added by us to
cover the project's multilingual + multi-domain corpus
(B2B contracts in Russian, English emails / support transcripts,
analytical reports).  Modifications:

  - `{entity_types}` is plugged from
    `typing.get_args(src.graph.schema.EntityType)` so the taxonomy
    stays in one place (our universal Person/Organization/Concept/
    Issue/Resolution/EventOrAction/etc. set instead of LightRAG's
    fiction-novel-ish defaults).
  - `{language}` is left as a free-form description ("the source-text
    language") so the same prompt handles mixed-language input —
    qwen3 / gpt-4o-mini follow this reliably.
"""

from __future__ import annotations

# ── Delimiters (verbatim from LightRAG) ─────────────────────────────


TUPLE_DELIM = "<|#|>"
"""Field separator inside an `entity` / `relation` tuple line."""

COMPLETE_DELIM = "<|COMPLETE|>"
"""End-of-output sentinel the LLM emits on a line of its own."""


# ── Merge-stage tuning (LightRAG defaults, exposed for env tuning) ─


DEFAULT_FORCE_SUMMARY_ON_COUNT = 8
"""LightRAG default: trigger an LLM summary when ≥8 chunks
mentioned the same entity / relation."""

DEFAULT_FORCE_SUMMARY_ON_CHARS = 12_000
"""LightRAG default: trigger an LLM summary when concatenated
descriptions for one entity / relation cross this many chars."""

DEFAULT_SUMMARY_MAX_TOKENS = 1200
"""Soft cap fed into the summarisation prompt."""


# ── System prompt (verbatim LightRAG `entity_extraction_system_prompt`) ──


ENTITY_EXTRACTION_SYSTEM = """\
---Role---
You are a Knowledge Graph Specialist responsible for extracting entities and relationships from the input text.

---Instructions---
1.  **Entity Extraction & Output:**
    *   **Identification:** Identify clearly defined and meaningful entities in the input text.
    *   **Entity Details:** For each identified entity, extract the following information:
        *   `entity_name`: The name of the entity. If the entity name is case-insensitive, capitalize the first letter of each significant word (title case). Ensure **consistent naming** across the entire extraction process.
        *   `entity_type`: Categorize the entity using one of the following types: `{entity_types}`. If none of the provided entity types apply, do not add new entity type and classify it as `Other`.
        *   `entity_description`: Provide a concise yet comprehensive description of the entity's attributes and activities, based *solely* on the information present in the input text.
    *   **Prioritize named entities (recall):** ALWAYS extract every PROPER NOUN as its own standalone entity — organizations, companies, brands, products, people, places, government bodies, military units — and their ABBREVIATIONS / ACRONYMS (e.g. `ЦБ`, `SpaceX`, `RSR`, `Герань-2`). Do this even when the name is short, is in Latin script inside a non-Latin text, or appears only once. NEVER fold a named organization / brand / product into a description or into another entity's name; emit it on its own line.
    *   **Do not emit attribute labels as entities:** A bare measurement or attribute noun (e.g. "weight", "energy consumption", "peak speed", "battery capacity") is NOT an entity. Put such facts in the `entity_description` of the product / organization they describe; extract a `Metric` entity only when it has a concrete value AND names its subject.
    *   **Output Format - Entities:** Output a total of 4 fields for each entity, delimited by `{tuple_delimiter}`, on a single line. The first field *must* be the literal string `entity`.
        *   Format: `entity{tuple_delimiter}entity_name{tuple_delimiter}entity_type{tuple_delimiter}entity_description`

2.  **Relationship Extraction & Output:**
    *   **Identification:** Identify direct, clearly stated, and meaningful relationships between previously extracted entities.
    *   **N-ary Relationship Decomposition:** If a single statement describes a relationship involving more than two entities, decompose it into binary relationship pairs.
    *   **Relationship Details:** For each binary relationship, extract:
        *   `source_entity`: The name of the source entity (consistent with entity extraction; title-case for case-insensitive names).
        *   `target_entity`: The name of the target entity (same rule).
        *   `relationship_keywords`: One or more high-level keywords summarizing the overarching nature, concepts, or themes of the relationship. Multiple keywords within this field must be separated by a comma `,`. **DO NOT use `{tuple_delimiter}` for separating multiple keywords within this field.**
        *   `relationship_description`: A concise explanation of the nature of the relationship between the source and target entities, providing a clear rationale for their connection.
        *   `relationship_polarity`: The *logical* status of the relationship — NOT sentiment. Use exactly one of: `affirmed` (the text asserts the relationship holds), `negated` (the text states it does NOT hold or no longer holds, e.g. "X is not the owner", "Y left the company"), or `uncertain` (the text hedges, e.g. "allegedly", "reportedly", "is suspected to"). Default to `affirmed` when the relationship is plainly stated.
        *   `temporal_validity`: The period during which the relationship holds, as `from..to` using ISO dates (`YYYY` or `YYYY-MM-DD`). Use an open bound when only one side is known: `2015..` (since 2015), `..2020` (until 2020). For a single point in time use just the date. **A date may ONLY be output when the Input Text explicitly states it** (a written date, a year, a quarter). NEVER invent a date, NEVER approximate, and NEVER reuse a date you saw in these instructions or examples — when the text carries no explicit time information this field MUST be empty (nothing between the delimiters). An empty field is the correct and expected output for most relationships.
    *   **Output Format - Relationships:** Output a total of 7 fields for each relationship, delimited by `{tuple_delimiter}`, on a single line. The first field *must* be the literal string `relation`.
        *   Format: `relation{tuple_delimiter}source_entity{tuple_delimiter}target_entity{tuple_delimiter}relationship_keywords{tuple_delimiter}relationship_description{tuple_delimiter}relationship_polarity{tuple_delimiter}temporal_validity`

3.  **Delimiter Usage Protocol:**
    *   The `{tuple_delimiter}` is a complete, atomic marker and **must not be filled with content**. It serves strictly as a field separator.

4.  **Relationship Direction & Duplication:**
    *   Treat all relationships as **undirected** unless explicitly stated otherwise.
    *   Avoid outputting duplicate relationships.

5.  **Output Order & Prioritization:**
    *   Output all extracted entities first, followed by all extracted relationships.
    *   Within the list of relationships, prioritize those most significant to the core meaning of the input text first.

6.  **Context & Objectivity:**
    *   Write all entity names and descriptions in the **third person**.
    *   Explicitly name the subject or object; **avoid pronouns** such as `this article`, `this paper`, `our company`, `I`, `you`, `he`, `she`.

7.  **Language & Proper Nouns:**
    *   The entire output (entity names, keywords, and descriptions) must be written in `{language}`.
    *   Proper nouns (personal names, place names, organization names) should be retained in their original language if a proper, widely accepted translation is not available or would cause ambiguity.

8.  **Completion Signal:** Output the literal string `{completion_delimiter}` only after all entities and relationships have been completely extracted.

---Examples---
{examples}
"""


# ── User prompt for initial extraction ─────────────────────────────


ENTITY_EXTRACTION_USER = """\
---Task---
Extract entities and relationships from the input text in Data to be Processed below.

---Instructions---
1.  **Strict Adherence to Format:** Strictly adhere to all format requirements for entity and relationship lists, including output order, field delimiters, and proper noun handling, as specified in the system prompt.
2.  **Output Content Only:** Output *only* the extracted list of entities and relationships. Do not include any introductory or concluding remarks, explanations, or additional text before or after the list.
3.  **Completion Signal:** Output `{completion_delimiter}` as the final line after all relevant entities and relationships have been extracted and presented.
4.  **Output Language:** Ensure the output language is {language}. Proper nouns must be kept in their original language.

---Data to be Processed---
<Entity_types>
[{entity_types}]

<Input Text>
```
{input_text}
```

<Output>
"""


# ── User prompt for gleaning (verbatim entity_continue_extraction) ──


ENTITY_CONTINUE_EXTRACTION_USER = """\
---Task---
Based on the last extraction task, identify and extract any **missed or incorrectly formatted** entities and relationships from the input text.

---Instructions---
1.  **Strict Adherence to System Format:** Strictly adhere to all format requirements specified in the system instructions.
2.  **Focus on Corrections/Additions:**
    *   **Do NOT** re-output entities and relationships that were **correctly and fully** extracted in the last task.
    *   If an entity or relationship was **missed** in the last task, extract and output it now according to the system format.
    *   If an entity or relationship was **truncated, had missing fields, or was otherwise incorrectly formatted**, re-output the *corrected and complete* version.
3.  **Output Format - Entities:** 4 fields, delimited by `{tuple_delimiter}`, one entity per line. First field must be `entity`.
4.  **Output Format - Relationships:** 7 fields, delimited by `{tuple_delimiter}`, one relation per line. First field must be `relation`. Trailing fields are `relationship_polarity` (`affirmed`/`negated`/`uncertain`) and `temporal_validity` (`from..to` ISO dates, empty if unknown).
5.  **Output Content Only:** Output *only* the extracted list. No remarks before or after.
6.  **Completion Signal:** Output `{completion_delimiter}` as the final line.
7.  **Output Language:** {language}. Proper nouns kept in original language.

<Output>
"""


# ── Summarisation prompt (verbatim summarize_entity_descriptions) ──


SUMMARIZE_ENTITY_DESCRIPTIONS = """\
---Role---
You are a Knowledge Graph Specialist, proficient in data curation and synthesis.

---Task---
Your task is to synthesize a list of descriptions of a given entity or relation into a single, comprehensive, and cohesive summary.

---Instructions---
1. Input Format: The description list is provided in JSON format. Each JSON object (representing a single description) appears on a new line within the `Description List` section.
2. Output Format: The merged description will be returned as plain text, presented in multiple paragraphs, without any additional formatting or extraneous comments before or after the summary.
3. Comprehensiveness: The summary must integrate all key information from *every* provided description. Do not omit any important facts or details.
4. Context: Ensure the summary is written from an objective, third-person perspective; explicitly mention the name of the entity or relation for full clarity and context.
5. Conflict Handling:
  - In cases of conflicting or inconsistent descriptions, first determine if these conflicts arise from multiple, distinct entities or relationships that share the same name.
  - If distinct entities/relations are identified, summarize each one *separately* within the overall output.
  - If conflicts within a single entity/relation exist, attempt to reconcile them or present both viewpoints with noted uncertainty.
6. Length Constraint: The summary's total length must not exceed {summary_length} tokens, while still maintaining depth and completeness.
7. Language: The entire output must be written in {language}. Proper nouns may stay in their original language if a proper translation is not available.

---Input---
{description_type} Name: {description_name}

Description List:

```
{description_list}
```

---Output---
"""


# ── Few-shot examples (3 verbatim from LightRAG + 3 domain-tuned) ──


_LIGHTRAG_EXAMPLE_LITERARY = """\
<Entity_types>
[{entity_types}]

<Input Text>
```
while Alex clenched his jaw, the buzz of frustration dull against the backdrop of Taylor's authoritarian certainty. It was this competitive undercurrent that kept him alert, the sense that his and Jordan's shared commitment to discovery was an unspoken rebellion against Cruz's narrowing vision of control and order.

Then Taylor did something unexpected. They paused beside Jordan and, for a moment, observed the device with something akin to reverence.
```

<Output>
entity{tuple_delimiter}Alex{tuple_delimiter}Person{tuple_delimiter}Alex is a character who experiences frustration and is observant of the dynamics among other characters.
entity{tuple_delimiter}Taylor{tuple_delimiter}Person{tuple_delimiter}Taylor is portrayed with authoritarian certainty and shows a moment of reverence towards a device, indicating a change in perspective.
entity{tuple_delimiter}Jordan{tuple_delimiter}Person{tuple_delimiter}Jordan shares a commitment to discovery and has a significant interaction with Taylor regarding a device.
entity{tuple_delimiter}Cruz{tuple_delimiter}Person{tuple_delimiter}Cruz is associated with a vision of control and order, influencing the dynamics among other characters.
entity{tuple_delimiter}The Device{tuple_delimiter}Product{tuple_delimiter}The Device is central to the story, with potential game-changing implications, and is revered by Taylor.
relation{tuple_delimiter}Alex{tuple_delimiter}Taylor{tuple_delimiter}power dynamics, observation{tuple_delimiter}Alex observes Taylor's authoritarian behavior and notes changes in Taylor's attitude toward the device.{tuple_delimiter}affirmed{tuple_delimiter}
relation{tuple_delimiter}Jordan{tuple_delimiter}Cruz{tuple_delimiter}ideological conflict, rebellion{tuple_delimiter}Jordan's commitment to discovery is in rebellion against Cruz's vision of control and order.{tuple_delimiter}affirmed{tuple_delimiter}
relation{tuple_delimiter}Taylor{tuple_delimiter}The Device{tuple_delimiter}reverence, technological significance{tuple_delimiter}Taylor shows reverence towards the device, indicating its importance and potential impact.{tuple_delimiter}affirmed{tuple_delimiter}
{completion_delimiter}
"""

_LIGHTRAG_EXAMPLE_FINANCIAL = """\
<Entity_types>
[{entity_types}]

<Input Text>
```
Stock markets faced a sharp downturn today as tech giants saw significant declines, with the global tech index dropping by 3.4% in midday trading. Analysts attribute the selloff to investor concerns over rising interest rates and regulatory uncertainty.

Among the hardest hit, Nexon Technologies saw its stock plummet by 7.8% after reporting lower-than-expected quarterly earnings.
```

<Output>
entity{tuple_delimiter}Global Tech Index{tuple_delimiter}Concept{tuple_delimiter}The Global Tech Index tracks the performance of major technology stocks and experienced a 3.4% decline today.
entity{tuple_delimiter}Nexon Technologies{tuple_delimiter}Organization{tuple_delimiter}Nexon Technologies is a tech company that saw its stock decline by 7.8% after disappointing earnings.
entity{tuple_delimiter}Market Selloff{tuple_delimiter}EventOrAction{tuple_delimiter}Market selloff refers to the significant decline in stock values due to investor concerns over interest rates and regulations.
entity{tuple_delimiter}3.4% Decline{tuple_delimiter}Metric{tuple_delimiter}The Global Tech Index experienced a 3.4% decline in midday trading.
relation{tuple_delimiter}Global Tech Index{tuple_delimiter}Market Selloff{tuple_delimiter}market performance, investor sentiment{tuple_delimiter}The decline in the Global Tech Index is part of the broader market selloff driven by investor concerns.{tuple_delimiter}affirmed{tuple_delimiter}
relation{tuple_delimiter}Nexon Technologies{tuple_delimiter}Global Tech Index{tuple_delimiter}company impact, index movement{tuple_delimiter}Nexon Technologies' stock decline contributed to the overall drop in the Global Tech Index.{tuple_delimiter}affirmed{tuple_delimiter}
{completion_delimiter}
"""

_DOMAIN_EXAMPLE_RU_CONTRACT = """\
<Entity_types>
[{entity_types}]

<Input Text>
```
Договор поставки № ДП-2024/178-К от 15.03.2024 заключён между ООО «Северные технологии» (ИНН 7707083893) и АО «Промсервис». Контактное лицо: Иванов Иван Петрович, должность — руководитель отдела продаж.
```

<Output>
entity{tuple_delimiter}Договор № ДП-2024/178-К{tuple_delimiter}Document{tuple_delimiter}Договор поставки от 15.03.2024 между ООО «Северные технологии» и АО «Промсервис».
entity{tuple_delimiter}ООО «Северные технологии»{tuple_delimiter}Organization{tuple_delimiter}Поставщик по договору № ДП-2024/178-К; ИНН 7707083893.
entity{tuple_delimiter}АО «Промсервис»{tuple_delimiter}Organization{tuple_delimiter}Заказчик по договору № ДП-2024/178-К.
entity{tuple_delimiter}Иванов Иван Петрович{tuple_delimiter}Person{tuple_delimiter}Руководитель отдела продаж в ООО «Северные технологии», контактное лицо по договору.
entity{tuple_delimiter}7707083893{tuple_delimiter}INN{tuple_delimiter}ИНН ООО «Северные технологии».
relation{tuple_delimiter}ООО «Северные технологии»{tuple_delimiter}Договор № ДП-2024/178-К{tuple_delimiter}contractual party, supply{tuple_delimiter}ООО «Северные технологии» заключило договор поставки № ДП-2024/178-К.{tuple_delimiter}affirmed{tuple_delimiter}2024-03-15..
relation{tuple_delimiter}АО «Промсервис»{tuple_delimiter}Договор № ДП-2024/178-К{tuple_delimiter}contractual party, supply{tuple_delimiter}АО «Промсервис» — вторая сторона договора № ДП-2024/178-К.{tuple_delimiter}affirmed{tuple_delimiter}2024-03-15..
relation{tuple_delimiter}Иванов Иван Петрович{tuple_delimiter}ООО «Северные технологии»{tuple_delimiter}employment, contact{tuple_delimiter}Иванов работает в ООО «Северные технологии» руководителем отдела продаж.{tuple_delimiter}affirmed{tuple_delimiter}
{completion_delimiter}
"""

_DOMAIN_EXAMPLE_SUPPORT_TRANSCRIPT = """\
<Entity_types>
[{entity_types}]

<Input Text>
```
Agent: Hello, how can I help today? Customer: My order #4521 never arrived, but the tracking says delivered three days ago. Agent: I see — I'll issue a refund for order #4521 and forward the case to logistics.
```

<Output>
entity{tuple_delimiter}Customer{tuple_delimiter}Person{tuple_delimiter}Customer reporting that order #4521 was never received despite tracking confirmation.
entity{tuple_delimiter}Agent{tuple_delimiter}Person{tuple_delimiter}Support agent handling the case for order #4521; issued a refund and escalated to logistics.
entity{tuple_delimiter}Order #4521{tuple_delimiter}Document{tuple_delimiter}Order whose delivery is disputed despite the tracking marking it delivered.
entity{tuple_delimiter}Refund for Order #4521{tuple_delimiter}Resolution{tuple_delimiter}Refund issued by the agent as the resolution to the missing-delivery complaint.
entity{tuple_delimiter}Missing Delivery{tuple_delimiter}Issue{tuple_delimiter}Customer's complaint that order #4521 was never delivered though tracking shows delivered.
relation{tuple_delimiter}Customer{tuple_delimiter}Missing Delivery{tuple_delimiter}complaint, support{tuple_delimiter}The customer reported the missing-delivery issue to support.{tuple_delimiter}affirmed{tuple_delimiter}
relation{tuple_delimiter}Missing Delivery{tuple_delimiter}Refund for Order #4521{tuple_delimiter}resolution, refund{tuple_delimiter}Missing delivery resolved by issuing a refund.{tuple_delimiter}affirmed{tuple_delimiter}
relation{tuple_delimiter}Agent{tuple_delimiter}Refund for Order #4521{tuple_delimiter}action, resolution{tuple_delimiter}The agent issued the refund.{tuple_delimiter}affirmed{tuple_delimiter}
{completion_delimiter}
"""

_DOMAIN_EXAMPLE_ANALYTICAL = """\
<Entity_types>
[{entity_types}]

<Input Text>
```
The Q1 2024 report shows conversion grew 12% driven by the onboarding redesign launched on 2024-02-15. The cohort of new users completed the welcome flow 1.8× faster than the previous quarter.
```

<Output>
entity{tuple_delimiter}Q1 2024 Report{tuple_delimiter}Document{tuple_delimiter}Quarterly analytical report covering Q1 2024 conversion and onboarding metrics.
entity{tuple_delimiter}Conversion Growth{tuple_delimiter}Metric{tuple_delimiter}Conversion grew 12% during Q1 2024 as reported in the analytical report.
entity{tuple_delimiter}Onboarding Redesign{tuple_delimiter}EventOrAction{tuple_delimiter}Product change launched on 2024-02-15 that drove the Q1 2024 conversion growth.
entity{tuple_delimiter}Welcome Flow{tuple_delimiter}Product{tuple_delimiter}New-user onboarding flow whose completion time improved 1.8× quarter-on-quarter.
entity{tuple_delimiter}New User Cohort{tuple_delimiter}Concept{tuple_delimiter}The cohort of new users measured against the welcome-flow completion KPI.
relation{tuple_delimiter}Onboarding Redesign{tuple_delimiter}Conversion Growth{tuple_delimiter}causation, product change{tuple_delimiter}The onboarding redesign caused the 12% conversion growth in Q1 2024.{tuple_delimiter}affirmed{tuple_delimiter}2024-01-01..2024-03-31
relation{tuple_delimiter}New User Cohort{tuple_delimiter}Welcome Flow{tuple_delimiter}user journey, KPI{tuple_delimiter}The cohort of new users completed the welcome flow 1.8× faster than the previous quarter.{tuple_delimiter}affirmed{tuple_delimiter}2024-01-01..2024-03-31
{completion_delimiter}
"""


EXAMPLES_DEFAULT = [
    _LIGHTRAG_EXAMPLE_LITERARY,
    _LIGHTRAG_EXAMPLE_FINANCIAL,
    _DOMAIN_EXAMPLE_RU_CONTRACT,
    _DOMAIN_EXAMPLE_SUPPORT_TRANSCRIPT,
    _DOMAIN_EXAMPLE_ANALYTICAL,
]
"""Five few-shots covering: literary scene, financial news,
Russian B2B contract, English support transcript, English
analytical report.  The examples each declare their own
`<Entity_types>` block — that anchors the model to the project's
universal taxonomy so it doesn't drift toward generic categories."""


def render_examples(
    examples: list[str],
    *,
    entity_types: str,
    tuple_delimiter: str = TUPLE_DELIM,
    completion_delimiter: str = COMPLETE_DELIM,
) -> str:
    """Render the few-shot examples block for the system prompt.

    Each example template carries `{entity_types}`,
    `{tuple_delimiter}` and `{completion_delimiter}` placeholders;
    we fill them once with the same values that will be in the
    actual extraction call so the model sees a perfectly consistent
    schema.
    """
    rendered = []
    for ex in examples:
        rendered.append(
            ex.format(
                entity_types=entity_types,
                tuple_delimiter=tuple_delimiter,
                completion_delimiter=completion_delimiter,
            )
        )
    return "\n\n".join(rendered)


# ── Event extraction addendum (injected ONLY when events are requested) ──
#
# Append EVENT_INSTRUCTION to the system prompt when the caller passes
# `extraction_events=True` (Task 3 wires this).  Default entity/relation
# extraction is unchanged when this block is absent.


EVENT_INSTRUCTION = """\
---Events---
Extract *discrete occurrences* described in the text as `event` tuples, on top of the entity and relation output above.

**Output Format — Events:** Output 7 fields per event, delimited by `{tuple_delimiter}`, on a single line.  The first field *must* be the literal string `event`.

  Format: `event{tuple_delimiter}event_type{tuple_delimiter}trigger_phrase{tuple_delimiter}participants{tuple_delimiter}event_timestamp{tuple_delimiter}event_location{tuple_delimiter}event_polarity`

Field definitions:
  * `event_type`: one of: {taxonomy}. Use `other` if none fits — do not invent new labels.
  * `trigger_phrase`: the verb phrase or noun that signals the event in the text (e.g. "signed a contract", "merger announced").
  * `participants`: semicolon-separated list of entity names involved (e.g. `Romashka;Lutik`).  Use the same names as in the entity list.
  * `event_timestamp`: copy the time expression VERBATIM from the text (e.g. "вчера", "в марте", "6 июля с 12:00 до 18:00"). Leave **empty** if the text states no time. NEVER guess or invent a date — do not normalize, do not add a year.
  * `event_location`: place where the event occurred; leave **empty** if not mentioned.
  * `event_polarity`: logical polarity — `affirmed` (event occurred), `negated` (explicitly did NOT occur), or `uncertain` (hedged).  Default `affirmed`.

Output all events after the entity and relation lines, before the `{completion_delimiter}` sentinel.

---Event Examples---
<Input Text>
```
Вчера ООО «Ромашка» и ООО «Лютик» подписали договор поставки.
```
<Output>
event{tuple_delimiter}deal{tuple_delimiter}подписали договор поставки{tuple_delimiter}ООО «Ромашка»;ООО «Лютик»{tuple_delimiter}Вчера{tuple_delimiter}{tuple_delimiter}affirmed
{completion_delimiter}

<Input Text>
```
Совет директоров одобрил слияние компаний Alpha и Beta. (нет времени в тексте)
```
<Output>
event{tuple_delimiter}deal{tuple_delimiter}одобрил слияние{tuple_delimiter}Alpha;Beta{tuple_delimiter}{tuple_delimiter}{tuple_delimiter}affirmed
{completion_delimiter}
"""
"""Addendum appended to the system prompt when event extraction is enabled.

Contains the tuple spec and few-shot examples.  Append to
`ENTITY_EXTRACTION_SYSTEM` (after `.format(...)`) only when events are
requested — do NOT include in the default entity/relation prompt.
"""


__all__ = [
    "COMPLETE_DELIM",
    "DEFAULT_FORCE_SUMMARY_ON_CHARS",
    "DEFAULT_FORCE_SUMMARY_ON_COUNT",
    "DEFAULT_SUMMARY_MAX_TOKENS",
    "ENTITY_CONTINUE_EXTRACTION_USER",
    "ENTITY_EXTRACTION_SYSTEM",
    "ENTITY_EXTRACTION_USER",
    "EVENT_INSTRUCTION",
    "EXAMPLES_DEFAULT",
    "SUMMARIZE_ENTITY_DESCRIPTIONS",
    "TUPLE_DELIM",
    "render_examples",
]
