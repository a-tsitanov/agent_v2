# Hermes Agent integration runbook

Подключение `kb-llamaindex` к [Hermes Agent](https://hermes-agent.nousresearch.com)
(Nous Research) — персистентному серверному агенту. RAG выступает источником
инструментов через MCP по SSE; память, цикл диалога и обучаемость — на стороне
Hermes. Интеграция **аддитивна**: поведение MCP-серверов не меняется.

Связанные runbook'и: [`mcp.md`](mcp.md) (сами MCP-серверы; §3 про контракт
`kb_search` частично устарел — см. баннер в начале того файла),
[`search-usage.md`](search-usage.md).

## 1. Поднять SSE-сервисы

Рядом с Temporal worker (тот же стек: Milvus, Neo4j, Postgres, LiteLLM):

```bash
# Атомарные тулы (основная поверхность для интерактивного цикла)
uv run python -m src.mcp.tools_server  --transport sse --host 0.0.0.0 --port 9002

# kb_search (тяжёлый escape-hatch для многоходовых вопросов)
uv run python -m src.mcp.search_server --transport sse --host 0.0.0.0 --port 9001
```

**Авторизация:** при `KB_MCP_REQUIRE_AUTH=true` (по умолчанию) сервер требует
заголовок `Authorization: Bearer <key>`, где `<key>` ∈ `API_KEYS`. Без ключей
сервер не стартует (`assert_api_key_env_set`). Для локального stdio-режима
desktop-клиентов auth отключается через `KB_MCP_REQUIRE_AUTH=false`.

> **⚠️ Про `StaticTokenVerifier`.** Auth реализован через FastMCP
> `StaticTokenVerifier` — он хранит токены (= `API_KEYS`) в открытом виде в
> памяти процесса. Функционально это эквивалент Bearer-аутентификации по
> API-ключу, но: (1) держите SSE-порты во внутренней сети, не публикуйте
> наружу; (2) при выставлении наружу — закрывайте reverse-proxy с TLS;
> (3) ротация ключей = смена `API_KEYS` + рестарт сервиса; (4) если нужны
> срок жизни/скоупы токенов — переходите на `JWTVerifier`.

## 2. Зарегистрировать в Hermes

Скопировать блок из [`integrations/hermes/config.example.yaml`](../../integrations/hermes/config.example.yaml)
в `~/.hermes/config.yaml`, заменив `<kb-host>` и выставив `KB_API_KEY` в окружении
процесса Hermes. При старте Hermes сам дискаверит тулы и покажет чек-лист; имена
в агенте получают префикс `mcp_kbtools_*` и `mcp_kbsearch_kb_search`.

## 3. Установить скилл

Скилл [`integrations/hermes/knowledge-base/SKILL.md`](../../integrations/hermes/knowledge-base/SKILL.md)
кладётся в `~/.hermes/skills/knowledge-base/SKILL.md` (или публикуется через Skills
Hub). Он учит Hermes выбирать тул под тип задачи, форматировать ответы с
цитатами, пользоваться памятью и резолвить follow-up-реплики в самодостаточные
запросы.

## 4. Smoke-проверка

1. Hermes стартует без ошибок и в списке тулов видны 8 `mcp_kbtools_*` +
   `mcp_kbsearch_kb_search`.
2. Запрос с точным идентификатором (телефон/ИНН) → Hermes зовёт
   `find_entity_by_id` и возвращает досье.
3. Неверный/отсутствующий `KB_API_KEY` → запрос к SSE отклоняется (401).

## 5. Приёмочные сценарии

См. `tests/eval/hermes_scenarios.py` — золотой набор интерактивных сценариев
(по одному на ветку дерева выбора тула + многоходовый follow-up + досье). Прогон
end-to-end через живой Hermes — ручной; критерии: верный тул, применён шаблон,
сработала реформулировка.
