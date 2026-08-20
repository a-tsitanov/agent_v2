# Мост к openclaw: РЕШЕНО одним флагом

Дата: 2026-08-19, закрыто 2026-08-20. Третий кусок телеграм-бота.

## Итог: обёртка не нужна

openclaw умеет OpenAI-совместимую ручку `POST /v1/chat/completions` ИЗ КОРОБКИ.
По умолчанию выключена. Включается одним флагом в `openclaw.json`:

```json
"gateway": {"http": {"endpoints": {"chatCompletions": {"enabled": true}}}}
```

Бот ходит в неё как в любой OpenAI-эндпоинт. Ни WebSocket, ни device-подписи,
ни CLI, ни docker-сокета, ни отдельной обёртки. Обычный httpx.

Запрос:
```
POST http://openclaw:18789/v1/chat/completions
Authorization: Bearer <OPENCLAW_GATEWAY_TOKEN>
{"model":"openclaw","messages":[{"role":"user","content":"…"}]}
```
`model` — литерал `openclaw` (или `openclaw/<agentId>`). Ответ — стандартный
`choices[0].message.content`.

## Тупик, из которого выбрались

Полдня ушло на низкоуровневый WebSocket-протокол шлюза (handshake снят,
записан ниже для истории). Он упёрся в device-авторизацию: bearer-токен даёт
роль operator с ПУСТЫМИ scopes, а `chat.send` требует `operator.write`. Права
выдаёт только спаренное устройство (`identity/device.json`, Ed25519-подпись
каждого подключения). Это был неверный вход — HTTP-ручка обходит его целиком.

## Замеры (полный стек, 2026-08-20)

```
«сколько сущностей»                   30 с   → «163753 сущности» (точно)
«топ связанных + с кем связаны»       98 с   → сам вызвал graph_pagerank,
                                               потом find_neighbours на каждого
```

Второе — то, ради чего мост и нужен: наш `/ask` это ОДИН вызов kb_search,
агент же КОМБИНИРУЕТ инструменты. Первый прогон казался 6-минутным — это были
ретраи по недоступным MCP (полстека было выключено), не режим ручки.

## Осталось

1. **Команда `/agent` в боте** — httpx-вызов ручки, рядом с `/ask`, session_key
   не нужен (у openclaw своя память). Квоту и слот тратит как `/ask`.
2. **Модель агента = openai/gpt-5.5** (внешний OpenAI, платно). Возможно
   переключить на локальный litellm — но gpt-5.5 умнее в выборе инструментов;
   решать после сравнения.
3. **kb-stats не подключён к openclaw** — в конфиге только kb-search/kb-tools.
   Одна команда `openclaw mcp add`, но статистики всё равно нет (реестр пуст).
4. **Токен шлюза дефолтный** `change-me-strong-key` — поменять при выкатке.

## Снятая схема WebSocket (для истории, НЕ используется)

Протокол 4. Сервер шлёт `connect.challenge{nonce,ts}`. Клиент — `connect` с
`client:{id:"cli"|"vscode", mode:"cli", platform, version}`, `role:"operator"`,
`auth:{token}`. Ответ `hello-ok{protocol:4, auth:{role,scopes:[]}}`. Кадры:
`req{type,id,method,params}`, `res{type,id,ok,payload|error}`, `event`. Методы
чата: chat.send/abort/history/startup/metadata/message.get. Пустые scopes —
причина, по которой этот путь брошен.
