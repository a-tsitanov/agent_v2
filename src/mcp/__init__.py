"""MCP servers exposing kb-llamaindex to external LLM-tool clients.

Two servers:

* ``search_server`` (MCP-1) — high-level: one ``kb_search(query)``
  tool that submits ``SearchOrchestratorWorkflow`` (plan-execute-
  synthesize) and streams progress back as MCP notifications.  Goes
  through Temporal so concurrent search sessions share GPU budget via
  the search task queue cap.

* ``tools_server`` (MCP-2) — atomic: seven raw retrieval tools
  (vector_search, graph_search, ...) reusing ``atomic_tools.*``
  in-process.  Faster (no workflow overhead), GPU protected via
  the project-wide ``BoundedLLM`` semaphore.

Both speak MCP over stdio and HTTP/SSE.  Auth: Bearer ``API_KEY``
matched against ``settings.api.api_keys_list``.
"""
