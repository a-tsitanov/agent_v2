# Graph Report - .  (2026-07-24)

## Corpus Check
- Large corpus: 793 files · ~996,837 words. Semantic extraction will be expensive (many Claude tokens). Consider running on a subfolder.

## Summary
- 7417 nodes · 15894 edges · 365 communities (333 shown, 32 thin omitted)
- Extraction: 95% EXTRACTED · 5% INFERRED · 0% AMBIGUOUS · INFERRED: 848 edges (avg confidence: 0.67)
- Token cost: 2,315,489 input · 27,400 output

## Community Hubs (Navigation)
- test_workflow: test_search_community.py
- test_graph: entity_vid()
- test_graph: test_lightrag_parse.py
- workflow: get_llm_pool()
- test_ingestion: test_translate_transform.py
- test_make_env.py
- retrieval: BoundedLLM
- test_workflow: graph_admin.py
- test_graph: test_communities.py
- workflow: orchestrator.py
- test_workflow: IngestParams
- test_api: search_v2.py
- test_ingestion: extract_identifiers()
- test_workflow: contracts.py
- workflow: contracts.py
- storage: push_entities()
- eval: test_scale_bench_smoke.py
- ingestion: identifiers.py
- test_api: AsyncPostgres
- graph: analysis.py
- graph: _q()
- test_graph: test_wiki_graph_ops.py
- test_graph: NebulaGraphStore
- test_graph: merge_kg_extraction()
- test_storage: ChunkRepository
- workflow: test_search_drift_roundtrip.py
- test_graph: test_event_extract.py
- config.py: Settings
- test_graph: MilvusEntityVectorStore
- graph: signals.py
- test_graph: resolve_entities()
- test_workflow: activities.py
- test_bot: Turn
- test_analytics: _FakeStore
- test_storage: test_ingest_metrics.py
- graph: entity_resolution.py
- graph: test_community_summarize.py
- graph: clamp_top_n()
- test_workflow: rerank.py
- analytics: PrimitiveResult
- analytics: Claim
- test_graph: test_nebula_schema.py
- test_workflow: test_search_global.py
- test_workflow: Ctx
- test_workflow: test_search_orchestrator.py
- test_graph: test_nebula_store_subgraph.py
- test_graph: LightRAGExtractor
- test_ingestion: test_classifier.py
- MinioStorage
- mcp: tools_server.py
- workflow: SerializedNode
- test_graph: test_aggregations_graph_ops.py
- test_scripts: datetime
- graph: communities.py
- test_graph: test_index.py
- test_config: LiteLLMSettings
- test_graph: test_nebula_store_writes.py
- api: ingest.py
- analytics: catalog.py
- test_config: test_settings.py
- superpowers: leidenalg/igraph community backend (community_backend flag)
- test_analytics: test_planner.py
- analytics: dynamics.py
- test_workflow: merge_and_resolve()
- test_observability: test_ingest_metrics_extractor.py
- graph: events_llm.py
- test_graph: test_entity_resolution.py
- retrieval: build_vector_index()
- workflow: worker.py
- setup_wikibase.py
- test_workflow: test_search_retrieve.py
- test_retrieval: test_hf_offline.py
- graph: events.py
- IngestSchedulerWorkflow
- ingest_queue: ingest_submit.py
- test_graph: test_event_merge.py
- observability: trace_request()
- retrieval: DateBounds
- events_eval.py
- test_api: analyze.py
- storage: backfill_doc_id.py
- reresolve_graph.py
- tg_ingest.py
- graph: communities.py
- test_graph: test_rollups_graph_ops.py
- build_graph_store()
- eval: score_case()
- eval: test_medical_fixture.py
- graph: MilvusCommunityReportVectorStore
- graph: domain.py
- test_analytics: test_aggregations.py
- test_analytics: _FakeOps
- bot: __main__.py
- graph: centrality.py
- test_ingestion: IdentifierCanonicalizationTransform
- workflow: _search_deps.py
- test_ingestion: index_vector.py
- test_graph: resolve()
- test_analytics: test_events_llm.py
- workflow: materialize_activities.py
- aggregations_graph_ops.py
- test_graph: write_entity_article()
- test_graph: test_er_graph_ops.py
- SEARCH.md — search subsystem deep reference
- retrieval: build_llm()
- GraphRetriever
- mcp: _shared.py
- workflow: KGExtracted
- workflow: test_search_route.py
- test_graph: test_events_llm_graph_ops.py
- wipe_db.py
- test_analytics: test_claim_nli.py
- graph: community_writeback.py
- retrieval: GroupFilter
- storage: AsyncMediaWiki
- test_analytics: config.py
- superpowers: DocumentIngestWorkflow
- build_ingestion_pipeline()
- test_scripts: test_tg_ingest_reingest.py
- analytics: materialize.py
- test_graph: extract_entity_edges()
- workflow: contextualize.py
- test_workflow: test_article.py
- test_graph: test_quality_graph_ops.py
- test_graph: test_signals_graph_ops.py
- test_graph: test_alerts.py
- test_graph: stamp_first_seen()
- test_retrieval: test_graph_walk_retriever.py
- workflow: global_search.py
- workflow: StagingStore
- eval: identifier_recall.py
- retrieval: test_answer_template.py
- test_retrieval: _StubGraphRetriever
- test_workflow: CoverageResult
- ner_eval.py
- test_api: test_ingest.py
- test_graph: test_domain_graph_ops.py
- test_graph: test_dynamics_graph_ops.py
- test_scripts: test_reresolve_graph.py
- diagrams: activity: retrieve_subquestion (hybrid)
- superpowers: WikiGraphOps Protocol (dirty bookkeeping, subgraph,
- test_graph: index.py
- test_retrieval: test_query_planner.py
- test_workflow: MapCommunitiesParams
- test_graph_edge_export.py
- test_ingest_queue: RabbitMQSettings
- test_retrieval: test_hybrid.py
- diagrams: DocumentIngestWorkflow (IngestParams to IngestResult, queue
- superpowers: Phase 3 er_vec slice —
- superpowers: channel group enum (src/retrieval/groups.py)
- superpowers: doc_group chunk metadata (mirrors doc_date_epoch
- analytics: StepResult
- test_graph: test_alert_store.py
- test_graph: ERConfig
- graph: GLiNERExtractor
- test_graph: test_communities_graph_ops.py
- diagrams: Temporal Worker (activities + workflows
- superpowers: Automatic event detection — design
- test_analytics: test_claim_extract.py
- test_graph: test_analysis_nebula.py
- graph: CanonicalLinker
- graph_edge_export.py
- graph: lightrag_extract.py
- test_graph: write_with_retry()
- test_mcp: test_tools_server.py
- run_answer_eval.py
- test_graph: test_centrality_graph_ops.py
- test_graph: test_events_graph_ops.py
- download_models.py
- test_retrieval: test_llm_factory.py
- superpowers: kb-llamaindex Conference Deck plan
- superpowers: LLMPool (per-process role lanes +
- graph: admin.py
- er_graph_ops.py
- test_retrieval: RoundGraphData
- test_workflow: push_wikibase.py
- test_graph: test_community_read.py
- ingest_scale_bottlenecks.svg: DocumentIngestWorkflow
- superpowers: Analytical layer — design (NL
- setup_db.py
- test_analytics: ids.py
- bot: pipeline.py
- bot: with_fallback()
- test_config: TemporalSettings
- test_graph: community_graphscope.py
- graph: community_read.py
- ingestion: identifier_transform.py
- retrieval: atomic_tools.py
- test_retrieval: get_chunks_by_doc_id()
- test_graph: _FakeClient
- superpowers: Seven Tracks plan
- di: providers.py
- merge_identifier_duplicates.py
- set_admission.py: get_temporal_client()
- graph: alert_store.py
- graph: event_ts_resolver.py
- test_api: test_search_v2_routes.py
- test_analytics: _FakeOps
- test_graph: test_retriever_triplet_parse.py
- test_ingest_queue: test_consumer.py
- Architecture Decision Records (ADR) practice
- ANALYTICS-GUIDE.md: Centralities (four notions of importance)
- CONCEPTS.md: Entity Resolution (ER)
- superpowers: Wikibase populator runbook
- superpowers: Agentic Search plan (Plan #2)
- workflow: wiki_sweep.py
- build_er_graph_ops()
- Neo4jWikiGraphOps
- test_workflow: select_communities_descent()
- eval: run_scale_bench.py
- test_retrieval: test_atomic_tools.py
- test_storage: test_minio_stream.py
- superpowers: NebulaGraph cutover — neo4j decommissioned
- superpowers: Spec — Seven Tracks (build
- test_workflow: dispatch_for_route()
- eval: diag_kg_lightrag.py
- Neo4jAggregationsGraphOps
- Neo4jAnalyticsGraphOps
- workflow: retrieve.py
- test_retrieval: test_graph_path_depth.py
- diagrams: GlobalSearchWorkflow (mode=global): GraphRAG map-reduce over
- presentation: Conference deck A (tech/ML)
- superpowers: Nebula community-BUILD (nGQL) implementation plan
- message_stats.py
- test_config: test_preflight.py
- AnalyticsGraphOps
- test_graph: test_store.py
- ARCHITECTURE.md: DocumentIngestWorkflow
- test_analytics: detect_contradictions_e2e()
- ARCHITECTURE.md: Production docker-compose
- superpowers: GraphRetriever.for_store (store-only, no PropertyGraphIndex)
- api: stats.py
- test_graph: test_retriever_fulltext.py
- WikiGraphOps
- eval: bench_flat_vs_hnsw()
- CONCEPTS.md: LLMPool concurrency gating
- runbook: DocumentIngestWorkflow (parent)
- adr: Neo4j property graph store
- ANALYTICS-GUIDE.md: Community detection / Leiden
- CONCEPTS.md: Local search (vector + graph
- superpowers: WikiSweepWorkflow (dirty-entity sweep)
- test_retrieval: find_entity_by_name()
- test_api: test_documents.py
- test_graph: _FakeWriteback
- superpowers: LLMPool (per-process role-keyed pool)
- superpowers: Community-detection offload from Neo4j —
- NebulaEventsLlmGraphOps
- retrieval: GraphRetrieverProtocol
- test_observability: test_litellm_models.py
- test_scripts: test_merge_identifier_duplicates.py
- test_workflow: _FakeNebulaStore
- nebula_bootstrap.py: _connect()
- ARCHITECTURE.md: Neo4j property graph store
- superpowers: EntityVectorStore seam (knn/upsert)
- backfill_er_vector.py: configure_logging()
- ingest_medical.py
- test_scripts: _pages_to_delete()
- KbGraphStore
- test_analytics: test_domain.py
- test_graph: test_community_vector_store.py
- runbook: Per-role/tier LLM selection
- presentation: 5-step ingestion pipeline
- runbook: Milvus chunk vector index
- Runbook index
- superpowers: GET /api/v1/documents/{doc_id} download endpoint
- superpowers: Hermes ↔ kb-llamaindex RAG integration
- build_property_graph_index()
- check_ingestion.py
- test_config: WikiSettings
- test_graph: schema.py
- ingestion: _extract_addresses()
- retrieval: pick_priority()
- test_analytics: test_communities.py
- test_api: test_graph_admin.py
- test_mcp: test_hermes_skill.py
- test_mcp: test_search_server.py
- test_workflow: test_completion_no_walltime_cap.py
- knowledge-base Hermes skill (routes to kb-llamaindex
- tg_ingest.py: load_state()
- eval: hermes_scenarios.py
- test_api: test_admin_wiki.py
- test_api: test_monitor_route.py
- test_api: test_route_skeletons.py
- test_scripts: test_setup_db.py
- CONCEPTS.md: ANN vector search
- INGEST.md: GraphBuildWorkflow (child)
- graph_refresh.sh
- graph: AlertStore
- graph: NoOpKGExtractor
- test_ingestion: build_custom_kg_payload()
- retrieval: _injected_params()
- eval: graph_depth_probes.py
- test_analytics: test_rollups.py
- test_graph: _Boom
- test_workflow: test_search_pooled_llm.py
- CAPACITY_TUNING.md: LLM_POOL_N throttle
- superpowers: Graph-scale follow-ups (items 8/13/12) plan
- superpowers: GraphScope community backend (distributed single-level
- superpowers: Analytics query fixes implementation plan
- superpowers: scripts/make_env.py builder
- Grafana datasources provisioning (Prometheus prom-kb +
- smoke.sh
- conftest.py
- eval: test_identifier_recall_thresholds.py
- test_graph: _is_exempt()
- test_mcp: test_hermes_config_example.py
- ARCHITECTURE.md: Postgres (documents, ingest_metrics)
- superpowers: Wiki article quality + source-download
- superpowers: Marp two-version deck (A tech
- superpowers: TG → ingest test harness
- graph: read_alerts()
- test_api: test_health.py
- test_mcp: test_kb_analyze_registered.py
- ANALYTICS-GUIDE.md: Link prediction (LIKELY_LINK)
- ingest_scale_bottlenecks.svg: Postgres (psycopg connect per call)
- superpowers: Conversation history (multi-turn search) plan
- superpowers: Deploy redesign Batch 2 (single
- superpowers: Deploy redesign Batch 3 (DX)
- start.sh
- mcp: __init__.py
- workflow: __init__.py
- workflow: __init__.py
- workflow: __init__.py
- eval: __init__.py
- test_workflow: _stub_activity_ctx()
- CONCEPTS.md: Conversation history contextualization
- pyproject.toml: kb-llamaindex
- eval: Synthetic scale-bench harness README (250k-entity

## God Nodes (most connected - your core abstractions)
1. `PrimitiveResult` - 117 edges
2. `entity_vid()` - 101 edges
3. `extract_identifiers()` - 95 edges
4. `_q()` - 89 edges
5. `AsyncPostgres` - 80 edges
6. `Primitive` - 70 edges
7. `_Frozen` - 61 edges
8. `_FakeStore` - 61 edges
9. `build_graph_store()` - 58 edges
10. `Ctx` - 55 edges

## Surprising Connections (you probably didn't know these)
- `Production docker-compose` --conceptually_related_to--> `Neo4j property graph store`  [AMBIGUOUS]
  docker-compose.prod.yml → docs/ARCHITECTURE.md
- `Telegram bot overlay` --semantically_similar_to--> `OpenClaw agent gateway overlay`  [INFERRED] [semantically similar]
  docker-compose.bot.yml → docker-compose.openclaw.yml
- `_ReadOnlyGraphStore` --uses--> `ERConfig`  [INFERRED]
  scripts/reresolve_graph.py → src/graph/entity_resolution.py
- `_main()` --calls--> `get_temporal_client()`  [EXTRACTED]
  scripts/setup_wiki_schedule.py → src/workflow/client.py
- `test_clamp_top_n()` --calls--> `clamp_top_n()`  [EXTRACTED]
  tests/test_analytics/test_ids.py → src/analytics/ids.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Ingest pipeline (workflow + claim-check staging)** — docs_ingest_document_ingest_workflow, docs_ingest_extract_kg, docs_ingest_merge_and_resolve, docs_ingest_graph_build_workflow, docs_concepts_claim_check [INFERRED 0.85]
- **LLM concurrency & capacity model** — docs_concepts_llmpool, docs_concepts_queue_isolation, docs_capacity_tuning_llm_pool_n, docs_models_two_tier [INFERRED 0.80]
- **GraphRAG global search** — docs_concepts_leiden, docs_concepts_community_reports, docs_concepts_global_search, docs_features_hierarchical_communities [INFERRED 0.80]
- **Benchmark-gated adoption (measure recall/latency/parity before flipping a default)** — docs_adr_0006_milvus_hnsw_default_index, docs_adr_0008_native_vector_knn_er, docs_adr_0015_community_detection_backend_leidenalg, docs_scale_phase2_3 [INFERRED 0.75]
- **Offline decoupled community build on kb-graph-build (Leiden hierarchy → structured reports)** — docs_search_communitybuildworkflow, docs_adr_0009_hierarchical_leiden_communities, docs_adr_0015_community_detection_backend_leidenalg, docs_queues [EXTRACTED 0.85]
- **Fail-open degradation philosophy across the live search path (never hard-fail on a flaky helper)** — docs_search, docs_adr_0011_plan_execute_search_orchestrator, docs_adr_0010_dynamic_community_selection [INFERRED 0.75]
- **kb-llamaindex storage / infra map** — docs_adr_readme_kb_llamaindex, docs_adr_readme_milvus, docs_adr_readme_neo4j, docs_presentation_kb_llamaindex_conf_a_postgres, docs_presentation_kb_llamaindex_conf_a_litellm [EXTRACTED 0.85]
- **5-step ingestion pipeline stages** — docs_presentation_kb_llamaindex_conf_a_ingestion_pipeline, docs_presentation_kb_llamaindex_conf_a_identifier_canonicalization, docs_presentation_kb_llamaindex_conf_a_lightrag_extraction, docs_presentation_kb_llamaindex_conf_a_cross_chunk_merge, docs_presentation_kb_llamaindex_conf_a_entity_resolution [EXTRACTED 0.85]
- **Graph analytics layer components** — docs_runbook_graph_analytics_analyze_layer, docs_runbook_graph_analytics_materialize, docs_runbook_graph_analytics_monitor_arc2 [EXTRACTED 0.85]
- **DocumentIngestWorkflow ingest activity chain** — docs_superpowers_plans_2026_05_15_ingest_temporal_workflow_documentingest, docs_superpowers_plans_2026_05_15_ingest_temporal_workflow_fetch_source, docs_superpowers_plans_2026_05_15_ingest_temporal_workflow_parse_and_chunk, docs_superpowers_plans_2026_05_15_ingest_temporal_workflow_extract_kg, docs_superpowers_plans_2026_05_15_ingest_temporal_workflow_merge_and_resolve [EXTRACTED 1.00]
- **Agentic search workflow decomposition** — docs_superpowers_plans_2026_05_25_agentic_search_orchestrator, docs_superpowers_plans_2026_05_25_agentic_search_subquery, docs_superpowers_plans_2026_05_25_agentic_search_global, docs_superpowers_plans_2026_05_25_agentic_search_community_build [EXTRACTED 1.00]
- **LLM gating + response-cache stack** — docs_superpowers_plans_2026_06_06_llm_pool_consolidation_llmpool, docs_superpowers_plans_2026_06_06_llm_pool_consolidation_boundedllm, docs_superpowers_plans_2026_05_18_redis_llm_cache_cachedllm, docs_superpowers_plans_2026_05_18_llm_cache_litellm_redis [INFERRED 0.75]
- **Analytical Layer program (Waves 0-3)** — docs_superpowers_plans_2026_06_28_analytical_layer_wave0_plan, docs_superpowers_plans_2026_06_29_analytical_layer_wave1_plan, docs_superpowers_plans_2026_06_30_analytical_layer_wave2_plan, docs_superpowers_plans_2026_07_01_analytical_layer_wave3_plan [EXTRACTED 0.95]
- **Deploy redesign program (Batches 1-3)** — docs_superpowers_plans_2026_06_16_deploy_redesign_batch1_plan, docs_superpowers_plans_2026_06_16_deploy_redesign_batch2_plan, docs_superpowers_plans_2026_06_16_deploy_redesign_batch3_plan [EXTRACTED 0.95]
- **Analytics activation runbooks (local/prod/rollout)** — docs_superpowers_plans_2026_07_01_analytics_activation_local_plan, docs_superpowers_plans_2026_07_01_analytics_activation_prod_plan, docs_superpowers_plans_2026_07_01_analytics_activation_rollout_plan [EXTRACTED 0.90]
- **Nebula backend migration seams (read / build / summarize)** — docs_superpowers_plans_2026_07_10_nebula_read_slice_plan_graphretriever_for_store, docs_superpowers_plans_2026_07_11_nebula_community_build_communitywriteback_seam, docs_superpowers_plans_2026_07_11_nebula_community_summarize_communitysummarize_seam [INFERRED 0.75]
- **Milvus opt-in vector-store seam pattern (er_vec + report_vec)** — docs_superpowers_plans_2026_07_10_er_vec_milvus_plan_entityvectorstore_seam, docs_superpowers_plans_2026_07_10_er_vec_milvus_plan_entity_er_vec_collection, docs_superpowers_plans_2026_07_10_report_vec_milvus_plan_communityreportvectorstore_seam [INFERRED 0.85]
- **Telegram ingest extensions threaded through tg_ingest post_ingest** — docs_superpowers_plans_2026_07_21_channel_groups_group_enum, docs_superpowers_plans_2026_07_22_tg_reingest_command_reingest_channels, docs_superpowers_plans_2026_07_23_channel_message_stats_source_columns [INFERRED 0.75]
- **Backend-dispatched seam pattern (Protocol + Neo4j-verbatim + Nebula-nGQL + build_* dispatch) across the NebulaGraph migration** — docs_superpowers_specs_2026_07_11_nebula_analytics_connections_design_analytics_graph_ops, docs_superpowers_specs_2026_07_11_nebula_community_build_design_community_writeback, docs_superpowers_specs_2026_07_11_nebula_entity_resolution_design_er_graph_ops, docs_superpowers_specs_2026_07_10_er_vec_milvus_design_entity_vector_store [INFERRED 0.85]
- **Graph-analytics product suite: compute primitives + event detection + actionable signals** — docs_superpowers_specs_2026_06_24_analytical_layer_design, docs_superpowers_specs_2026_06_25_event_detection_design, docs_superpowers_specs_2026_06_25_actionable_signals_design [EXTRACTED 0.90]
- **Community lifecycle on Nebula: BUILD → SUMMARIZE → READ backend-dispatched seams** — docs_superpowers_specs_2026_07_11_nebula_community_build_design_community_writeback, docs_superpowers_specs_2026_07_11_nebula_community_summarize_design_community_summarize, docs_superpowers_specs_2026_07_11_nebula_community_read_design_community_read [EXTRACTED 0.90]
- **NebulaGraph migration design specs (2026-07-11 batch)** — docs_superpowers_specs_2026_07_11_nebula_graph_compute_read_design_spec, docs_superpowers_specs_2026_07_11_nebula_ingest_batch_design_spec, docs_superpowers_specs_2026_07_11_nebula_wiki_ops_design_spec [INFERRED 0.85]
- **doc_group used in search filter, rerank, and synthesis** — docs_superpowers_specs_2026_07_21_channel_groups_design_group_search_filter, docs_superpowers_specs_2026_07_21_channel_groups_design_group_weights, docs_superpowers_specs_2026_07_21_channel_groups_design_synthesis_group_context [EXTRACTED 1.00]
- **Shared AsyncPostgres aggregation consumed by HTTP route and CLI** — docs_superpowers_specs_2026_07_23_channel_message_stats_design_status_counts_by, docs_superpowers_specs_2026_07_23_channel_message_stats_design_stats_router, docs_superpowers_specs_2026_07_23_channel_message_stats_design_message_stats_cli [EXTRACTED 1.00]

## Communities (365 total, 32 thin omitted)

### Community 0 - "test_workflow: test_search_community.py"
Cohesion: 0.04
Nodes (83): DetectCommunitiesParams, DetectCommunitiesResult, DetectedCommunity, Input to ``detect_communities_activity`` — GDS Leiden detection.      ``min_size, Slim cross-activity handle the build workflow fans out over to     summarise.  D, Output of ``detect_communities_activity`` — the communities to     summarise.  E, Input to ``summarize_community_activity`` — summarise ONE     community's member, Output of ``summarize_community_activity`` — the summary text and     whether it (+75 more)

### Community 1 - "test_graph: entity_vid()"
Cohesion: 0.04
Nodes (71): entity_vid(), Stable 128-bit VID as a 32-char hex string from an entity name.      read/write, _FakeNode, _FakePath, _FakeRel, _FakeVal, _NebulaRaisingStore, _NebulaRecStore (+63 more)

### Community 2 - "test_graph: test_lightrag_parse.py"
Cohesion: 0.05
Nodes (81): _clean_raw_name(), _correct_entity_label(), _display_entity_name(), drop_unsupported_dates(), ensure_orphan_entities(), _first_keyword(), _normalize_entity_name(), _normalize_polarity() (+73 more)

### Community 3 - "workflow: get_llm_pool()"
Cohesion: 0.04
Nodes (62): main(), Remove orphaned ``kb-staging/{workflow_run_id}/`` prefixes from MinIO.  Workflow, build_entity_vector_store(), Neo4jEntityVectorStore, Any, Dispatch: nebula (or the opt-in flag) -> Milvus; else Neo4j native., Wraps the existing in-graph ER vector index (unchanged behavior)., dedup_cross_channel_events() (+54 more)

### Community 4 - "test_ingestion: test_translate_transform.py"
Cohesion: 0.05
Nodes (65): DocumentTranslateTransform, _drop_doc_translation_metadata(), _exclude_doc_translation_metadata(), _looks_russian(), Any, BaseNode, TransformComponent, Per-chunk translation step that normalises ingest input to Russian without losin (+57 more)

### Community 5 - "test_make_env.py"
Cohesion: 0.07
Nodes (70): Line, Blank, build_reference(), build_values(), Comment, EnvVar, gen_secret(), _int() (+62 more)

### Community 6 - "retrieval: BoundedLLM"
Cohesion: 0.05
Nodes (46): AbstractAsyncContextManager, Lane, LLMPool, Any, LLM, LLMRole, Test hook - drop the singleton so the next get_llm_pool rebuilds., A named counting async gate: bounded concurrency + an in_use counter.      Usabl (+38 more)

### Community 7 - "test_workflow: graph_admin.py"
Cohesion: 0.06
Nodes (56): CentralityIn, LinkPredictionIn, MaterializeParams, MaterializeResult, RiskIn, StageResult, graph_components(), graph_pagerank() (+48 more)

### Community 8 - "test_graph: test_communities.py"
Cohesion: 0.05
Nodes (63): _coarsest_from_rows(), detect_communities(), detect_hierarchy(), _group_by_levels(), _leiden_stream_cypher(), members_hash(), _project_cypher(), _projection_stats() (+55 more)

### Community 9 - "workflow: orchestrator.py"
Cohesion: 0.07
Nodes (58): Context, _bounds_or_error(), _global_params(), kb_analyze(), kb_auto_search(), kb_drift_search(), kb_global_search(), kb_search() (+50 more)

### Community 10 - "test_workflow: IngestParams"
Cohesion: 0.08
Nodes (53): Event, main(), RabbitMQ consumer for the ingest queue (Track B).  Replaces the ``IngestSchedule, ``python -m src.ingest_queue.consumer`` process entrypoint., Connect, set prefetch=K, and consume ``ingest.pending`` until     ``stop_event``, run_consumer(), build_minio_storage(), Module-level singleton.  Calls `ensure_bucket()` on first build     so the rest (+45 more)

### Community 11 - "test_api: search_v2.py"
Cohesion: 0.07
Nodes (54): _global_params(), _local_params(), _outcome_to_response(), `POST /api/v1/search/local` — plan-execute-synthesize search (R2).  Submits ``Se, doc_id list → relative download links (preserves order)., Map the workflow's ``SearchOutcome`` onto the shared response shape     (identic, Run ``GlobalSearchWorkflow`` — map-reduce over the R6 community     summaries fo, Run ``DriftSearchWorkflow`` — local plan-execute pass first, then     expand wit (+46 more)

### Community 12 - "test_ingestion: extract_identifiers()"
Cohesion: 0.08
Nodes (62): extract_identifiers(), Run every detector on ``text``; return matches sorted by span.      Multiple occ, _by_type(), Unit tests for ``src/ingestion/identifiers.py``.  Coverage goals:   * One happy-, Body-text 'no symptoms' / 'no warranties' must not match —     the regex earlier, `No. SYMPTOMS` should not match — captured token has no digit., Legit `No. 17-K` style references must still extract., test_amount() (+54 more)

### Community 13 - "test_workflow: contracts.py"
Cohesion: 0.07
Nodes (47): DeliverIn, DeliverResult, _Frozen, MonitorIn, MonitorResult, BaseModel, Frozen wire types for the analytical layer.  Mirrors the style of src/workflow/c, SweepResult (+39 more)

### Community 14 - "workflow: contracts.py"
Cohesion: 0.07
Nodes (54): Pull out small JSON-serialisable samples of what the extractor     emitted.  The, _summarise_kg(), finalize(), mark_failed(), mark_skipped(), _persist_ingest_metrics(), `finalize` (success path) and `mark_failed` (workflow-level on-failure) — write, Classifier said skip: write the ``skipped`` terminal status +     reason and cle (+46 more)

### Community 15 - "storage: push_entities()"
Cohesion: 0.06
Nodes (45): _load_cache_from_neo4j(), main(), Operator-side smoke for :mod:`src.storage.wikibase`.  Pushes a tiny fake corpus, Pull cached base-class QIDs + property PIDs from Neo4j., Self-hosted Wikibase populator settings.      When ``enabled=True``, ``DocumentI, WikibaseSettings, AsyncWikibase, _build_claim() (+37 more)

### Community 16 - "eval: test_scale_bench_smoke.py"
Cohesion: 0.06
Nodes (54): bench_cost_curve(), bench_milvus_vs_native(), bench_native_vs_milvus(), bench_native_vs_window(), _driver(), _load(), _milvus_client(), ndarray (+46 more)

### Community 17 - "ingestion: identifiers.py"
Cohesion: 0.06
Nodes (57): _account_control_ok(), _canonicalize_contract(), _check_inn_10(), _check_inn_12(), _check_ogrn_13(), _check_ogrn_15(), _extract_amounts(), _extract_bank_accounts() (+49 more)

### Community 18 - "test_api: AsyncPostgres"
Cohesion: 0.06
Nodes (37): download_document(), FromDishka, `GET /api/v1/documents/{doc_id}` — download the original source file.  Streams t, AsyncPostgres, Any, AsyncConnection, UUID, Async Postgres client for the documents table.  Tracks ingestion-job state acros (+29 more)

### Community 19 - "graph: analysis.py"
Cohesion: 0.07
Nodes (53): components(), _components_from_edges(), _components_nebula(), graph_stats(), _graph_stats_nebula(), pagerank(), _pagerank_cypher(), _pagerank_nebula() (+45 more)

### Community 20 - "graph: _q()"
Cohesion: 0.07
Nodes (15): _nebula_fail_soft(), NebulaAnalyticsGraphOps, Any, Backend-dispatched analytics "connections" graph ops (read-only, fail-soft neigh, Mirrors ``Neo4jAnalyticsGraphOps._rows``'s ``try/except -> []`` (same     warnin, nGQL connections graph ops: GO/FETCH for neighbourhood reads,     FIND SHORTEST, FETCH name+label for a set of Entity vids -> {vid: (name, label)}., Extract ordered node names + edge rel-types from a         ``PathWrapper``-shape (+7 more)

### Community 21 - "test_graph: test_wiki_graph_ops.py"
Cohesion: 0.06
Nodes (46): Records structured_query(cypher, param_map) calls; returns [] (or a     canned v, Fake NebulaGraphStore: records structured_query(q) statements; returns     a can, _RecSession, _RecStore, test_dispatch_returns_nebula_when_backend_nebula(), test_dispatch_returns_neo4j_when_backend_not_nebula(), test_nebula_merge_community_inserts_vertex_and_member_edges(), test_nebula_merge_subcommunity_adds_parent_of_edge() (+38 more)

### Community 22 - "test_graph: NebulaGraphStore"
Cohesion: 0.06
Nodes (29): build_nebula_graph_store(), _chunks(), _is_session_dead(), NebulaGraphStore, Any, NebulaGraph implementation of the KbGraphStore seam (write path).  Phase 1 scope, Bounded GET SUBGRAPH from `vid`, mapped to the shape         GraphRetriever._map, Map a nebula3 ResultSet to a list of column->value dicts. (+21 more)

### Community 23 - "test_graph: merge_kg_extraction()"
Cohesion: 0.10
Nodes (52): _cypher_safe_label(), Convert a free-text predicate/keyword to a Cypher-safe upper-case     relation l, _EntityAgg, _id_to_name(), _maybe_summarize_descriptions(), merge_kg_extraction(), Any, BaseNode (+44 more)

### Community 24 - "test_storage: ChunkRepository"
Cohesion: 0.08
Nodes (42): ChunkRepository, _escape(), _normalise_chunk_row(), Any, Path, Doc-id / file-path access layer for chunks and source files.  Wraps three lower-, Return on-disk path for the source file of `doc_id` or         None if the docum, Read the source file from disk, capped to `max_chars`.          Returns None if (+34 more)

### Community 25 - "workflow: test_search_drift_roundtrip.py"
Cohesion: 0.06
Nodes (43): Compose the final answer over accumulated context., synthesize_answer(), DocumentsForCommunitiesParams, DocumentsForCommunitiesResult, MapPartialResult, Input to the ``synthesize_answer`` activity., Output of ``synthesize_answer``., Output of ``map_community_partial`` — the per-community partial     answer + a s (+35 more)

### Community 26 - "test_graph: test_event_extract.py"
Cohesion: 0.08
Nodes (46): events_to_graph(), EntityNode, Relation, Convert `ParsedEvent` objects → graph nodes + edges.  Called from `LightRAGExtra, Convert a list of ``ParsedEvent`` objects to graph nodes + relations.      Param, ParsedEvent, Intermediate parsed event tuple., _ev_line() (+38 more)

### Community 27 - "config.py: Settings"
Cohesion: 0.05
Nodes (27): BaseSettings, AnalyticsSettings, BotSettings, ClassifierSettings, EventsSettings, GraphSettings, IngestionSettings, MetricsSettings (+19 more)

### Community 28 - "test_graph: MilvusEntityVectorStore"
Cohesion: 0.07
Nodes (24): _emb(), main(), EntityCandidate, EntityVectorStore, _btrunc(), MilvusEntityVectorStore, Any, Truncate to fit a Milvus VARCHAR: max_length counts UTF-8 BYTES, not chars. (+16 more)

### Community 29 - "graph: signals.py"
Cohesion: 0.07
Nodes (26): circular_ownership(), CircularOwnershipParams, investigate_next(), InvestigateNextParams, _Params, Any, BaseModel, P2 — composite, decision-ready signals & queues (read materialized scores + comp (+18 more)

### Community 30 - "test_graph: resolve_entities()"
Cohesion: 0.08
Nodes (40): _apply_name_map(), BaseNode, Relation, Run ER over already-merged entities.      Returns:       * `resolved_entities`:, Rewrite chunk-level KG_NODES_KEY entity names AND merged     relations to use ca, resolve_entities(), _UnionFind, _EmbeddingStub (+32 more)

### Community 31 - "test_workflow: activities.py"
Cohesion: 0.09
Nodes (32): AnalysisPlan, ExecInput, PlanInput, PrimitiveCall, SynthInput, SynthResult, assemble_provenance(), Deterministic provenance assembly (no LLM). (+24 more)

### Community 32 - "test_bot: Turn"
Cohesion: 0.10
Nodes (36): RewriteFn, Wire the pure ``rewrite_query`` to the app's LLM (litellm via build_llm).  Kept, answer_question(), SearchFn, Produce the bot's reply for one incoming message.      Denied users get ``DENIED, build_rewrite_prompt(), Rewrite a follow-up message into a standalone search query.  "а что по нему?" af, Assemble the rewrite prompt from prior turns + the new question. (+28 more)

### Community 33 - "test_analytics: _FakeStore"
Cohesion: 0.08
Nodes (29): _FakeStore, Captures the last Cypher + params and returns canned rows., test_alerts_clamps_top_n(), test_alerts_filters_passed_as_params(), test_alerts_no_window_means_since_null(), test_alerts_reads_alert_nodes_newest_first(), test_alerts_window_days_sets_since(), test_link_prediction_reads_edges() (+21 more)

### Community 34 - "test_storage: test_ingest_metrics.py"
Cohesion: 0.07
Nodes (34): AsyncConnectionPool, AsyncIngestMetrics, build_ingest_metrics_store(), MetricRow, AsyncConnection, BaseModel, Async Postgres wrapper for the ``ingest_metrics`` table.  Owned by the analytics, Factory consistent with the rest of ``src/storage/``. (+26 more)

### Community 35 - "graph: entity_resolution.py"
Cohesion: 0.08
Nodes (41): ADR-0005 Deterministic identifier canonicalization before LLM, ADR-0007 Entity Resolution (candidates + LLM judge + verdict cache + union-find), ADR-0008 Optional native-vector kNN ER over 5000-row window, _candidate_pairs(), _consolidate_cluster(), _cosine(), _embed_entities(), _format_cluster_prompt() (+33 more)

### Community 36 - "graph: test_community_summarize.py"
Cohesion: 0.07
Nodes (21): build_community_summarize(), CommunitySummarize, NebulaCommunitySummarize, Neo4jCommunitySummarize, Any, Protocol, Backend-dispatched community SUMMARIZE I/O (context reads + report write).  `Neo, Runs the historical Cypher constants verbatim — zero behaviour change. (+13 more)

### Community 37 - "graph: clamp_top_n()"
Cohesion: 0.08
Nodes (24): clamp_top_n(), Clamp a requested row cap into ``[1, hard_max]``; ``None``/<=0 → default., contradictions(), ContradictionsParams, incomplete_entities(), IncompleteEntitiesParams, merge_candidates(), MergeCandidatesParams (+16 more)

### Community 38 - "test_workflow: rerank.py"
Cohesion: 0.08
Nodes (37): Input to the ``rerank_sources`` activity (Search R5).      The merged graph+vect, Output of ``rerank_sources`` — the reranked top-N pool., RerankParams, RerankResult, apply_group_weights(), prepare_rerank_pool(), ``rerank_sources`` activity — unified graph+vector rerank (R5).  Before the sing, Build the unified pool fed to the cross-encoder.      A chunk may surface from B (+29 more)

### Community 39 - "analytics: PrimitiveResult"
Cohesion: 0.15
Nodes (40): Primitive, PrimitiveResult, count_entities(), count_relationships(), CountEntitiesParams, CountRelationshipsParams, distribution_by_polarity(), distribution_by_relation_type() (+32 more)

### Community 40 - "analytics: Claim"
Cohesion: 0.11
Nodes (37): cluster_claims(), cosine(), detect_contradictions_clustered(), Claim, EmbedFn, Semantic claim clustering (hybrid method B, iteration 3).  Claims from different, Greedy single-pass clustering of claims by slot-embedding similarity.     Each c, Cluster claims semantically, then flag clusters where sources disagree. (+29 more)

### Community 41 - "test_graph: test_nebula_schema.py"
Cohesion: 0.08
Nodes (27): ensure_schema(), _execute_with_retry(), _migrate_related_validity_to_string(), _probe_edge_write_ready(), _probe_tag_write_ready(), Any, NebulaGraph schema for the KB graph (nGQL DDL).  Mirrors the Neo4j model: `:__En, # NOTE: `entity_wiki_dirty_idx` (on the ALTER-added `wiki_dirty` column) is (+19 more)

### Community 42 - "test_workflow: test_search_global.py"
Cohesion: 0.08
Nodes (30): CommunitySummaryRef, MapPartialParams, One community's stored summary — the unit the global MAP step     produces a par, Input to ``map_community_partial`` — produce a partial answer for     ONE commun, is_relevant_partial(), map_community_partial(), kNN over the community report vectors → ``CommunitySummaryRef``s for     ``level, Pure: did the MAP model return a usable partial (not the 'НЕТ'     refusal)?  To (+22 more)

### Community 43 - "test_workflow: Ctx"
Cohesion: 0.11
Nodes (34): index_vector(), inject_canonical(), parse_and_chunk(), _scrub(), Ctx, Parsed, `index_vector` loads nodes from staging, scrubs Milvus-oversized metadata, inser, Chunks must carry `doc_id` so they can be fetched back by     document id (get_c (+26 more)

### Community 44 - "test_workflow: test_search_orchestrator.py"
Cohesion: 0.12
Nodes (36): OrchestratorParams, Workflow input for ``SearchOrchestratorWorkflow`` — what the     ``/search/local, Plan-execute-synthesize search session (local mode)., Progress snapshot (mirrors SearchWorkflow.get_state)., SearchOrchestratorWorkflow, Retrieve sources for one sub-question — deterministic, no agent., SubQueryRetrievalWorkflow, _exec() (+28 more)

### Community 45 - "test_graph: test_nebula_store_subgraph.py"
Cohesion: 0.07
Nodes (12): _Cell, _Elem, _Node, NebulaGraphStore.subgraph maps GET SUBGRAPH results into _map_walk_rows shape., GET SUBGRAPH frontier vertex: an edge endpoint one step past the last     hop, r, _Rel, _ResultSet, _Session (+4 more)

### Community 46 - "test_graph: LightRAGExtractor"
Cohesion: 0.09
Nodes (34): _extraction_text(), LightRAGExtractor, Any, BaseNode, ChatMessage, TransformComponent, Per-chunk extractor with optional gleaning, LightRAG-style.      Output:       *, Sync entry point.  Forwards to async via asyncio.run.          IMPORTANT: do NOT (+26 more)

### Community 47 - "test_ingestion: test_classifier.py"
Cohesion: 0.10
Nodes (36): apply_rules(), classify_with_llm(), _ext(), LLMVerdict, BaseModel, Path, Input document classifier — decides whether a freshly-fetched document is worth, Deterministic skip rules.  Returns ``skip=True`` + a human reason     for blocke (+28 more)

### Community 48 - "MinioStorage"
Cohesion: 0.09
Nodes (27): BinaryIO, Minio, MinioSettings, S3-compatible upload storage.      User uploads land in `bucket` synchronously f, MilvusClient, MinioStorage, Path, MinIO storage wrapper for user-uploaded documents.  `/api/v1/ingest` writes each (+19 more)

### Community 49 - "mcp: tools_server.py"
Cohesion: 0.09
Nodes (38): _c(), channel_message_stats(), channel_message_timeline(), find_entity_by_id(), find_entity_by_name(), find_neighbours(), _g(), get_chunks_by_doc_id() (+30 more)

### Community 50 - "workflow: SerializedNode"
Cohesion: 0.09
Nodes (33): ``synthesize_answer`` activity — final answer composition.  Plain ResponseSynthe, Prepend the channel group so the synthesis LLM sees each source's     type/trust, with_group_prefix(), Wire-friendly projection of LlamaIndex ``NodeWithScore``.      Only the bits the, SerializedNode, build_reduce_call(), Pure spec for the global REDUCE ``synthesize_answer`` schedule.      Mirrors the, dedup_by_chunk_id() (+25 more)

### Community 51 - "test_graph: test_aggregations_graph_ops.py"
Cohesion: 0.08
Nodes (29): _NebulaRaisingStore, _NebulaRecStore, _RaisingStore, Records nGQL statements (nebula never binds param_map); returns canned     rows, Records (cypher, param_map); returns canned rows popped in call order., _RecStore, test_dispatch_returns_nebula_when_backend_nebula(), test_dispatch_returns_neo4j_when_backend_not_nebula() (+21 more)

### Community 52 - "test_scripts: datetime"
Cohesion: 0.11
Nodes (27): datetime, Keep channels and groups (incl. megagroups); DROP personal chats.      A megagro, select_dialogs(), _FakeDialog, _FakeEntity, _FakeFolder, _FakeMsg, _FakeTitle (+19 more)

### Community 53 - "graph: communities.py"
Cohesion: 0.10
Nodes (33): Neo4j GDS Leiden (gds.leiden.stream), ADR-0009 Hierarchical Leiden communities + structured reports, ADR-0015 Community detection backend — in-worker leidenalg (offload from GDS), _graphscope_rows(), _leiden_rows(), Any, Offline graph-community detection (Search R6, decision C1).  DECOUPLED / OFFLINE, leidenalg backend: stream edges + cluster in-worker (off Neo4j heap).      Retur (+25 more)

### Community 54 - "test_graph: test_index.py"
Cohesion: 0.08
Nodes (34): ExtractorMode, KGExtractor, build_kg_extractor(), ensure_chunk_date_indexes(), ensure_community_indexes(), ensure_community_report_vector_index(), ensure_entity_fulltext_index(), ensure_entity_lookup_indexes() (+26 more)

### Community 55 - "test_config: LiteLLMSettings"
Cohesion: 0.07
Nodes (30): LLMTier, LiteLLMSettings, Any, LLMRole, Connection to a LiteLLM proxy (or any OpenAI-compatible endpoint).      ``llama-, Accept a JSON string (pydantic-settings env), a dict, or an         empty/None v, Merge any provided overrides onto the full default map so a         partial ``ro, Base model for the no-role legacy path: the deprecated         ``llm_model`` if (+22 more)

### Community 56 - "test_graph: test_nebula_store_writes.py"
Cohesion: 0.14
Nodes (32): SimpleNamespace, _Cast, _FakeSession, _node(), _q_expect(), upsert_nodes/upsert_relations emit the expected nGQL (no live DB)., Wraps a plain python value with a nebula-ValueWrapper-like .cast()., Records executed statements. Also answers `FETCH PROP ON `Entity``     read-back (+24 more)

### Community 57 - "api: ingest.py"
Cohesion: 0.08
Nodes (31): FastAPI, X-API-Key auth dependency.  Same simple shared-key model enterprise-kb uses — co, require_api_key(), lifespan(), FastAPI app entry point.  Wires routes, CORS, and the dishka DI container.  Inge, health(), Liveness + dependency health endpoint., Returns 200 when the API process is alive.      Dependency-health pings (Milvus (+23 more)

### Community 58 - "analytics: catalog.py"
Cohesion: 0.08
Nodes (30): Human-readable catalog (name + description + params) for the planner prompt., register(), render_catalog_for_planner(), alerts(), AlertsParams, _Params, Any, BaseModel (+22 more)

### Community 59 - "test_config: test_settings.py"
Cohesion: 0.07
Nodes (23): AgentSettings, ApiSettings, IngestAdmissionSettings, LLMPoolSettings, PostgresSettings, Document-level admission control (always on).  /ingest hands every     document, FastAPI surface — host, port, auth keys, CORS, log level., Knobs for the search endpoints (`/api/v1/search/*`). (+15 more)

### Community 60 - "superpowers: leidenalg/igraph community backend (community_backend flag)"
Cohesion: 0.07
Nodes (35): GraphRAG community system, Hierarchical Leiden detection (detect_hierarchy), Hierarchical communities + dynamic selection plan, Structured community reports (report_vec, incremental), Dynamic community selection (semantic kNN v1 + descent v2), leidenalg/igraph community backend (community_backend flag), Community detection offload plan, Search date filters (Rev 2) plan (+27 more)

### Community 61 - "test_analytics: test_planner.py"
Cohesion: 0.11
Nodes (31): coerce_entity_type(), Map a user/LLM-supplied entity type to its canonical casing, or None     if it i, parse_plan(), plan_query(), Any, NL → AnalysisPlan. Plain achat + tolerant parse + strict pydantic validation.  M, Deterministic fallback for trend/popularity questions the LLM couldn't plan., Call LLM and parse the result into an AnalysisPlan. Fail-open on LLM error. (+23 more)

### Community 62 - "analytics: dynamics.py"
Cohesion: 0.09
Nodes (23): entity_activity(), EntityActivityParams, _iso_to_epoch_days(), _Params, polarity_evolution(), PolarityEvolutionParams, Any, BaseModel (+15 more)

### Community 63 - "test_workflow: merge_and_resolve()"
Cohesion: 0.15
Nodes (34): merge_and_resolve(), Relation, Repoint a relation's name-endpoints through ``alias`` (folded entity     name ->, _rewrite_endpoints(), _base_patches(), _ctx(), _epoch(), _make_entity() (+26 more)

### Community 64 - "test_observability: test_ingest_metrics_extractor.py"
Cohesion: 0.15
Nodes (31): HistoryEvent, parse_activity_timings(), WorkflowHistory, Pure-functional extractor that turns Temporal workflow history into ``MetricRow`, Resolve the right oneof attributes block for a terminal event., Return one ``MetricRow`` per (activity, attempt) found in the     given workflow, _terminal_attrs(), Static map activity_name → LLM role.  Used by ``ingest_metrics_extractor.parse_a (+23 more)

### Community 65 - "graph: events_llm.py"
Cohesion: 0.09
Nodes (18): build_burst_cypher(), E3 — shared burst computation over event created_at (single source for the trend, Parameterized burst query grouped by (participant entity, event_type).      rece, EventDossierParams, EventTimelineParams, _Params, BaseModel, E2 event read primitives — event_dossier + event_timeline. (+10 more)

### Community 66 - "test_graph: test_entity_resolution.py"
Cohesion: 0.07
Nodes (33): _deep_normalize(), _deterministic_pairs(), _initials_signature(), _is_cyrillic_name(), _load_existing_canonicals(), Read Neo4j entities with `er_canonical_name` and their stored     embedding.  Re, Drop combining marks (é → e, ё → е).  Preserves case., Aggressive normalisation for the deterministic pre-pass —     casefold + drop di (+25 more)

### Community 67 - "retrieval: build_vector_index()"
Cohesion: 0.11
Nodes (29): BasePydanticVectorStore, MilvusSettings, main(), _parse_args(), Namespace, CLI: ingest a directory into Milvus.  Usage::      python -m src.ingestion.run ., build_vector_index(), build_vector_store() (+21 more)

### Community 68 - "workflow: worker.py"
Cohesion: 0.09
Nodes (29): Runtime, CommunityBuildWorkflow, Offline detect → summarize community build., DriftSearchWorkflow, Drift mode — local pass, then global community expansion., _build_runtime(), _build_worker(), _child_main() (+21 more)

### Community 69 - "setup_wikibase.py"
Cohesion: 0.10
Nodes (29): _api_url(), _bootstrap_credentials(), _configure_wbi(), _Counter, _ensure_item(), _ensure_property(), _find_entity_by_label(), _identifier_properties() (+21 more)

### Community 70 - "test_workflow: test_search_retrieve.py"
Cohesion: 0.14
Nodes (29): Input to the ``retrieve_subquestion`` activity.      One deterministic retrieval, RetrieveParams, Run the deterministic retrieve pipeline for one sub-question., Pick the top entity_name from a ``graph_search`` observation.      PURE (no I/O), retrieve_subquestion(), top_entity_name(), _DispatchRecorder, _gs_obs() (+21 more)

### Community 71 - "test_retrieval: test_hf_offline.py"
Cohesion: 0.12
Nodes (28): SentenceTransformerRerank, HFSettings, Offline HuggingFace model loading for air-gapped deploys.      Two project model, configure_hf(), Offline HuggingFace cache wiring for air-gapped deploys.  ``configure_hf()`` tra, Set ``os.environ[name]`` only when it is not already set, so an     operator's e, Apply ``settings.hf`` to the HuggingFace env vars (idempotent).      * ``cache_d, _set_if_absent() (+20 more)

### Community 72 - "graph: events.py"
Cohesion: 0.10
Nodes (16): entity_new_connections(), EntityNewConnectionsParams, new_events(), NewEventsParams, _Params, Any, BaseModel, E1 read side — first_seen-based "what's new" primitives. (+8 more)

### Community 73 - "IngestSchedulerWorkflow"
Cohesion: 0.09
Nodes (20): AdmissionState, Pure admission-control state machine (Track 5).  Bounds how many documents inges, FIFO admission with a hard ``max_inflight`` ceiling.      ``submit`` enqueues (d, Pop up to ``free_slots`` documents off the front of the queue         into fligh, IngestSchedulerWorkflow, Recycle (continue_as_new) once event history crosses the         threshold — reg, Still-queued documents handed to the next run on recycle.         In-flight docs, Enqueue a document for admission (dedup by doc_id). (+12 more)

### Community 74 - "ingest_queue: ingest_submit.py"
Cohesion: 0.11
Nodes (26): AbstractRobustConnection, Ingest message priority levels (RabbitMQ backend).  A single work queue declared, close_publisher(), _get_connection(), publish_ingest(), RabbitMQ producer for the ingest queue (Track B).  ``/ingest`` → :func:`submit_d, Return the per-process robust connection, opening it lazily., Publish one document to a configured ingest queue (``queue``,     default the fi (+18 more)

### Community 75 - "test_graph: test_event_merge.py"
Cohesion: 0.13
Nodes (29): _cos(), event_key(), merge_events(), EntityNode, Relation, E2 event de-duplication: deterministic (type, participants, ts-bucket) match-key, Deterministic, order-insensitive match key for an event.      Returns:         `, Collapse event nodes sharing an ``event_key`` into one canonical node.      The (+21 more)

### Community 76 - "observability: trace_request()"
Cohesion: 0.12
Nodes (27): _count_by(), get_current_trace(), Any, Structured tracing for the search endpoints.  A trace is a list of `TraceEvent`, Bind a fresh Trace to the current context for the duration     of the `with` blo, Append an event to the current trace.  Cheap no-op if no     trace is active (al, Convenience: time the wrapped block, append one event., One structured event in the request trace. (+19 more)

### Community 77 - "retrieval: DateBounds"
Cohesion: 0.14
Nodes (26): bounds_from_iso(), date_metadata_filters(), DateBounds, _field_in_range(), filter_nodes(), iso_to_epoch_days(), node_metadata_in_range(), overfetch_top_k() (+18 more)

### Community 78 - "events_eval.py"
Cohesion: 0.12
Nodes (24): build_extraction_llm(), High-volume KG triple extraction + translation (small tier)., EventsExtractor, EventStats, format_report(), _keys_by_type(), _llm_events_extractor_factory(), main() (+16 more)

### Community 79 - "test_api: analyze.py"
Cohesion: 0.14
Nodes (25): main(), Run one analytical query against the knowledge graph.  Usage::      python -m sc, AnalyticsOutcome, AnalyzeParams, Provenance, AnalyticalQueryWorkflow input (epoch-day bounds, like OrchestratorParams)., analyze(), `POST /api/v1/analyze` — plan → compute → synthesize analytical Q&A. (+17 more)

### Community 80 - "storage: backfill_doc_id.py"
Cohesion: 0.14
Nodes (27): _iter_rows(), _load_path_index(), main(), MilvusClient, Backfill the `doc_id` metadata field on legacy Milvus chunks.  Chunks indexed be, Yield Milvus rows in batches via query_iterator (offset-paging has     a 16 384, BackfillStats, build_path_index() (+19 more)

### Community 81 - "reresolve_graph.py"
Cohesion: 0.10
Nodes (26): _amain(), _apply_merges(), _is_write_cypher(), _load_all_entities(), _loader_cypher(), main(), _parse_args(), _plan_merges() (+18 more)

### Community 82 - "tg_ingest.py"
Cohesion: 0.12
Nodes (28): dialog_in_folders(), dialog_slug(), _filter_title(), _message_to_doc(), post_ingest(), Any, TG → ingest harness: enqueue Telegram messages via POST /api/v1/ingest (which up, Backfill: read last-`limit` messages per channel (oldest→newest) and enqueue. (+20 more)

### Community 83 - "graph: communities.py"
Cohesion: 0.11
Nodes (17): community_overview(), CommunityOverviewParams, entity_communities(), EntityCommunitiesParams, _Params, personalized_pagerank(), PersonalizedPagerankParams, Any (+9 more)

### Community 84 - "test_graph: test_rollups_graph_ops.py"
Cohesion: 0.09
Nodes (16): build_rollups_graph_ops(), NebulaRollupsGraphOps, Neo4jRollupsGraphOps, Any, Protocol, Backend-dispatched analytics "rollups" graph op (numeric Amount rollup, read-onl, RollupsGraphOps, _NebulaRecStore (+8 more)

### Community 85 - "build_graph_store()"
Cohesion: 0.11
Nodes (22): build_graph_store(), build_neo4j_graph_store(), _construct_neo4j_graph_store(), _install_query_logging(), _neo4j_driver_kwargs(), PropertyGraphStore, Graph-store factory.  Two flavours:   * Neo4j — production / live-stack path; us, Return the process-global Neo4j graph store, building it once.      Lazily const (+14 more)

### Community 86 - "eval: score_case()"
Cohesion: 0.15
Nodes (27): _contains(), _content_words(), GoldenCase, load_golden_cases(), _norm(), Path, Answer-quality eval primitives (R9).  Given a golden Q&A case and a `SearchRespo, Split on sentence terminators; cheap and good-enough for eval. (+19 more)

### Community 87 - "eval: test_medical_fixture.py"
Cohesion: 0.13
Nodes (26): _entity_candidates(), _evidence_phrases(), load_medical_golden_cases(), load_medical_qas(), MedicalQA, Loader for the Medical benchmark corpus.  The corpus comes from `tests/eval/corp, Return parsed Q&A entries, optionally filtered & sampled.      `limit=None`: ret, Extract substring-matchable medical keywords from     `evidence_relations`. (+18 more)

### Community 88 - "graph: MilvusCommunityReportVectorStore"
Cohesion: 0.13
Nodes (16): _emb(), main(), build_community_report_vector_store(), CommunityRef, CommunityReport, CommunityReportVectorStore, MilvusCommunityReportVectorStore, Any (+8 more)

### Community 89 - "graph: domain.py"
Cohesion: 0.11
Nodes (16): communication_stats(), CommunicationStatsParams, issue_resolution_stats(), IssueResolutionStatsParams, _Params, Any, BaseModel, P3 domain rollups — issue/resolution + communication intensity primitives. (+8 more)

### Community 90 - "test_analytics: test_aggregations.py"
Cohesion: 0.18
Nodes (19): _FakeOps, _patch_ops(), The most-mentioned-entities surface must not let degenerate names     (numerish,, Records (method, args) calls; returns canned rows per method name., test_count_entities_excludes_identifiers_by_default(), test_count_entities_failsoft(), test_count_entities_routes_through_seam(), test_count_relationships_filters_rel_type_and_polarity() (+11 more)

### Community 91 - "test_analytics: _FakeOps"
Cohesion: 0.16
Nodes (16): _FakeOps, _patch_ops(), Records (method, args) calls; returns canned rows per method name., test_common_connections_routes_through_seam(), test_connection_path_clamps_hops_inline(), test_connection_path_routes_through_seam_with_clamped_hops(), test_cooccurrence_routes_through_seam(), test_cooccurrence_via_shared_chunks() (+8 more)

### Community 92 - "bot: __main__.py"
Cohesion: 0.11
Nodes (22): AnswerFn, Classifier, classify_intent(), Route a question to the analytical layer (graph aggregation via /analyze) or the, ANALYTICAL for aggregation/statistics/contradiction questions, else SEARCH., make_rewrite(), Build an async ``rewrite(history, question) -> standalone_query`` backed     by, _api_key() (+14 more)

### Community 93 - "graph: centrality.py"
Cohesion: 0.11
Nodes (15): link_prediction(), LinkPredictionParams, _Params, Any, BaseModel, Family 3 heavy tier (offline-materialized reads): centrality + link prediction., top_central_entities(), TopCentralParams (+7 more)

### Community 94 - "test_ingestion: IdentifierCanonicalizationTransform"
Cohesion: 0.12
Nodes (25): _description_for(), IdentifierCanonicalizationTransform, inject_canonical_entities(), PropertyGraphStore, TransformComponent, Cut ±`window` chars around the identifier span; collapse whitespace., Push canonical identifier nodes into the property-graph store.      Reads `canon, Augment each chunk with canonical identifiers.      Pure transformation — does N (+17 more)

### Community 95 - "workflow: _search_deps.py"
Cohesion: 0.10
Nodes (25): coverage_check(), _parse(), ``coverage_check`` activity — pre-synthesis completeness gate.  After the orches, Judge whether gathered evidence fully covers the query., CoverageParams, Input to the ``coverage_check`` activity.      Asks whether the evidence gathere, _build_chunk_repo_once(), _build_synthesizer_once() (+17 more)

### Community 96 - "test_ingestion: index_vector.py"
Cohesion: 0.15
Nodes (26): _node_content_len(), `index_vector` — embed + Milvus insert.  Loads parsed nodes from staging, snapsh, Strip metadata so every node's ``_node_content`` fits the Milvus     VARCHAR cap, Truncate any chunk whose ``text`` field exceeds the Milvus cap     (a chunking p, Length of the ``_node_content`` VARCHAR Milvus will actually     store for this, _restore_metadata(), _restore_text(), _snapshot_for_milvus() (+18 more)

### Community 97 - "test_graph: resolve()"
Cohesion: 0.18
Nodes (26): Resolved, resolve(), Table-driven tests for the deterministic event-time resolver.  Anchor below = 20, test_bare_month_uses_anchor_year(), test_bare_year(), test_bare_year_implausible_clamped_to_none(), test_day_span_in_month(), test_explicit_dmy_date() (+18 more)

### Community 98 - "test_analytics: test_events_llm.py"
Cohesion: 0.14
Nodes (22): event_dossier(), event_timeline(), Any, Event dossier: core event info + actors., Events a named entity participated in, ordered by resolved start time (untimed l, (entity, event_type) pairs whose event ingest-rate surged recently., trending_events(), _FakeOps (+14 more)

### Community 99 - "workflow: materialize_activities.py"
Cohesion: 0.15
Nodes (23): compute_risk(), normalize(), Pure composite risk scoring (no I/O). Components arrive already normalized to 0., RiskResult, _gather_risk_nebula(), _get_store(), materialize_centrality(), materialize_link_prediction() (+15 more)

### Community 100 - "aggregations_graph_ops.py"
Cohesion: 0.11
Nodes (11): AggregationsGraphOps, _canonical_label(), _list_literal(), _nebula_fail_soft(), NebulaAggregationsGraphOps, Protocol, Backend-dispatched analytics "aggregations" graph ops (read-only, fail-soft coun, Mirrors ``Neo4jAggregationsGraphOps._rows``'s ``try/except -> []``     (same war (+3 more)

### Community 101 - "test_graph: write_entity_article()"
Cohesion: 0.17
Nodes (24): EntityContext, Read an entity's 1-hop subgraph from the graph store and hash it for change dete, Stable sha256 over the entity's facts AND its source-document set.     Order-ind, Distinct source-document ids that mention this entity, sorted.     Used both for, read_citations(), read_entity_subgraph(), read_source_docs(), subgraph_hash() (+16 more)

### Community 102 - "test_graph: test_er_graph_ops.py"
Cohesion: 0.12
Nodes (20): Records (cypher, param_map) calls; returns canned rows per call,     popped in c, Fake nebula store: records nGQL statements (asserts no param_map —     nebula bi, Safety guarantee: if a redirected-edge re-insert fails, the loser     must NOT b, _RecNebula, _RecStore, test_dispatch_returns_nebula_when_backend_nebula(), test_dispatch_returns_neo4j_when_backend_not_nebula(), test_nebula_ensure_verdict_schema_is_noop() (+12 more)

### Community 103 - "SEARCH.md — search subsystem deep reference"
Cohesion: 0.14
Nodes (26): ADR-0002 Claim-check staging via MinIO, ADR-0006 Milvus HNSW as default chunk index, ADR-0010 Dynamic community selection (lexical/semantic/descent, fail-open), ADR-0011 Plan-execute SearchOrchestratorWorkflow (ReAct removed), ADR-0012 Wikibase anchor + continuous anti-drift wiki editor, ADR-0014 Source download via stable API endpoint (not presigned URL), LLM-мониторинг панелей Grafana (PDF report), grafana/mcp-grafana MCP server (+18 more)

### Community 104 - "retrieval: build_llm()"
Cohesion: 0.13
Nodes (23): main(), Deep diagnostic: try every layer of SchemaLLMPathExtractor.  Steps:   1. Plain `, _build(), build_judge_llm(), build_llm(), build_search_llm(), build_synthesis_llm(), _fc_flag() (+15 more)

### Community 105 - "GraphRetriever"
Cohesion: 0.11
Nodes (17): _dedupe_entities(), _dedupe_relations(), _find_by_name_ngql(), GraphRetriever, _parse_triplet_chains(), PropertyGraphIndex, Graph-search wrapper for the agent loop.  Returns a ``RoundGraphData`` with stru, FUZZY (partial) name lookup under nebula — mirrors the neo4j full-text     index (+9 more)

### Community 106 - "mcp: _shared.py"
Cohesion: 0.11
Nodes (24): main(), assert_api_key_env_set(), _auth_required(), build_sse_auth(), is_valid_key(), log_banner(), parse_args(), Any (+16 more)

### Community 107 - "workflow: KGExtracted"
Cohesion: 0.17
Nodes (19): build_property_graph(), _is_neo4j_safe(), `build_property_graph` — Chunk + MENTIONS + entity/relation upsert., _strip_neo4j_unsafe_metadata(), GraphBuildResult, GraphBuilt, KGExtracted, Merged (+11 more)

### Community 108 - "workflow: test_search_route.py"
Cohesion: 0.14
Nodes (20): Input to the ``route_query`` activity — the raw user question., Output of ``route_query`` — the chosen search mode.      Fail-safe: any classifi, RouteParams, RouteResult, classify_route(), _get_route_llm(), ``route_query`` activity — classify a question's search mode (R7a).  Decision C, Map a router LLM reply to a ``RouteResult``.  Pure / unit-testable.      Toleran (+12 more)

### Community 109 - "test_graph: test_events_llm_graph_ops.py"
Cohesion: 0.12
Nodes (19): _NebulaRaisingStore, _NebulaRecStore, _RaisingStore, _RecStore, test_dispatch_nebula(), test_dispatch_neo4j_default(), test_nebula_event_actors_go_then_fetch_names(), test_nebula_event_core_empty_when_not_event() (+11 more)

### Community 110 - "wipe_db.py"
Cohesion: 0.13
Nodes (24): confirm(), main(), _parse_args(), Namespace, DESTRUCTIVE — wipe all data stores.  Drops:   * Temporal   — terminate running +, Drop EVERY collection, not just ``settings.milvus.collection`` —     the stack h, Dispatch on the configured graph backend (mirrors     ``src.graph.store.build_gr, DROP the whole Nebula space.  ``nebula_schema.ensure_schema``     re-creates the (+16 more)

### Community 111 - "test_analytics: test_claim_nli.py"
Cohesion: 0.16
Nodes (21): build_nli_prompt(), nli_verdict(), parse_nli_verdict(), LLM NLI verdict over a pair of claim values (hybrid method B, iteration 4).  Emb, Tolerant parse → CONTRADICT / AGREE / NEUTRAL. contradiction wins over     agree, Ask the LLM for the NLI relation. Fail-open to NEUTRAL on any error., Drop structurally-flagged contradictions that NLI judges to be mere     phrasing, refine_contradictions() (+13 more)

### Community 112 - "graph: community_writeback.py"
Cohesion: 0.12
Nodes (9): build_community_writeback(), _carry_params(), CommunityWriteback, Neo4jCommunityWriteback, Any, Protocol, Backend-dispatched community BUILD write-back (the `:Community` + `IN_COMMUNITY`, Map the clean-keyed carry dict to the `carry_*` params the neo4j     MERGE Cyphe (+1 more)

### Community 113 - "retrieval: GroupFilter"
Cohesion: 0.18
Nodes (21): combined_metadata_filters(), filter_nodes_by_group(), group_metadata_filters(), GroupFilter, node_group_ok(), MetadataFilter, MetadataFilters, Channel-group search filter — the doc_group analogue of date_filters.  `doc_grou (+13 more)

### Community 114 - "storage: AsyncMediaWiki"
Cohesion: 0.14
Nodes (14): AsyncMediaWiki, AsyncClient, Minimal async MediaWiki Action API client (login + read/edit page + sitelink). U, Link a Wikibase Item to its MediaWiki article page. Best-effort., _api_url(), get_mediawiki(), Process-singleton MediaWiki client for wiki activities., _client_returning() (+6 more)

### Community 115 - "test_analytics: config.py"
Cohesion: 0.14
Nodes (17): ADR-0001 Temporal for durable orchestration, ADR-0003 Task-queue isolation (avoid head-of-line blocking), ADR-0004 Per-process LLMPool (tier + role lanes), ADR-0013 Multi-model role/tier selection + ingest_metrics snapshots, QUEUES.md — Temporal task-queue topology, DocumentIngestWorkflow (kb-ingest), extract_kg (KG extraction activity, kb-ingest-llm), GraphBuildWorkflow (merge lane, kb-ingest-merge) (+9 more)

### Community 116 - "superpowers: DocumentIngestWorkflow"
Cohesion: 0.09
Nodes (24): Ingest Temporal Workflow plan, Pydantic v2 workflow contracts, DocumentIngestWorkflow, fetch_source activity, inject_canonical activity, merge_and_resolve activity, parse_and_chunk activity, StagingStore MinIO claim-check (+16 more)

### Community 117 - "build_ingestion_pipeline()"
Cohesion: 0.14
Nodes (22): IngestionCache, IngestionPipeline, _build_cache(), build_ingestion_pipeline(), _build_splitter(), BaseEmbedding, Path, TransformComponent (+14 more)

### Community 118 - "test_scripts: test_tg_ingest_reingest.py"
Cohesion: 0.15
Nodes (16): main(), Choose the run mode from parsed args: reingest wins over the legacy     backfill, select_mode(), _FakeDialog, _FakeEntity, _FakeMsg, iter_messages(entity, limit, reverse) → prepared newest-`limit` msgs., _RecHTTP (+8 more)

### Community 119 - "analytics: materialize.py"
Cohesion: 0.14
Nodes (21): compute_all(), compute_centrality(), Any, In-worker centrality (igraph) over the exported __Entity__ graph.  Mirrors src/g, Return ``{entity_name -> score}`` for ``metric`` over the weighted     undirecte, Stream the graph once, compute every metric. Returns     ``{metric -> {name -> s, Any, Offline GDS compute + write-back into Neo4j. Mirrors src/graph/communities.py. (+13 more)

### Community 120 - "test_graph: extract_entity_edges()"
Cohesion: 0.11
Nodes (16): extract_entity_edges(), Stream the ``__Entity__`` graph out via the backend-dispatched     ``GraphEdgeEx, _FakeExport, _FakeStore, _FakeStoreHighDegreeSource, _FakeStoreNullCursor, Returns one page of node rows, then one page of edge rows, then empties., Regression: all edges from a high-degree source must survive multi-page reads. (+8 more)

### Community 121 - "workflow: contextualize.py"
Cohesion: 0.18
Nodes (19): ContextualizeParams, ContextualizeResult, ConversationTurnDict, Input to the ``contextualize_query`` activity., Standalone, self-contained rewrite of ``query`` (== original on no-op/failure)., _bound_history(), _build_prompt(), contextualize_query() (+11 more)

### Community 122 - "test_workflow: test_article.py"
Cohesion: 0.15
Nodes (21): _fmt_citations(), _fmt_relations(), _fmt_sources(), Bot-section splice + LLM render for entity wiki articles.  The bot owns ONLY the, Replace the marked bot section with `bot_md` (wrapped in markers).     If no mar, Deterministic '== Источники ==' section with download links to the     original, LLM-render the bot section grounded ONLY in `ctx` (graph facts) and     `citatio, render_bot_section() (+13 more)

### Community 123 - "test_graph: test_quality_graph_ops.py"
Cohesion: 0.13
Nodes (17): _NebulaRaisingStore, _NebulaRecStore, _RaisingStore, _RecStore, test_dispatch_nebula(), test_dispatch_neo4j_default(), test_nebula_contradictions_two_match_empty_chunks(), test_nebula_fail_soft_returns_empty_on_raise() (+9 more)

### Community 124 - "test_graph: test_signals_graph_ops.py"
Cohesion: 0.13
Nodes (17): _NebulaRaisingStore, _NebulaRecStore, _RaisingStore, _RecStore, test_dispatch_nebula(), test_dispatch_neo4j_default(), test_nebula_circular_ownership_sorts_by_length(), test_nebula_fail_soft() (+9 more)

### Community 125 - "test_graph: test_alerts.py"
Cohesion: 0.17
Nodes (18): alert_key(), mark_watched(), Arc 2 — Alert store + watchlist Cypher helpers (called off-loop from activities), SET e.watched on __Entity__ nodes by name list; fail-soft on error., Compose a stable dedup key for an Alert node.      Format: ``kind:entity:detail`, MERGE an :Alert node keyed on (kind, entity, detail); fail-soft on error.      W, upsert_alert(), Tests for src/graph/alerts.py — Alert store + watchlist Cypher helpers. (+10 more)

### Community 126 - "test_graph: stamp_first_seen()"
Cohesion: 0.17
Nodes (19): Any, E1 — emulate ON CREATE stamping (created_at/first_doc_id) post-upsert.  The enti, Stamp created_at/first_doc_id on newly-created graph elements.      Sets the fie, stamp_first_seen(), _stamp_first_seen_nebula(), _stamp_first_seen_neo4j(), Tests for E1 — ON-CREATE-emulated first_seen stamping.  Covers the backend dispa, When relations list is empty, only the entity pass fires. (+11 more)

### Community 127 - "test_retrieval: test_graph_walk_retriever.py"
Cohesion: 0.19
Nodes (20): True if a walk-relation dict should be surfaced to the agent.      Drops edges t, _relation_is_live(), _FakeStore, Unit tests for GraphRetriever.awalk — bounded N-hop graph traversal.  Uses a fak, Captures the Cypher + params and returns canned rows., Build a GraphRetriever without touching PropertyGraphIndex., _rel(), _retriever_with_store() (+12 more)

### Community 128 - "workflow: global_search.py"
Cohesion: 0.12
Nodes (22): MapCommunitiesResult, Output of ``map_communities`` — the community summaries to map     over.  Empty, _descent_children(), _descent_root(), _embed_query(), _get_embed_model(), _get_map_llm(), _get_store() (+14 more)

### Community 129 - "workflow: StagingStore"
Cohesion: 0.12
Nodes (16): _parse_uri(), Any, Find and delete orphaned ``{run_id}/`` prefixes.  Returns         the list of ru, Thin wrapper around the MinIO client for stage blobs., Pickle `obj` and upload to ``{run_id}/{stage}.pkl``.          Returns the full `, Reverse of `write_pickle`., Best-effort cleanup of every blob under ``{run_id}/``., Return ``run_id`` prefixes whose newest blob is older than         ``older_than_ (+8 more)

### Community 130 - "eval: identifier_recall.py"
Cohesion: 0.15
Nodes (20): check_thresholds(), evaluate_case(), format_report(), _is_extra(), load_cases(), main(), _match(), _parse_args() (+12 more)

### Community 131 - "retrieval: test_answer_template.py"
Cohesion: 0.14
Nodes (19): build_query(), load_template(), Server-side answer templates (Track 6, variant a).  Lets a caller shape the SHAP, Russian-output instruction (the default when no template is set)., Resolve a template.  A bare safe name that matches a file under     ``prompts/an, Compose the synthesis instruction.  No template → the RU preamble;     otherwise, ru_query(), build_synthesize_call() (+11 more)

### Community 132 - "test_retrieval: _StubGraphRetriever"
Cohesion: 0.15
Nodes (19): find_entity_by_id(), graph_search(), graph_walk(), Knowledge-graph traversal: matched entities + their neighbours up     to ``depth, Bounded multi-hop graph traversal from a known entity.      Unlike ``graph_searc, Exact lookup by canonical name (E.164 phone, INN, email …)., _StubGraphData, _StubGraphRetriever (+11 more)

### Community 133 - "test_workflow: CoverageResult"
Cohesion: 0.20
Nodes (19): CoverageResult, Output of ``coverage_check``.      ``complete`` — is the gathered evidence suffi, build_evidence(), Pure coverage-gate helpers for the plan-execute flow (R4).  Extracted as plain f, Join merged source texts into one bounded evidence blob for the     coverage jud, Decide whether to issue ONE more sub-question after a coverage check.      Retur, should_run_coverage_round(), _n() (+11 more)

### Community 134 - "ner_eval.py"
Cohesion: 0.16
Nodes (16): _accumulate_lang(), Extractor, format_report(), _llm_only_extractor_factory(), main(), NERStats, _norm(), Path (+8 more)

### Community 135 - "test_api: test_ingest.py"
Cohesion: 0.19
Nodes (21): _api_key_header(), ASGI tests for `POST /api/v1/ingest`.  The MinIO client, Postgres, and the Tempo, MinIO container fully down: the SDK raises a urllib3     `MaxRetryError`, not an, Reuse policy on the Temporal side raises     ``WorkflowAlreadyStartedError`` whe, Admission is always-on: every upload MUST route through the singleton     Ingest, A valid `document_date` is converted to epoch-days and snapshotted     onto Inge, A malformed `document_date` is rejected with 422 before any     upload / Postgre, rabbitmq backend: an unconfigured `queue` is rejected with 422     before any up (+13 more)

### Community 136 - "test_graph: test_domain_graph_ops.py"
Cohesion: 0.13
Nodes (15): _NebulaRaisingStore, _NebulaRecStore, _RaisingStore, Returns canned rows keyed by first matching substring of the statement., _RecStore, test_dispatch_nebula(), test_dispatch_neo4j_default(), test_nebula_comms_adds_name_filter() (+7 more)

### Community 137 - "test_graph: test_dynamics_graph_ops.py"
Cohesion: 0.14
Nodes (15): _NebulaRaisingStore, _NebulaRecStore, _RaisingStore, _RecStore, test_dispatch_nebula(), test_dispatch_neo4j_default(), test_nebula_fail_soft(), test_nebula_polarity_evolution_optional_filters() (+7 more)

### Community 138 - "test_scripts: test_reresolve_graph.py"
Cohesion: 0.12
Nodes (6): Unit tests for the pure / stubbed helpers in scripts/reresolve_graph.py.  The wh, Inner stub that records every structured_query call and exposes     an arbitrary, _RecordingStore, test_proxy_delegates_arbitrary_attribute(), test_proxy_noops_write_without_touching_inner(), test_proxy_passes_read_through_to_inner()

### Community 139 - "diagrams: activity: retrieve_subquestion (hybrid)"
Cohesion: 0.12
Nodes (21): 3b · coverage gap? — coverage_check (small LLM) decision, extra SubQueryRetrievalWorkflow(gap) — budget max_coverage_rounds (1), dedup by chunk_id, find_entity_by_name — full-text entity lookup, graph_search — path_depth = graph_search_path_depth (1), matched entities + neighbours + chunks, graph_walk (seed) — start = top graph_search entity, hops = graph_walk_hops (2, ≤3), FAIL-OPEN, MCP-1 tool: kb_search(query, max_refinements=3) — @mcp.tool timeout=1800s, submits workflow, polls get_state, MCP client (Hermes / OpenWebUI) (+13 more)

### Community 140 - "superpowers: WikiGraphOps Protocol (dirty bookkeeping, subgraph,"
Cohesion: 0.12
Nodes (21): build_graph_edge_export (backend dispatch nebula vs neo4j), detect_communities (leidenalg end-to-end under nebula), extract_entity_edges (routes through GraphEdgeExport seam), GraphEdgeExport Protocol (stream_names/stream_edges seam), NebulaGraphEdgeExport (LOOKUP names + batched GO over RELATED, reads weight), Neo4jGraphEdgeExport (keyset Cypher pagination, byte-for-byte), RELATED weight column (mention_count-derived, weighted Leiden parity), Nebula graph-compute read design (extract_entity_edges + RELATED weight) (+13 more)

### Community 141 - "test_graph: index.py"
Cohesion: 0.14
Nodes (16): main(), _pending(), One-time E1 backfill: stamp a sentinel ``created_at`` on pre-existing graph elem, ensure_first_seen_indexes(), _parse_triplets_strip_thinking(), `PropertyGraphIndex` factory and KG extractor wiring.  Layers:   * **Extractor**, Idempotently create the E1 temporal indexes: ``created_at`` on     entities + pe, Wrap the upstream parser with a `<think>...</think>` stripper.      Qwen3 emits (+8 more)

### Community 142 - "test_retrieval: test_query_planner.py"
Cohesion: 0.18
Nodes (19): decompose(), _parse_subquestions(), LLM, Query decomposition for the plan-execute search flow (R2).  A compound question, Parse the planner's reply into a list of sub-questions.      Tolerant of three s, Split ``question`` into ≤``max_subqueries`` sub-questions.      Returns ``[quest, _strip_marker(), Unit tests for the query planner (R2 plan-execute flow).  Stubs the LLM so the s (+11 more)

### Community 143 - "test_workflow: MapCommunitiesParams"
Cohesion: 0.17
Nodes (16): MapCommunitiesParams, Input to the ``map_communities`` activity — fetch community     summaries to map, map_communities(), Select the community summaries to map over.      Strategy switch on ``params.sel, Temporal activities for the plan-execute search subsystem (R2).  ``SEARCH_V2_ACT, _FakeEmbed, _FakeStore, test_map_communities_descent_empty_falls_back_to_lexical() (+8 more)

### Community 144 - "test_graph_edge_export.py"
Cohesion: 0.15
Nodes (16): Fake nebula store: records nGQL (asserts inline, no param_map);     returns cann, Records (cypher, param_map) calls; returns canned pages per call,     popped in, _RecNebula, _RecStore, test_dispatch_returns_nebula_when_backend_nebula(), test_dispatch_returns_neo4j_when_backend_not_nebula(), test_nebula_stream_edges_chunks_go_calls_by_batch_size(), test_nebula_stream_edges_names_none_falls_back_to_internal_stream_names() (+8 more)

### Community 145 - "test_ingest_queue: RabbitMQSettings"
Cohesion: 0.15
Nodes (15): AbstractChannel, AbstractQueue, RabbitMQSettings, RabbitMQ ingest-queue connection (Track B).      Only consumed when ``INGEST_QUE, Allow a comma-separated env string (RABBITMQ_QUEUES=a,b) as well         as a JS, Queue used when /ingest doesn't name one (the first configured)., declare_ingest_topology(), RabbitMQ topology for the ingest queue (Track B).  Declared (idempotently) by BO (+7 more)

### Community 146 - "test_retrieval: test_hybrid.py"
Cohesion: 0.15
Nodes (18): BaseRetriever, BM25Retriever, build_bm25_retriever(), build_hybrid_retriever(), BaseNode, LLM, VectorStoreIndex, Hybrid retrieval — BM25 + dense vector + RRF fusion.  NOT wired into the active (+10 more)

### Community 147 - "diagrams: DocumentIngestWorkflow (IngestParams to IngestResult, queue"
Cohesion: 0.14
Nodes (20): build_property_graph: PropertyGraphIndex Neo4j upsert (chunks, MENTIONS, entities, relations, fulltext), Client entry: POST /api/v1/ingest (202 job_id) + CLI src.ingestion.run (direct, no Temporal), CommunityBuildWorkflow (offline, admin/schedule, queue kb-graph-build): GDS Leiden + report build, Document ingest flow architecture diagram (D2), DocumentIngestWorkflow (IngestParams to IngestResult, queue kb-ingest), extract_kg step: per-chunk LLM KG extraction (LightRAG), queue kb-ingest-llm, LLMPool-gated, GraphBuildWorkflow (child, queue kb-ingest-merge): merge_and_resolve + build_property_graph, Graph half (best-effort; failure implies graph_status=vector_only) (+12 more)

### Community 148 - "superpowers: Phase 3 er_vec slice —"
Cohesion: 0.14
Nodes (20): NebulaGraph migration plan (Phases 0-4; strangler-fig, backend-dispatched), Phase 3 er_vec slice — ER candidate-kNN via Milvus, backend-dispatched, Milvus collection entity_er_vec (PK name, er_vec FLOAT_VECTOR dim=1536 COSINE HNSW, canonicals only), EntityVectorStore seam (Protocol knn/upsert; Neo4j + Milvus impls; build_entity_vector_store), AgentSettings.er_vector_backend (native|milvus, default native; nebula forces Milvus), Phase 2 vertical slice — Nebula read-path design, find-by-name → nGQL LOOKUP ON Entity (parity gap vs Neo4j fulltext, accepted), Generic RELATED edge + rel_type property (entity–entity relations avoid edge-label injection) (+12 more)

### Community 149 - "superpowers: channel group enum (src/retrieval/groups.py)"
Cohesion: 0.11
Nodes (20): Channel groups implementation plan, doc_group chunk metadata (dynamic field), channel group enum (src/retrieval/groups.py), GroupFilter (Milvus push-down + graph post-filter), per-group rerank weights (AGENT_GROUP_WEIGHTS), tg_ingest resolve_group_map (folder → group), Manual channel reingest + low-priority lane plan, x-max-priority RabbitMQ ingest queue (+12 more)

### Community 150 - "superpowers: doc_group chunk metadata (mirrors doc_date_epoch"
Cohesion: 0.16
Nodes (20): doc_group chunk metadata (mirrors doc_date_epoch push-down), Group search filter (Milvus IN/NIN MetadataFilter + graph post-filter), AGENT_GROUP_WEIGHTS rerank weights (official 1.30 … opinion 0.80), GROUPS enum (news, analytics, digest, opinion, official, data) src/retrieval/groups.py, Channel groups design (six editorial groups → RAG), Synthesis group context ([group] prefix per source), RabbitMQSettings.max_priority (RABBITMQ_MAX_PRIORITY default 10), PRIO_LIVE=5 / PRIO_BACKFILL=0 priority constants (+12 more)

### Community 151 - "analytics: StepResult"
Cohesion: 0.23
Nodes (17): StepResult, build_synthesis_prompt(), extract_numbers(), faithfulness_score(), _norm(), numbers_in_rows(), ChatMessage, Synthesis prompt + numeric-faithfulness checker.  The LLM verbalizes; it must no (+9 more)

### Community 152 - "test_graph: test_alert_store.py"
Cohesion: 0.16
Nodes (15): alert_vid(), 32-hex VID for an :Alert, mirroring entity_vid/verdict_vid., _NebulaRecStore, _RecStore, test_dispatch_nebula(), test_dispatch_neo4j_default(), test_nebula_mark_watched_updates_each_entity(), test_nebula_read_alerts_filters_and_sorts_in_python() (+7 more)

### Community 153 - "test_graph: ERConfig"
Cohesion: 0.14
Nodes (15): ERConfig, _llm_judge_pairs(), _pick_canonical(), For each input pair, return True when LLM judges SAME, else     False (DIFFERENT, Pick the canonical entity for a cluster.      Priority order:       1. `source =, Tuning knobs for entity resolution., _GraphStoreStub, EntityNode (+7 more)

### Community 154 - "graph: GLiNERExtractor"
Cohesion: 0.16
Nodes (15): _default_entity_types(), gliner_ner_callable(), GLiNERExtractor, _load_gliner(), Any, BaseNode, TransformComponent, Optional GLiNER span-based entity extractor as a LlamaIndex ``TransformComponent (+7 more)

### Community 155 - "test_graph: test_communities_graph_ops.py"
Cohesion: 0.15
Nodes (13): _NebulaRaisingStore, _NebulaRecStore, _RaisingStore, _RecStore, test_dispatch_nebula(), test_dispatch_neo4j_default(), test_nebula_community_overview_matches_by_level(), test_nebula_entity_communities_empty_when_no_edges() (+5 more)

### Community 156 - "diagrams: Temporal Worker (activities + workflows"
Cohesion: 0.16
Nodes (19): Clients layer, System Architecture Diagram (layered: Clients / Edge-API / Temporal / Stores / Observability), Edge / API layer, FastAPI :8000, Hermes Agent client, LiteLLM proxy :4000, MCP-1 :9001, MCP-2 :9002 (+11 more)

### Community 157 - "superpowers: Automatic event detection — design"
Cohesion: 0.12
Nodes (19): Search date filters — design (Rev 2), Single stamping point in parse_and_chunk (chunk metadata epoch dates → Milvus + :Chunk), epoch-days canonical filter value (doc_date_epoch, inserted_at_epoch), Uniform post-filter for both stores (over-fetch + drop out-of-range; Milvus push-down deferred), Automatic event detection — design, Event model — :__Entity__:EventOrAction specialization (event_type/trigger/event_ts + created_at), Event resolution/dedup (event-specific match key: type + participants + event_ts proximity), first_seen / created_at stamping (ON CREATE on nodes/rels/events; one-time backfill sentinel) (+11 more)

### Community 158 - "test_analytics: test_claim_extract.py"
Cohesion: 0.20
Nodes (17): build_extract_prompt(), extract_claims(), _one(), parse_claims(), Claim, LLM extraction of atomic claims from a document (offline, hybrid method B).  Mir, Pure, tolerant parse of an LLM claims array. Never raises., Extract claims from one document. Fail-open ([]) on any LLM error. (+9 more)

### Community 159 - "test_graph: test_analysis_nebula.py"
Cohesion: 0.14
Nodes (16): _personalized_pagerank_from_edges(), _personalized_pagerank_nebula(), Seed-biased PageRank via in-worker igraph (no GDS under nebula)., Undirected shortest path via in-worker igraph (no GDS under nebula)., _shortest_path_from_edges(), _shortest_path_nebula(), _FakeNebulaStore, Nebula backend for graph analysis (TDD). Under nebula there is no GDS: pagerank (+8 more)

### Community 160 - "graph: CanonicalLinker"
Cohesion: 0.19
Nodes (11): CanonicalCandidate, CanonicalLinker, Any, Canonical entity linker — resolve a mention to an existing Wikibase QID.  Turns, One linking candidate — an existing Wikibase item.      ``score`` is an alias-ma, Resolve a mention → existing Wikibase QID, else ``None``.      ``index`` exposes, Return the QID this mention links to, or ``None`` to mint new., _FakeIndex (+3 more)

### Community 161 - "graph_edge_export.py"
Cohesion: 0.15
Nodes (9): build_graph_edge_export(), GraphEdgeExport, NebulaGraphEdgeExport, Neo4jGraphEdgeExport, Any, Protocol, Backend-dispatched graph edge EXPORT (Leiden read-phase).  ``Neo4jGraphEdgeExpor, nGQL graph edge EXPORT. Node names via keyset ``LOOKUP``; edges via a     batche (+1 more)

### Community 162 - "graph: lightrag_extract.py"
Cohesion: 0.12
Nodes (15): _default_entity_types(), _is_transient_llm_error(), BaseException, LightRAG-style KG extractor as a LlamaIndex `TransformComponent`.  One LLM call, True for retryable LLM-backend failures (vs a genuine empty response or     a re, Pull entity-type strings out of `src.graph.schema.EntityType`., LightRAG-style KG extraction prompts, ported for kb-llamaindex.  Three prompts v, Render the few-shot examples block for the system prompt.      Each example temp (+7 more)

### Community 163 - "test_graph: write_with_retry()"
Cohesion: 0.18
Nodes (17): _is_transient(), Any, BaseException, Bounded retry for transient Neo4j write failures (Track A3).  Concurrent ``MERGE, True for Neo4j transient/contention errors that are safe to retry.      Matches, Call ``fn(*args, **kwargs)``, retrying on transient Neo4j errors.      Up to ``m, write_with_retry(), T (+9 more)

### Community 164 - "test_mcp: test_tools_server.py"
Cohesion: 0.12
Nodes (14): _stats_by(), _timeline(), Smoke tests for the MCP-2 atomic tools server.  Verifies the 8 atomic tools are, The MCP tool descriptions came from atomic_tools.TOOL_DESCRIPTIONS     plus a fe, MCP-2 serves over Streamable HTTP (transport='http'), not SSE., filter_by_metadata operates on an in-process accumulator —     doesn't make sens, test_channel_message_stats_bad_date_errors(), test_channel_message_stats_bad_group_by_errors() (+6 more)

### Community 165 - "run_answer_eval.py"
Cohesion: 0.18
Nodes (18): aggregate_by(), CaseScore, check_thresholds(), Group `scores` by attribute (`doc_type`, `category`, `endpoint`)     and return, `by_endpoint_and_doc[endpoint][doc_type]` → metrics dict.      Returns list of v, Per-case scoring breakdown., _hit_endpoint(), main() (+10 more)

### Community 166 - "test_graph: test_centrality_graph_ops.py"
Cohesion: 0.16
Nodes (12): _NebulaRaisingStore, _NebulaRecStore, _RaisingStore, _RecStore, test_dispatch(), test_nebula_fail_soft(), test_nebula_link_prediction_empty(), test_nebula_top_central_reads_column() (+4 more)

### Community 167 - "test_graph: test_events_graph_ops.py"
Cohesion: 0.16
Nodes (13): _NebulaRecStore, _RaisingStore, _RecStore, test_dispatch_nebula(), test_dispatch_neo4j_default(), test_nebula_entity_new_connections_name_anchored(), test_nebula_new_edges_respects_top_n(), test_nebula_new_edges_scans_and_filters_by_created_at() (+5 more)

### Community 168 - "download_models.py"
Cohesion: 0.15
Nodes (15): ArgumentParser, build_arg_parser(), _download_gliner(), _download_reranker(), _force_online(), main(), Pre-download the project's HuggingFace models into a local cache.  Two models fl, Download the cross-encoder reranker into the cache. (+7 more)

### Community 169 - "test_retrieval: test_llm_factory.py"
Cohesion: 0.17
Nodes (17): _capture(), _capture_kwargs(), Role-keyed LLM factory tests.  Confirms each wrapper resolves its model name thr, Default config ⇒ no extra_body wired into the request., Patch OpenAILike, call ``build_llm(...)`` once, return the model     name that t, Legacy path: no role kwarg ⇒ small tier (effective_base)., Explicit LITELLM_LLM_MODEL still wins for the no-role path., Like ``_capture`` but returns the full kwargs dict OpenAILike saw. (+9 more)

### Community 170 - "superpowers: kb-llamaindex Conference Deck plan"
Cohesion: 0.14
Nodes (17): Graph-scale & GraphRAG-parity backlog, AgentSettings config class, Claims/covariates extraction (KG_CLAIMS_KEY), kb-llamaindex Conference Deck plan, Entity Resolution 12-step pipeline, LightRAG-style KG extractor, Marp Markdown decks (A/D), Milvus vector index (+9 more)

### Community 171 - "superpowers: LLMPool (per-process role lanes +"
Cohesion: 0.15
Nodes (17): LiteLLM proxy gateway, LiteLLM Redis cache plan, LiteLLM proxy Redis response cache, Project-side Redis LLM cache plan, CachedLLM (OpenAILike read-through), LLMCacheSettings, Interactive .env Builder plan, scripts/make_env.py (+9 more)

### Community 172 - "graph: admin.py"
Cohesion: 0.20
Nodes (13): monitor_sweep(), monitor_watch(), Admin operations: trigger wiki/monitor sweeps and monitor watchlist., wiki_rebuild(), clear_dirty(), mark_dirty(), Dirty-flag bookkeeping for the wiki editor (Neo4j __Entity__ props).  Routes thr, build_wiki_graph_ops() (+5 more)

### Community 173 - "er_graph_ops.py"
Cohesion: 0.13
Nodes (8): ERGraphOps, NebulaERGraphOps, Protocol, Backend-dispatched entity-resolution GRAPH ops (verdict cache + edge-redirect me, nGQL ER graph ops: verdict cache (VID-addressed ``ERVerdict``     vertices) + th, # NOTE: do NOT catch exceptions here — `structured_query` raises on, Deterministic Nebula VID for an ``ERVerdict`` vertex.      Mirrors ``nebula_stor, verdict_vid()

### Community 174 - "test_retrieval: RoundGraphData"
Cohesion: 0.23
Nodes (12): Build a retriever backed only by a KbGraphStore (structured_query),         with, RoundGraphData, _FakeStore, Nebula read slice: store-only retriever construction + aretrieve guard., graph_search under nebula: er_vec kNN picks entities, then subgraph-expand., test_aretrieve_empty_without_retriever(), test_aretrieve_nebula_knn_then_expands(), test_awalk_nebula_applies_rel_filter() (+4 more)

### Community 175 - "test_workflow: push_wikibase.py"
Cohesion: 0.21
Nodes (15): _load_base_classes(), _load_properties(), push_wikibase(), push_wikibase activity.  Reads the merged-entities staging blob, loads the boots, _fake_merged(), Tests for the push_wikibase activity.  Three behaviours:   * cache disabled -> s, If push_entities returns counters where created+updated == 0     yet we DID rece, An ingest with no entities to push (empty merged blob) IS a     valid no-op — st (+7 more)

### Community 176 - "test_graph: test_community_read.py"
Cohesion: 0.16
Nodes (11): Fake nebula store: records nGQL; returns canned rows per substring., Records structured_query(cypher, param_map) calls; returns canned rows., _RecNebula, _RecStore, test_dispatch_returns_nebula_when_backend_nebula(), test_dispatch_returns_neo4j_when_backend_not_nebula(), test_map_communities_lexical_routes_through_reader(), test_nebula_read_summaries_defaults_missing_member_count_to_zero() (+3 more)

### Community 177 - "ingest_scale_bottlenecks.svg: DocumentIngestWorkflow"
Cohesion: 0.15
Nodes (16): Ingest scale bottlenecks — architecture diagram, DocumentIngestWorkflow, Entity Resolution — FIXED: native vector kNN now DEFAULT ON (was 5000-window → recall→0 as graph grows), extract_kg activity, GraphBuildWorkflow (kb-ingest-merge), admit K=1, IngestSchedulerWorkflow (singleton) — docs run ONE-AT-A-TIME, inject_canonical activity — MED: no heartbeat (only long stage w/o one), LiteLLM proxy (single) (+8 more)

### Community 178 - "superpowers: Analytical layer — design (NL"
Cohesion: 0.16
Nodes (16): Analytical layer — design (NL analytical Q&A over the graph), AnalyticalQueryWorkflow (plan → execute primitives|cypher → synthesize + provenance), AnalyticsMaterializeWorkflow (offline GDS: centrality/link-prediction → Neo4j properties), cypher_guard — guarded read-only text-to-Cypher fallback (denylist, timeout, LIMIT; kill switch), analytical planner (small-tier LLM NL → validated AnalysisPlan), Primitive catalog (~24 primitives, 4 families; open/closed registry in catalog.py), Provenance chain (answer + primitive/Cypher + raw rows + source chunks; numbers from executor not LLM), Actionable signals — from graph metrics to decisions (+8 more)

### Community 179 - "setup_db.py"
Cohesion: 0.17
Nodes (12): main(), Idempotent DB initialisation.  Stage 1 scope:   * Postgres — create ``documents`, Register the three custom Search Attributes used by the     analytics layer (Sta, Enforce the configured closed-workflow retention on the namespace.      Bounds P, Verify Milvus reachable; collection lifecycle owned by     ``MilvusVectorStore``, Ensure the user-upload bucket exists.  Same MinIO instance the     Milvus backen, setup_milvus(), setup_minio() (+4 more)

### Community 180 - "test_analytics: ids.py"
Cohesion: 0.19
Nodes (12): epoch_days_to_period(), is_meaningful_entity(), Constants + pure helpers for the analytical layer (no I/O, no LLM)., Bucket an epoch-day integer into a period label.      granularity: ``year`` → ``, Whether an entity row is worth showing in a themes/events answer.      Always dr, test_drops_degenerate_names(), test_drops_identifier_types(), test_drops_url_named_entities() (+4 more)

### Community 181 - "bot: pipeline.py"
Cohesion: 0.20
Nodes (13): is_allowed(), parse_allowed_users(), Telegram user whitelist. An empty whitelist denies EVERYONE — a personal KB bot, True iff ``user_id`` is whitelisted. Empty whitelist → always False., Parse a comma-separated ``BOT_ALLOWED_USERS`` into a set of user ids.     Non-in, The bot's answer pipeline: whitelist → session → follow-up rewrite → KB search →, Whitelist access control (TDD). Empty whitelist = deny-all (secure default)., test_allowed_user_passes() (+5 more)

### Community 182 - "bot: with_fallback()"
Cohesion: 0.18
Nodes (13): is_empty_answer(), Shared answer helpers: detect an empty/no-result answer so neither the search-fa, True for a blank answer or a known empty-synthesis marker., make_analyze(), SearchFn, KB search adapter for the bot: POST /api/v1/search/{mode} and return the synthes, Build an async ``analyze(query) -> answer`` over the analytical layer     (POST, Return a search that tries ``primary`` first and only falls back to     ``fallba (+5 more)

### Community 183 - "test_config: TemporalSettings"
Cohesion: 0.14
Nodes (8): Temporal worker / client connection settings., TemporalSettings, test_community_backend_is_constrained(), Dedicated merge queue (decouples GraphBuildWorkflow's merge stage     from a bur, test_merge_queue_defaults(), test_merge_queue_env_override(), Phase 4(a): the IngestSchedulerWorkflow singleton runs on its OWN task queue / w, test_scheduler_has_its_own_task_queue()

### Community 184 - "test_graph: community_graphscope.py"
Cohesion: 0.12
Nodes (6): _all_names(), GraphScope community-detection backend (single-level Leiden, distributed).  Mirr, Dedup names across node_names + edge endpoints (mirrors     community_leiden.bui, Build a GraphScope graph from `edges` and run its modularity community     algor, _run_graphscope_community(), single_level_rows_graphscope maps a mocked GraphScope partition to rows.

### Community 185 - "graph: community_read.py"
Cohesion: 0.17
Nodes (9): build_community_read(), CommunityRead, NebulaCommunityRead, Neo4jCommunityRead, Any, Protocol, Backend-dispatched community READ (map-phase summary fetch).  `Neo4jCommunityRea, Runs the historical Cypher constant verbatim — zero behaviour change. (+1 more)

### Community 186 - "ingestion: identifier_transform.py"
Cohesion: 0.19
Nodes (13): _exclude_augment_from_embed(), _ident_to_dict(), Any, BaseNode, LlamaIndex ``TransformComponent`` for identifier canonicalization.  Inserted int, Keep ``_AUGMENT_METADATA_KEY`` out of the EMBED metadata view.      The KG extra, build_augment_block(), dedupe_by_canonical() (+5 more)

### Community 187 - "retrieval: atomic_tools.py"
Cohesion: 0.20
Nodes (14): dispatch(), filter_by_metadata(), NodeWithScore, Protocol, Atomic retrieval tools as pure async functions.  Each function is a standalone u, Hybrid (BM25 + dense) retrieval over corpus chunks., In-memory filter over already-accumulated sources.      Pure / synchronous; does, Dispatch a tool call by name.  Used by the Temporal     ``tool_execution`` activ (+6 more)

### Community 188 - "test_retrieval: get_chunks_by_doc_id()"
Cohesion: 0.17
Nodes (11): ChunkRepositoryProtocol, get_chunks_by_doc_id(), Fetch all chunks of one document in source order., Read raw source text of one document (pre-chunking, pre-translation)., read_full_document(), _StubChunkRepo, test_get_chunks_by_doc_id_builds_sources(), test_get_chunks_by_doc_id_no_repo() (+3 more)

### Community 189 - "test_graph: _FakeClient"
Cohesion: 0.18
Nodes (6): _FakeClient, _I, _S, _store(), test_knn_filters_level_and_maps(), test_upsert_rows()

### Community 190 - "superpowers: Seven Tracks plan"
Cohesion: 0.13
Nodes (15): Always-on ingest admission (IngestSchedulerWorkflow), K+N single-semaphore LLM pool, K+N throttle migration plan, Answer template (Track 6), doc-by-id MinIO-aware load (Track 3), Ingest classifier + force (Track 2), Seven Tracks plan, Prod docker compose stack (Track 1) (+7 more)

### Community 191 - "di: providers.py"
Cohesion: 0.19
Nodes (11): Provider, build_api_container(), build_worker_container(), CommonProvider, BaseEmbedding, Dishka DI providers.  Two containers:   * **API** — long-lived: PG client + shar, Shared singletons available to both API and worker., Worker container — currently exposes only `CommonProvider`.      The Temporal wo (+3 more)

### Community 192 - "merge_identifier_duplicates.py"
Cohesion: 0.20
Nodes (14): _amain(), apply_merges(), _build_merge_cypher(), canonicalize_for_type(), collect_entity_nodes(), group_by_canonical(), main(), _parse_args() (+6 more)

### Community 193 - "set_admission.py: get_temporal_client()"
Cohesion: 0.20
Nodes (11): _main(), Live-update the ingest admission ceiling K (max_inflight) on the running ``inges, _main(), Idempotently create/update the Temporal Schedule that runs MonitorSweepWorkflow, Start the OFFLINE ``CommunityBuildWorkflow`` on ``kb-graph-build``.      Fully d, rebuild_communities(), get_temporal_client(), Client (+3 more)

### Community 194 - "graph: alert_store.py"
Cohesion: 0.19
Nodes (5): build_alert_store(), NebulaAlertStore, Neo4jAlertStore, Any, Backend-dispatched Arc-2 :Alert store (upsert / read / mark_watched).  ``Neo4jAl

### Community 195 - "graph: event_ts_resolver.py"
Cohesion: 0.25
Nodes (14): _anchor_date(), _dateparser_day(), _day_bounds(), _month_bounds(), _nearest_month_day(), _nearest_year(), date, Deterministic event-time resolver: raw phrase + doc date → interval.  Pure modul (+6 more)

### Community 196 - "test_api: test_search_v2_routes.py"
Cohesion: 0.20
Nodes (12): AutoSearchWorkflow, Auto mode — route the question, then dispatch to local/global/drift., _api_key_header(), _outcome(), _post(), _post_body(), ASGI tests for the R7a search endpoints (/search/global|drift|auto).  The Tempor, top_k is bounded [1,100], query non-empty, history capped — invalid     requests (+4 more)

### Community 197 - "test_analytics: _FakeOps"
Cohesion: 0.32
Nodes (8): _FakeOps, _patch(), Records (method, args); returns canned rows per method name., test_contradictions_routes_through_seam(), test_failsoft_all_primitives_return_empty_without_store(), test_incomplete_entities_resolves_and_threads_expected(), test_merge_candidates_routes_through_seam(), test_orphans_threads_min_degree()

### Community 198 - "test_graph: test_retriever_triplet_parse.py"
Cohesion: 0.27
Nodes (10): _build(), _FakePGIndex, _FakeRetriever, _node(), NodeWithScore, GraphRetriever.aretrieve must extract entities/relations from the TextNode-shape, test_duplicate_triplets_across_nodes_deduped(), test_multi_hop_chain_line_yields_pairwise_relations() (+2 more)

### Community 199 - "test_ingest_queue: test_consumer.py"
Cohesion: 0.30
Nodes (12): AbstractIncomingMessage, handle_message(), Client, Process one ingest message: start + await the document workflow,     then ack/re, _FakeMessage, _good_body(), Consumer message-handling contract (Track B3).  No broker / no Temporal: a fake, test_duplicate_doc_is_acked() (+4 more)

### Community 200 - "Architecture Decision Records (ADR) practice"
Cohesion: 0.18
Nodes (14): ADR-0002: Claim-check staging via MinIO, ADR-0007: Entity Resolution = candidate-gen + LLM-judge + cache + union-find, ADR-0009: Hierarchical Leiden communities + structured reports, ADR-0010: Dynamic community selection (lexical/semantic/descent), ADR-0014: Source download via stable API endpoint, ADR-0015: Community-detection backend = in-worker leidenalg, Architecture Decision Records (ADR) practice, CONCEPTS.md (planned educational companion) (+6 more)

### Community 201 - "ANALYTICS-GUIDE.md: Centralities (four notions of importance)"
Cohesion: 0.14
Nodes (14): Betweenness centrality, Bonacich (1987) Power and Centrality, Brin & Page (1998) PageRank, Burst detection (temporal dynamics), Burt (1992) Structural Holes, Centralities (four notions of importance), Degree centrality, Eigenvector centrality (+6 more)

### Community 202 - "CONCEPTS.md: Entity Resolution (ER)"
Cohesion: 0.16
Nodes (14): Entity Resolution (ER), Hyper-hub clamp (ER), Deterministic identifier canonicalization, LightRAG KG extraction, Native-vector kNN ER candidate source, Document parsing & chunking, Union-find clustering (ER), ER verdict cache (:ERVerdict) (+6 more)

### Community 203 - "superpowers: Wikibase populator runbook"
Cohesion: 0.21
Nodes (14): Wikibase populator runbook, QID writeback idempotency, scripts/setup_wikibase.py bootstrap, scripts/smoke_wikibase_push.py, WDQS / Blazegraph SPARQL endpoint, Self-hosted Wikibase, Wikibase population plan, push_entities orchestrator (src/storage/wikibase.py) (+6 more)

### Community 204 - "superpowers: Agentic Search plan (Plan #2)"
Cohesion: 0.20
Nodes (14): ReAct agent (/agent, react_agent.py), Agentic Search plan (Plan #2), CommunityBuildWorkflow (GDS Leiden offline), GlobalSearchWorkflow (community map-reduce), graph_walk multi-hop tool, SearchOrchestratorWorkflow, SubQueryRetrievalWorkflow, Search drift-fix + dead-code audit plan (+6 more)

### Community 205 - "workflow: wiki_sweep.py"
Cohesion: 0.20
Nodes (11): _main(), Idempotently create/update the Temporal Schedule that runs WikiSweepWorkflow eve, select_dirty(), ArticleOutcome, WikiSweepWorkflow — select dirty entities, (re)write each article., select_dirty_entities(), _tally(), WikiSweepWorkflow (+3 more)

### Community 206 - "build_er_graph_ops()"
Cohesion: 0.15
Nodes (8): _load_verdict_cache(), Fetch cached verdicts for `keys` from Neo4j.      Returns `{key -> same}`.  Empt, Persist freshly-judged verdicts to Neo4j (`MERGE` by key).      No-op when `stor, _store_verdicts(), build_er_graph_ops(), Neo4jERGraphOps, Any, Runs the historical ER graph Cypher/APOC verbatim — zero behaviour     change fr

### Community 207 - "Neo4jWikiGraphOps"
Cohesion: 0.14
Nodes (3): Neo4jWikiGraphOps, Any, Runs the historical wiki-editor Cypher verbatim — zero behaviour     change from

### Community 208 - "test_workflow: select_communities_descent()"
Cohesion: 0.20
Nodes (12): _cosine(), Cosine similarity of two vectors as a plain float.      Pure / deterministic.  E, v2 hierarchy descent: start at the coarsest level (0), keep the most     query-r, select_communities_descent(), _FakeTreeStore, Serves a fixed community tree for descent traversal.      ``roots`` are the leve, _row(), test_cosine_orthogonal_identical_empty() (+4 more)

### Community 209 - "eval: run_scale_bench.py"
Cohesion: 0.33
Nodes (13): bench_dedup_recall(), _candidate_norm_pairs(), Run _candidate_pairs and return the surfaced pairs as norm-sets., For each ``knn_k``, fraction of planted duplicate pairs surfaced     as candidat, _cmd_all(), _cmd_er_cost(), _cmd_er_recall(), _cmd_graph_write() (+5 more)

### Community 210 - "test_retrieval: test_atomic_tools.py"
Cohesion: 0.24
Nodes (10): _node(), NodeWithScore, Unit tests for src/retrieval/atomic_tools.py.  Each pure function gets mocked re, _StubRetriever, test_dispatch_routes_to_vector_search(), test_filter_by_metadata_by_doc_id(), test_filter_by_metadata_multi_filter(), test_graph_walk_carries_chunks_as_sources() (+2 more)

### Community 211 - "test_storage: test_minio_stream.py"
Cohesion: 0.22
Nodes (6): _FakeClient, _FakeResp, Unit tests for MinioStorage stat/stream (stub minio client)., _storage(), test_stat_object_returns_name_size_type(), test_stream_object_yields_and_releases()

### Community 212 - "superpowers: NebulaGraph cutover — neo4j decommissioned"
Cohesion: 0.22
Nodes (13): nGQL translation rules (nebula 3.8), Cypher→nGQL MATCH translation rules, NebulaGraph cutover — neo4j decommissioned, NebulaGraph backend, Neo4j graph backend (retained seam), NebulaGraph cutover-readiness assessment, Neo4j→nebula data-migration blocker, Wiki-editor crash set under nebula (+5 more)

### Community 213 - "superpowers: Spec — Seven Tracks (build"
Cohesion: 0.18
Nodes (13): Spec — Seven Tracks (build order for 7 capabilities), Track 6 — templated answers (answer_template threaded through synthesis), Track 2 — input document classifier (skip) with force-override, Track 1 — production docker-compose (whole app in compose, external litellm/ollama), Track 5 — document-level admission control (IngestSchedulerWorkflow, MAX_INFLIGHT_DOCS), Track 7a — meaningful relation weight + tags (mention_count/confidence, provenance aggregation), Track 3 — source-document-by-id bugfix (doc_id in Milvus + MinIO-aware chunk_repository), Track 4 — weighted Leiden at 50k: instrumentation + knobs (no silent GDS-error swallow) (+5 more)

### Community 214 - "test_workflow: dispatch_for_route()"
Cohesion: 0.23
Nodes (12): RouteLabel, dispatch_for_route(), merge_doc_ids(), Pure: map a route label to the workflow that serves it.      Unknown / unexpecte, Union of two doc_id lists, order-preserving, deduped., Routing dispatch tests (Search R7a).  The route→workflow mapping lives in the pu, test_dispatch_date_scoped_downgrades_global_to_local(), test_dispatch_date_scoped_leaves_local_and_drift_intact() (+4 more)

### Community 215 - "eval: diag_kg_lightrag.py"
Cohesion: 0.18
Nodes (11): main(), _parse_args(), Namespace, Offline probe of the LightRAG-style extract + merge stack.  Runs the live projec, main(), Probe KG extraction on real medical chunks.  Splits the medical corpus into chun, load_medical_source(), Return the raw `context` text from `medical.json`. (+3 more)

### Community 216 - "Neo4jAggregationsGraphOps"
Cohesion: 0.24
Nodes (3): Neo4jAggregationsGraphOps, Any, Runs the historical aggregations Cypher verbatim — zero behaviour     change fro

### Community 218 - "workflow: retrieve.py"
Cohesion: 0.22
Nodes (11): ``retrieve_subquestion`` activity — deterministic retrieval (R2).  For ONE sub-q, Seed entity name(s) for graph_walk.      Legacy (dual=False): graph_search's top, _walk_seeds(), _build_retriever_once(), get_retriever(), get_vector_retriever(), Any, Per-request vector retriever at a custom ``similarity_top_k``.      Used by the (+3 more)

### Community 219 - "test_retrieval: test_graph_path_depth.py"
Cohesion: 0.28
Nodes (8): _build(), _FakePGIndex, _FakeRetriever, Unit tests for GraphRetriever per-call ``path_depth``.  Uses a fake PropertyGrap, test_default_depth_prebuilt_and_reused(), test_depth_clamped_to_max(), test_depth_clamped_to_min(), test_per_call_depth_builds_then_caches()

### Community 220 - "diagrams: GlobalSearchWorkflow (mode=global): GraphRAG map-reduce over"
Cohesion: 0.29
Nodes (12): AutoSearchWorkflow (mode=auto): router decides local|global|drift, CommunityBuildWorkflow: community summaries built offline, dispatch_for_route: local | global | drift, Drift step 0: contextualize ONCE (children get history cleared), Drift step 2: global with drift_mode=True, seeded by local sources, Drift step 1: run local (child), DriftSearchWorkflow (mode=drift): specific + corpus context, heaviest mode, Entry surfaces: MCP kb_search/_global/_drift/_auto + FastAPI /api/v1/search/{local,global,drift,auto} on kb-search-small queue (+4 more)

### Community 221 - "presentation: Conference deck A (tech/ML)"
Cohesion: 0.18
Nodes (12): Conference deck A (tech/ML), Eval gate (287 tests + golden Q&A), Per-request tracing (trace_request), Postgres job-state store, /agent ReAct loop (8 tools), /selfrag reflective synthesis, Three query endpoints: /search, /agent, /selfrag (legacy), Conference deck D (internal defense) (+4 more)

### Community 222 - "superpowers: Nebula community-BUILD (nGQL) implementation plan"
Cohesion: 0.18
Nodes (12): Nebula Community TAG + level index DDL, community_vid VID scheme (blake2b level-scoped), CommunityWriteback seam, Nebula community-BUILD (nGQL) implementation plan, NebulaCommunityWriteback (nGQL INSERT/LOOKUP/DELETE/FETCH), Neo4jCommunityWriteback (verbatim Cypher), CommunitySummarize seam, Nebula community-SUMMARIZE (nGQL) implementation plan (+4 more)

### Community 223 - "message_stats.py"
Cohesion: 0.27
Nodes (10): format_status_rows(), format_timeline(), main(), Namespace, CLI for processed-message statistics — a thin wrapper over the same AsyncPostgre, Render status_counts_by output as an aligned text table., _run(), CLI wiring: `channels` calls status_counts_by('source_channel', ...) and its row (+2 more)

### Community 224 - "test_config: test_preflight.py"
Cohesion: 0.36
Nodes (10): Return a list of actionable config problems (empty == OK).          Hard problem, Boot-time preflight: actionable problems instead of mid-request stack traces., _settings(), test_preflight_clean_prod_has_no_problems(), test_preflight_dev_allows_placeholders(), test_preflight_flags_placeholder_key_in_multikey_api_keys(), test_preflight_flags_placeholder_minio_secret_in_prod(), test_preflight_flags_placeholder_secret_in_prod() (+2 more)

### Community 226 - "test_graph: test_store.py"
Cohesion: 0.23
Nodes (5): Process-global Neo4j graph-store cache + driver-pool kwargs (Track A2).  No live, _RecordingStore, test_build_installs_query_logging_only_when_flag_on(), test_query_logging_wrapper_is_idempotent(), test_query_logging_wrapper_logs_and_passes_through()

### Community 227 - "ARCHITECTURE.md: DocumentIngestWorkflow"
Cohesion: 0.20
Nodes (11): R7b legacy search cutover (BREAKING), FastAPI API service, Ingest path (DocumentIngestWorkflow), Temporal task queues, Search path (local/global/drift/auto), Temporal worker (queue pools), Durable Execution (Temporal), Answer templates (+3 more)

### Community 228 - "test_analytics: detect_contradictions_e2e()"
Cohesion: 0.33
Nodes (10): Complete, detect_contradictions_e2e(), EmbedFn, ``docs`` = list of ``{"doc_id","source","text"}``. Returns NLI-confirmed     con, _embed(), _extract(), End-to-end contradiction pipeline (TDD): extract → cluster → structural → NLI-re, test_e2e_confirmed_contradiction() (+2 more)

### Community 229 - "ARCHITECTURE.md: Production docker-compose"
Cohesion: 0.24
Nodes (11): Dev docker-compose stack, Telegram bot overlay, LiteLLM overlay, OpenClaw agent gateway overlay, Production docker-compose, Scale-out worker override, Telegram ingest harness overlay, LiteLLM config (OpenAI upstream) (+3 more)

### Community 230 - "superpowers: GraphRetriever.for_store (store-only, no PropertyGraphIndex)"
Cohesion: 0.20
Nodes (11): Nebula read-slice (Phase 2) implementation plan, GraphRetriever.for_store (store-only, no PropertyGraphIndex), NebulaGraphStore.subgraph mapper, nGQL GET SUBGRAPH bounded walk, nGQL LOOKUP find-by-name (afind_entities_by_name nebula branch), generic RELATED edge + rel_type property, afind_entities_by_name (fulltext partial-name recall), Graph-search entity recall design (+3 more)

### Community 231 - "api: stats.py"
Cohesion: 0.31
Nodes (10): messages_stats(), MessagesStatsResponse, BaseModel, date, FromDishka, Processed-message statistics over the `documents` table.  Two read-only endpoint, StatRow, timeline_stats() (+2 more)

### Community 232 - "test_graph: test_retriever_fulltext.py"
Cohesion: 0.27
Nodes (8): build_fulltext_query(), Build a Lucene OR-of-tokens query for the ``entity_name_fulltext``     index: wh, Tests for the full-text entity-name lookup helpers., _retriever(), _StubStore, test_afind_entities_by_name_blank_and_failopen(), test_afind_entities_by_name_maps_rows(), test_build_fulltext_query_or_tokens_escaped()

### Community 234 - "eval: bench_flat_vs_hnsw()"
Cohesion: 0.24
Nodes (10): bench_flat_vs_hnsw(), _build_and_query(), _client(), ndarray, Milvus FLAT-vs-HNSW latency + recall benchmark (P1.1).  Runs against a LOCAL Mil, Compare FLAT (exhaustive, = current default) vs HNSW at ``n``     vectors.  Retu, Return a connected MilvusClient or None if unreachable., Create a collection with the given index, insert corpus, run the     query set. (+2 more)

### Community 235 - "CONCEPTS.md: LLMPool concurrency gating"
Cohesion: 0.22
Nodes (10): Dedicated kb-ingest-merge queue, R1 two-tier model architecture, LiteLLM scale config (Ollama fleet), LiteLLM vLLM config, MCP servers (MCP-1 kb_search, MCP-2 atomic tools), LLMPool concurrency gating, Task queue isolation / head-of-line blocking, Role to tier map (7 roles) (+2 more)

### Community 236 - "runbook: DocumentIngestWorkflow (parent)"
Cohesion: 0.22
Nodes (10): ADR-0001: Temporal for durable orchestration, ADR-0003: Task-queue isolation to avoid head-of-line blocking, Temporal durable orchestration, RabbitMQ / taskiq broker, Admission control (IngestSchedulerWorkflow), Input document classifier (classify_document), DocumentIngestWorkflow (parent), GraphBuildWorkflow (child) (+2 more)

### Community 237 - "adr: Neo4j property graph store"
Cohesion: 0.29
Nodes (10): ADR-0008: Optional native-vector kNN ER over 5000-row window, ADR-0012: Wikibase canonical anchor + continuous wiki editor, kb-llamaindex RAG system, Neo4j property graph store, Wikibase canonical anchor, Entity Resolution 12-step pipeline, Native vector kNN ER (er_vec), Batch graph consolidation (reresolve_graph) (+2 more)

### Community 238 - "ANALYTICS-GUIDE.md: Community detection / Leiden"
Cohesion: 0.24
Nodes (10): Edge et al. (2024) GraphRAG (arXiv:2404.16130), Fortunato & Barthelemy (2007) Resolution Limit, Leiden & modularity, Resolution limit (modularity defect), Traag et al. (2019) From Louvain to Leiden, Community reports (map-reduce summarization), Community detection / Leiden, Hierarchical communities + dynamic selection (+2 more)

### Community 239 - "CONCEPTS.md: Local search (vector + graph"
Cohesion: 0.20
Nodes (10): Auto mode / query routing, Coverage check (bounded refinement round), DRIFT search (local + global), Global search (community map-reduce), Local search (vector + graph expansion), Plan-execute orchestrator, Reranking (bge cross-encoder), Drift soft fallback (+2 more)

### Community 240 - "superpowers: WikiSweepWorkflow (dirty-entity sweep)"
Cohesion: 0.20
Nodes (10): community_report_vec Milvus collection, MilvusCommunityReportVectorStore, AsyncMediaWiki client (Action API), Continuous wiki article editor design, subgraph_hash dirty tracking (skip unchanged), WikiSweepWorkflow (dirty-entity sweep), dynamic community selection (v1 semantic kNN + v2 hierarchy descent), community_report_vec native Neo4j index (+2 more)

### Community 241 - "test_retrieval: find_entity_by_name()"
Cohesion: 0.29
Nodes (7): find_entity_by_name(), Find entities whose NAME matches the query via the full-text index.      Partial, _Data, find_entity_by_name atomic tool., _Retriever, test_find_entity_by_name_none_retriever(), test_find_entity_by_name_returns_entities()

### Community 242 - "test_api: test_documents.py"
Cohesion: 0.42
Nodes (9): _get(), _key(), ASGI tests for GET /api/v1/documents/{doc_id}., _row(), _storage(), test_download_requires_api_key(), test_download_sanitizes_filename_header(), test_download_streams_original() (+1 more)

### Community 244 - "superpowers: LLMPool (per-process role-keyed pool)"
Cohesion: 0.25
Nodes (9): BoundedLLM gating wrapper (ordered semaphores), LLM pool consolidation design, hierarchical tier+lane sizing (over-subscription + judge_floor), LLMPool (per-process role-keyed pool), render_bot_section / splice_bot_section (anti-drift, grounded), K+N throttle migration design, IngestAdmission K (always-on max_inflight), K+N throttle model (single N semaphore + FIFO K admission) (+1 more)

### Community 245 - "superpowers: Community-detection offload from Neo4j —"
Cohesion: 0.25
Nodes (9): Community-detection offload from Neo4j — design, community_backend config selector (gds|leidenalg, default gds until parity benchmark), Edge-extractor — streams (s.name,t.name,weight) in batches (no GDS projection), leidenalg + python-igraph clusterer (hierarchical Leiden in worker RAM), Hierarchical community-summaries — leaf-level context fix, ADR-0009 (hierarchical-leiden) + ADR-0010 (dynamic-community-selection), descent community-selection mode (only mode that walks the hierarchy via PARENT_OF), Persist IN_COMMUNITY member-edges at all levels (_MERGE_SUBCOMMUNITY_CYPHER gains member block) (+1 more)

### Community 247 - "retrieval: GraphRetrieverProtocol"
Cohesion: 0.31
Nodes (6): find_neighbours(), GraphRetrieverProtocol, Any, Neighbours of an entity: matched node + relations up to ``hops``     triplet-hop, test_find_neighbours_passes_hops_as_path_depth(), test_find_neighbours_returns_entities_and_relations()

### Community 248 - "test_observability: test_litellm_models.py"
Cohesion: 0.31
Nodes (8): _proxy_returns(), Unit tests for the LiteLLM model-validator that runs at API + worker startup.  T, Build a context manager that patches httpx.Client.get to return     a fake LiteL, Connectivity failure → empty available list → no validation;     only an at-star, test_all_models_registered_logs_info(), test_missing_model_raises_in_strict_mode(), test_missing_model_warns_in_non_strict_mode(), test_proxy_unreachable_does_not_block_boot()

### Community 250 - "test_workflow: _FakeNebulaStore"
Cohesion: 0.25
Nodes (5): _FakeNebulaStore, _FakeReportVecStore, Returns canned rows keyed by first matching substring of the nGQL., test_descent_children_nebula_go_parent_of(), test_descent_root_nebula_reads_tree_and_attaches_milvus_vecs()

### Community 251 - "nebula_bootstrap.py: _connect()"
Cohesion: 0.29
Nodes (6): ConnectionPool, _connect(), main(), One-time NebulaGraph bootstrap: register the storaged host.  Run ONCE after the, Init a pool, retrying while graphd is still coming up.      graphd's compose hea, test_can_connect_and_show_hosts()

### Community 252 - "ARCHITECTURE.md: Neo4j property graph store"
Cohesion: 0.29
Nodes (8): 42-primitive Cypher catalog, Provenance-is-ground-truth rule, Graph analytics (/api/v1/analyze), Neo4j property graph store, Wikibase / MediaWiki anchor, Neo4j property graph & indexes, wipe_db reset procedure, Graph analytics layer (Waves 0-3)

### Community 253 - "superpowers: EntityVectorStore seam (knn/upsert)"
Cohesion: 0.32
Nodes (8): er_vector_backend dispatch (native/milvus, forced under nebula), ER-vec → Milvus (Phase 3) implementation plan, entity_er_vec Milvus collection, EntityVectorStore seam (knn/upsert), MilvusEntityVectorStore, Neo4jEntityVectorStore (native index wrapper), CommunityReportVectorStore seam, report_vec → Milvus (semantic slice) implementation plan

### Community 254 - "backfill_er_vector.py: configure_logging()"
Cohesion: 0.36
Nodes (6): _backfill_cypher(), main(), Backfill ``__Entity__.er_vec`` (native vector list) from the legacy ``er_embeddi, configure_logging(), Loguru bootstrap.  Call :func:`configure_logging` once at app/worker startup.  T, Replace loguru's default handler with a project-tuned one.      ``json_output=Tr

### Community 255 - "ingest_medical.py"
Cohesion: 0.32
Nodes (7): main(), _parse_args(), prepare_txt(), Namespace, Path, Ingest the Medical benchmark corpus through `/api/v1/ingest`.  Converts `tests/e, Materialize medical.json → medical.txt in /tmp for upload.

### Community 256 - "test_scripts: _pages_to_delete()"
Cohesion: 0.39
Nodes (7): _pages_to_delete(), Titles to delete: every listed (main-namespace) page except keep-list., Unit tests for the pure helpers in scripts/wipe_db.py.  The I/O wipe functions (, test_pages_to_delete_custom_keep(), test_pages_to_delete_empty(), test_pages_to_delete_excludes_keep_list(), test_pages_to_delete_keeps_main_page_by_default()

### Community 257 - "KbGraphStore"
Cohesion: 0.32
Nodes (4): KbGraphStore, Any, Protocol, The graph-store surface the app actually uses.  A narrow subset of LlamaIndex's

### Community 258 - "test_analytics: test_domain.py"
Cohesion: 0.25
Nodes (3): test_communication_stats_counts_pairs(), test_issue_resolution_stats_computes_rate(), test_issue_resolution_stats_empty_no_div_by_zero()

### Community 259 - "test_graph: test_community_vector_store.py"
Cohesion: 0.36
Nodes (5): _FakeGraphStore, CommunityReportVectorStore: Neo4j impl query/mapping + factory dispatch., test_factory_dispatches(), test_neo4j_knn_maps_rows(), test_neo4j_upsert_is_noop()

### Community 260 - "runbook: Per-role/tier LLM selection"
Cohesion: 0.29
Nodes (7): ADR-0004: Per-process LLMPool owns LLM concurrency, ADR-0013: Multi-model role/tier selection + snapshot-at-submit, LiteLLM proxy (LLM/embed gateway), ingest_metrics (Postgres per-run timings), Prometheus + Grafana ingest analytics, LLMPool (per-process concurrency owner), Per-role/tier LLM selection

### Community 261 - "presentation: 5-step ingestion pipeline"
Cohesion: 0.29
Nodes (7): ADR-0005: Deterministic identifier canonicalization before LLM extraction, Cross-chunk merge, IdentifierCanonicalization ingest step, 5-step ingestion pipeline, KG extraction fix: SchemaLLMPathExtractor -> SimpleLLMPathExtractor, LightRAG-style KG extraction, Russian-output guarantee

### Community 262 - "runbook: Milvus chunk vector index"
Cohesion: 0.33
Nodes (7): ADR-0006: Milvus HNSW as default chunk index, Milvus chunk vector index, doc_id backfill for legacy Milvus chunks, Hermes Agent integration, atomic_tools.py (pure retrieval functions), BoundedLLM GPU semaphore, MCP-2 atomic tools server

### Community 263 - "Runbook index"
Cohesion: 0.43
Nodes (7): ADR-0011: Plan-execute SearchOrchestratorWorkflow, Graph analytics layer (plan->compute->synthesize, 42 primitives), Analytics materialization (AnalyticsMaterializeWorkflow), Monitoring Arc-2, MCP-1 search server (5 tools via Temporal), Runbook index, Search modes /search/{local,global,drift,auto}

### Community 264 - "superpowers: GET /api/v1/documents/{doc_id} download endpoint"
Cohesion: 0.29
Nodes (7): Source document download design, DocumentRef search-response document links, GET /api/v1/documents/{doc_id} download endpoint, MinioStorage stream_object / stat_object, Wiki article quality + source-download links design, relation ranking+cap (WIKI_MAX_RELATIONS) + citation dedup, Источники source-download section (deterministic)

### Community 265 - "superpowers: Hermes ↔ kb-llamaindex RAG integration"
Cohesion: 0.29
Nodes (7): Hermes ↔ kb-llamaindex RAG integration design, Hermes Agent (persistent MCP-client consumer), knowledge-base SKILL.md (tool-selection + templates), kb MCP-2 tools_server (6 atomic tools, SSE), client-managed bounded history (stateless), contextualize_query activity (follow-up → standalone), Conversation history (multi-turn search) design

### Community 266 - "build_property_graph_index()"
Cohesion: 0.29
Nodes (7): SchemaLLMPathExtractor, build_property_graph_index(), BaseEmbedding, LLM, PropertyGraphIndex, PropertyGraphStore, Compose a PropertyGraphIndex from store + embed + extractor.      Pass ``nodes``

### Community 267 - "check_ingestion.py"
Cohesion: 0.48
Nodes (6): check_events(), check_milvus(), check_neo4j(), check_postgres(), main(), Diagnostic — show what landed in each backend after ingestion.  Pings every stor

### Community 268 - "test_config: WikiSettings"
Cohesion: 0.38
Nodes (5): Continuous wiki-article editor (Project A). Generates per-entity     MediaWiki p, WikiSettings, test_wiki_settings_defaults(), test_wiki_settings_env_override(), test_wiki_settings_new_fields_defaults()

### Community 270 - "ingestion: _extract_addresses()"
Cohesion: 0.29
Nodes (7): _extract_addresses(), _normalize_address(), _normalize_address_rule(), Lowercase + abbreviation expansion + whitespace cleanup., libpostal-based parse → structured fields → canonical assembly.      Falls back, Stop at the earliest natural address terminator.      Without this the 200-char, _truncate_address_window()

### Community 271 - "retrieval: pick_priority()"
Cohesion: 0.38
Nodes (4): pick_priority(), Canonical channel-group enum, imported by ingest/search/rerank/tg_ingest.  A doc, Return whichever group name ranks earlier in GROUP_PRIORITY.     Unknown names s, test_pick_priority_returns_earlier_in_order()

### Community 273 - "test_api: test_graph_admin.py"
Cohesion: 0.52
Nodes (6): _key(), _post(), ASGI tests for the /admin/graph/* analysis endpoints (Track 7b)., test_graph_stats_endpoint_returns_analysis(), test_graph_stats_requires_api_key(), test_pagerank_endpoint_wraps_top()

### Community 274 - "test_mcp: test_hermes_skill.py"
Cohesion: 0.43
Nodes (5): Validates the Hermes knowledge-base skill: frontmatter is present and the body r, _split_frontmatter(), test_skill_body_references_every_tool(), test_skill_covers_the_four_pillars(), test_skill_frontmatter_has_name_and_description()

### Community 275 - "test_mcp: test_search_server.py"
Cohesion: 0.29
Nodes (3): Smoke tests for the MCP-1 search server.  We don't talk to a real Temporal clust, assert_api_key_env_set raises SystemExit when require=true and     api_keys list, test_auth_gate_blocks_when_keys_missing()

### Community 276 - "test_workflow: test_completion_no_walltime_cap.py"
Cohesion: 0.38
Nodes (6): _activity_block(), Guard: LLM-bound ingest activities are bounded by ATTEMPT COUNT, not wall-clock., Permanently-failing docs must give up and free their slot (incident #2)., test_extract_kg_has_no_walltime_cap(), test_merge_and_resolve_has_no_walltime_cap(), test_retry_policies_bounded_to_max_attempts()

### Community 277 - "knowledge-base Hermes skill (routes to kb-llamaindex"
Cohesion: 0.40
Nodes (6): Hermes MCP servers config example (SSE, 30-min timeout), kbsearch MCP server (:9001/sse; kb_search/global/drift/auto), kbtools MCP server (:9002/sse; vector/graph_search/graph_walk/find_* tools), knowledge-base Hermes skill (routes to kb-llamaindex retrieval tools), Entity dossier answer template (Russian), Russian sample news text fixture (entities: ООО Ромашка, phones, INN-style)

### Community 278 - "tg_ingest.py: load_state()"
Cohesion: 0.40
Nodes (6): load_state(), Path, Read the sync-state JSON ({dialog_id: {last_id, title}}); {} if absent., Atomic-ish write (tmp + replace) so a Ctrl-C can't truncate the state., save_state(), test_state_roundtrip()

### Community 280 - "test_api: test_admin_wiki.py"
Cohesion: 0.33
Nodes (5): ASGI tests for POST /admin/wiki/rebuild.  The WIKI_ENABLED default is False, so, WIKI_ENABLED defaults False → route returns disabled without infra., `?all=true` (when enabled) marks every entity dirty via     build_wiki_graph_ops, test_wiki_rebuild_all_routes_mark_all_dirty_through_seam(), test_wiki_rebuild_returns_disabled_when_off()

### Community 281 - "test_api: test_monitor_route.py"
Cohesion: 0.33
Nodes (5): ASGI tests for POST /admin/monitor/sweep and /admin/monitor/watch.  The MONITOR_, MONITOR_ENABLED defaults False → returns disabled without touching Temporal., POST /admin/monitor/watch patches mark_watched and build_neo4j_graph_store., test_monitor_sweep_returns_disabled_when_off(), test_monitor_watch_calls_mark_watched()

### Community 282 - "test_api: test_route_skeletons.py"
Cohesion: 0.33
Nodes (5): Route skeleton tests — confirm the search endpoints are registered.  R7b cutover, The plan-execute / GraphRAG endpoints are wired; without an API     key (or with, R7b: the legacy ReAct + judge-based routes are gone → 404., test_legacy_search_routes_removed(), test_new_search_routes_registered()

### Community 283 - "test_scripts: test_setup_db.py"
Cohesion: 0.33
Nodes (3): Stage-1 unit tests for ``scripts/setup_db.py``.  Live-DB integration is verified, Idempotency contract: re-running the script against an existing     DB must not, test_documents_ddl_uses_create_if_not_exists()

### Community 284 - "CONCEPTS.md: ANN vector search"
Cohesion: 0.50
Nodes (5): Milvus vector store, ANN vector search, FLAT vs HNSW (Milvus index), RAG (Retrieval-Augmented Generation), Text embeddings

### Community 285 - "INGEST.md: GraphBuildWorkflow (child)"
Cohesion: 0.40
Nodes (5): MinIO (claim-check + uploads), Claim-check pattern (MinIO staging), GraphBuildWorkflow (child), Graph half (extract→merge/ER→Neo4j), vector_only degradation

### Community 288 - "graph: NoOpKGExtractor"
Cohesion: 0.40
Nodes (3): NoOpKGExtractor, TransformComponent, Identity extractor used when KG_NODES_KEY metadata is already     populated upst

### Community 289 - "test_ingestion: build_custom_kg_payload()"
Cohesion: 0.40
Nodes (5): build_custom_kg_payload(), Assemble a ``rag.ainsert_custom_kg`` payload from identifier matches.      One e, test_build_custom_kg_payload_dedupes_within_doc(), test_build_custom_kg_payload_empty_when_no_idents(), test_build_custom_kg_payload_structure()

### Community 290 - "retrieval: _injected_params()"
Cohesion: 0.40
Nodes (5): build_tool_schema(), _injected_params(), BaseModel, Names of the DI-injected dependency args.      Every tool function takes its dep, Pydantic schema of the LLM-facing kwargs for ``name``.      Derived from the rea

### Community 293 - "test_graph: _Boom"
Cohesion: 0.40
Nodes (4): _Boom, test_upsert_alert_is_fail_soft(), test_pagerank_nebula_fail_soft(), test_ensure_er_vector_index_ddl_and_failopen()

### Community 294 - "test_workflow: test_search_pooled_llm.py"
Cohesion: 0.50
Nodes (3): _fake_llm(), _pool(), Regression: every search-side LLM accessor goes through the LLM pool, so the glo

### Community 295 - "CAPACITY_TUNING.md: LLM_POOL_N throttle"
Cohesion: 0.50
Nodes (4): Congestion collapse (root cause of hangs), K+N concurrency model, LLM_POOL_N throttle, Document admission control (IngestSchedulerWorkflow)

### Community 296 - "superpowers: Graph-scale follow-ups (items 8/13/12) plan"
Cohesion: 0.50
Nodes (4): Community.level + Chunk.doc_id indexes, Drift graceful fallback to local answer, Dual walk-seed (graph_search + fulltext), Graph-scale follow-ups (items 8/13/12) plan

### Community 297 - "superpowers: GraphScope community backend (distributed single-level"
Cohesion: 0.50
Nodes (4): GraphScope community backend (Phase 4) implementation plan, GraphScope community backend (distributed single-level Leiden), _run_graphscope_community adapter (lazy, manual-gate), single_level_rows_graphscope mapping

### Community 298 - "superpowers: Analytics query fixes implementation plan"
Cohesion: 0.50
Nodes (4): coerce_entity_type (bogus planner type → None), Analytics query fixes implementation plan, is_meaningful_entity quality gate (drop identifiers + degenerate names), trend-fallback keyword broadening ('упомин')

### Community 299 - "superpowers: scripts/make_env.py builder"
Cohesion: 0.50
Nodes (4): cross-field validation (MILVUS_DIM, LLM_POOL sizing, Temporal caps), Interactive .env builder design, scripts/make_env.py builder, parse_example structure-preserving model (Line/KV)

### Community 300 - "Grafana datasources provisioning (Prometheus prom-kb +"
Cohesion: 0.50
Nodes (4): Grafana dashboards provisioning (kb-llamaindex file provider), Grafana datasources provisioning (Prometheus prom-kb + Postgres-kb), Prometheus dev scrape config (temporal-worker host.docker.internal:9090), Prometheus prod scrape config (7 worker pools 9090-9096)

### Community 301 - "smoke.sh"
Cohesion: 0.83
Nodes (3): curl_ok(), section(), smoke.sh script

### Community 302 - "conftest.py"
Cohesion: 0.50
Nodes (3): _pin_graph_backend_neo4j(), Pytest-asyncio mode is set globally in pyproject.toml.  Project-wide fixtures la, Post-cutover the PROD default is nebula (see src/config.py), but the     DB-free

### Community 304 - "test_graph: _is_exempt()"
Cohesion: 0.67
Nodes (3): _is_exempt(), Path, test_no_direct_neo4j_store_calls_outside_store_py()

### Community 306 - "ARCHITECTURE.md: Postgres (documents, ingest_metrics)"
Cohesion: 0.67
Nodes (3): Postgres (documents, ingest_metrics), init service (setup_db), Multimodel snapshot at /ingest

### Community 307 - "superpowers: Wiki article quality + source-download"
Cohesion: 0.67
Nodes (3): Wiki article quality + source-download links plan, Relation rank/cap + per-doc citation dedup, Deterministic Источники source-download section

### Community 308 - "superpowers: Marp two-version deck (A tech"
Cohesion: 0.67
Nodes (3): kb-llamaindex conference deck design, Marp two-version deck (A tech / D internal), Three-endpoints narrative (/search, /agent, /selfrag)

### Community 309 - "superpowers: TG → ingest test harness"
Cohesion: 1.00
Nodes (3): TG → ingest test harness — design, RabbitMQ in dev compose (default service; validates INGEST_QUEUE_BACKEND=rabbitmq end-to-end), scripts/tg_ingest.py (Telethon backfill → POST /api/v1/ingest per message)

### Community 310 - "graph: read_alerts()"
Cohesion: 0.67
Nodes (3): Any, Read persisted :Alert rows (backend-dispatched); fail-soft → []., read_alerts()

## Ambiguous Edges - Review These
- `Neo4j property graph store` → `Production docker-compose`  [AMBIGUOUS]
  docker-compose.prod.yml · relation: conceptually_related_to
- `ADR-0015: Community-detection backend = in-worker leidenalg` → `Leiden community-detection diagnostics`  [AMBIGUOUS]
  docs/runbook/leiden-diagnostics.md · relation: conceptually_related_to
- `ReAct agent (/agent, react_agent.py)` → `SearchOrchestratorWorkflow`  [AMBIGUOUS]
  docs/superpowers/plans/2026-05-25-agentic-search.md · relation: conceptually_related_to

## Knowledge Gaps
- **198 isolated node(s):** `kb-llamaindex`, `start.sh script`, `DepthProbe`, `Scenario`, `Neo4j property graph & indexes` (+193 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **32 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `Neo4j property graph store` and `Production docker-compose`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `ADR-0015: Community-detection backend = in-worker leidenalg` and `Leiden community-detection diagnostics`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `ReAct agent (/agent, react_agent.py)` and `SearchOrchestratorWorkflow`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **Why does `get_llm_pool()` connect `workflow: get_llm_pool()` to `test_workflow: test_search_community.py`, `workflow: global_search.py`, `retrieval: BoundedLLM`, `workflow: contracts.py`, `test_workflow: activities.py`, `test_workflow: Ctx`, `test_ingestion: test_classifier.py`, `mcp: tools_server.py`, `test_graph: test_index.py`, `test_workflow: merge_and_resolve()`, `di: providers.py`, `retrieval: build_vector_index()`, `workflow: wiki_sweep.py`, `reresolve_graph.py`, `workflow: _search_deps.py`, `test_graph: write_entity_article()`, `workflow: test_search_route.py`, `test_analytics: test_claim_nli.py`, `workflow: contextualize.py`?**
  _High betweenness centrality (0.038) - this node is a cross-community bridge._
- **Why does `entity_vid()` connect `test_graph: entity_vid()` to `test_graph_edge_export.py`, `graph: _q()`, `test_graph: test_wiki_graph_ops.py`, `test_graph: NebulaGraphStore`, `test_graph: test_alert_store.py`, `test_graph: test_communities_graph_ops.py`, `graph_edge_export.py`, `graph: test_community_summarize.py`, `graph: admin.py`, `er_graph_ops.py`, `test_retrieval: RoundGraphData`, `test_graph: test_nebula_store_writes.py`, `graph: events_llm.py`, `graph: alert_store.py`, `graph: communities.py`, `workflow: materialize_activities.py`, `test_graph: test_er_graph_ops.py`, `GraphRetriever`, `test_graph: test_events_llm_graph_ops.py`, `graph: community_writeback.py`, `NebulaEventsLlmGraphOps`, `analytics: materialize.py`, `test_graph: stamp_first_seen()`?**
  _High betweenness centrality (0.031) - this node is a cross-community bridge._
- **Why does `build_graph_store()` connect `build_graph_store()` to `test_workflow: test_search_community.py`, `workflow: global_search.py`, `workflow: get_llm_pool()`, `test_workflow: graph_admin.py`, `test_graph: index.py`, `test_workflow: contracts.py`, `storage: push_entities()`, `test_graph: NebulaGraphStore`, `workflow: test_search_drift_roundtrip.py`, `test_workflow: activities.py`, `test_workflow: Ctx`, `graph: admin.py`, `test_workflow: push_wikibase.py`, `mcp: tools_server.py`, `graph: communities.py`, `test_graph: test_index.py`, `test_workflow: merge_and_resolve()`, `merge_identifier_duplicates.py`, `setup_wikibase.py`, `workflow: wiki_sweep.py`, `reresolve_graph.py`, `workflow: _search_deps.py`, `workflow: materialize_activities.py`, `test_graph: write_entity_article()`, `workflow: KGExtracted`, `backfill_er_vector.py: configure_logging()`?**
  _High betweenness centrality (0.030) - this node is a cross-community bridge._
- **Are the 54 inferred relationships involving `PrimitiveResult` (e.g. with `CountEntitiesParams` and `CountRelationshipsParams`) actually correct?**
  _`PrimitiveResult` has 54 INFERRED edges - model-reasoned connections that need verification._