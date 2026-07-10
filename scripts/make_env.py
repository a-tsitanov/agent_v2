"""Interactive `.env` builder from `.env.example` (comment-preserving).

Run: uv run python -m scripts.make_env   (see --help for flags)
"""

from __future__ import annotations

import argparse
import getpass
import re
import secrets
import sys
from dataclasses import dataclass, field
from pathlib import Path

# Upstream credentials the user must supply — never auto-minted (an
# invented value would mask the validation that requires a real one).
_UPSTREAM_CREDENTIALS = {"OPENAI_API_KEY"}


@dataclass
class Comment:
    text: str


@dataclass
class Blank:
    pass


@dataclass
class Section:
    title: str
    raw: str


@dataclass
class KV:
    key: str
    example_val: str
    comment_lines: list[str] = field(default_factory=list)
    section: str = ""


Line = Comment | Blank | Section | KV

# A section header looks like:  # ── Title text ───────────
_SECTION_RE = re.compile(r"^#\s*─+\s*(.*?)\s*─+\s*$")
# An active KEY=VALUE line (uppercase env key, no leading '#').
_KV_RE = re.compile(r"^([A-Z][A-Z0-9_]*)=(.*)$")


def parse_example(text: str) -> list[Line]:
    """Parse `.env.example` text into an ordered list of Line records.

    Every source line becomes exactly one Line (so render can reproduce
    the file verbatim).  Each KV also captures the contiguous comment
    block directly above it (for prompting) and its current section.
    """
    lines: list[Line] = []
    section = ""
    recent: list[str] = []  # contiguous comments since last blank/kv/section
    parts = text.split("\n")
    if text.endswith("\n"):
        parts = parts[:-1]  # drop the empty artifact from the trailing newline
    for raw in parts:
        stripped = raw.strip()
        m_sec = _SECTION_RE.match(raw)
        if m_sec:
            section = m_sec.group(1)
            lines.append(Section(title=section, raw=raw))
            recent = []
        elif stripped == "":
            lines.append(Blank())
            recent = []
        elif raw.lstrip().startswith("#"):
            lines.append(Comment(text=raw))
            recent.append(raw)
        else:
            m_kv = _KV_RE.match(raw)
            if m_kv:
                lines.append(
                    KV(
                        key=m_kv.group(1),
                        example_val=m_kv.group(2),
                        comment_lines=list(recent),
                        section=section,
                    )
                )
                recent = []
            else:
                lines.append(Comment(text=raw))
                recent = []
    return lines


def render(lines: list[Line], values: dict[str, str]) -> str:
    """Re-emit the parsed file; KV lines take values[key] (fallback to the
    example default).  Comments / blanks / sections are verbatim."""
    out: list[str] = []
    for ln in lines:
        if isinstance(ln, Comment):
            out.append(ln.text)
        elif isinstance(ln, Blank):
            out.append("")
        elif isinstance(ln, Section):
            out.append(ln.raw)
        elif isinstance(ln, KV):
            out.append(f"{ln.key}={values.get(ln.key, ln.example_val)}")
    return "\n".join(out) + "\n"


def parse_env(text: str) -> dict[str, str]:
    """Simple KEY=VALUE reader for an existing .env (no interpolation).
    Skips blanks and comment lines; splits on the first '='."""
    out: dict[str, str] = {}
    for raw in text.split("\n"):
        s = raw.strip()
        if not s or s.startswith("#") or "=" not in raw:
            continue
        key, _, val = raw.partition("=")
        key = key.strip()
        if _KV_RE.match(f"{key}="):
            out[key] = val
    return out


_SECRET_MARKERS = ("PASSWORD", "PASS", "SECRET", "API_KEY", "API_KEYS", "ACCESS_KEY", "_KEY")


def is_secret(key: str) -> bool:
    """Name heuristic: does this var hold a secret/credential?"""
    k = key.upper()
    return any(m in k for m in _SECRET_MARKERS)


@dataclass
class EnvVar:
    env: str
    default: str  # rendered default ("" for secrets / None / undefined)
    secret: bool
    group: str  # settings class name (for grouping)


def _render_default(value) -> str:
    import json

    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def iter_app_env_vars() -> list[EnvVar]:
    """Every env var the app reads, from config.py BaseSettings classes.

    Env name = env_prefix + UPPER(field), or the field's explicit
    ``validation_alias`` (HFSettings).  Secrets are flagged and emitted with
    an empty default (never a real secret).
    """
    import importlib
    import inspect

    from pydantic import SecretStr
    from pydantic_core import PydanticUndefined
    from pydantic_settings import BaseSettings

    cfg = importlib.import_module("src.config")
    rows: list[EnvVar] = []
    seen: set[str] = set()
    for name, cls in vars(cfg).items():
        if not (inspect.isclass(cls) and issubclass(cls, BaseSettings)):
            continue
        if cls is BaseSettings or name == "Settings":
            continue
        prefix = cls.model_config.get("env_prefix", "") or ""
        for fname, fld in cls.model_fields.items():
            alias = getattr(fld, "validation_alias", None)
            env = (alias if isinstance(alias, str) else (prefix + fname)).upper()
            if env in seen:
                continue
            seen.add(env)
            if fld.default is not PydanticUndefined and fld.default is not None:
                raw = fld.default
            elif fld.default_factory is not None:
                raw = fld.default_factory()
            else:
                raw = None
            is_sec = isinstance(raw, SecretStr) or is_secret(env)
            default = "" if (is_sec or isinstance(raw, SecretStr)) else _render_default(raw)
            rows.append(EnvVar(env=env, default=default, secret=is_sec, group=name))
    rows.sort(key=lambda r: (r.group, r.env))
    return rows


_REFERENCE_HEADER = (
    "# Generated from src/config.py by `python -m scripts.make_env --reference`.\n"
    "# DO NOT EDIT BY HAND. Exhaustive catalog of every app env var.\n"
    "# Secrets show an empty value (set them yourself).\n"
)


# Russian one-line description per env var.  Curated here (not hand-edited
# into the generated file) so `--check` keeps the reference in sync.  Source
# of truth is the inline comments / field names in src/config.py.  A coverage
# guard test asserts EVERY iter_app_env_vars() var has an entry here.
_ENV_DESCRIPTIONS: dict[str, str] = {
    # ── GraphSettings (GRAPH_*) — выбор графового бэкенда ────────────
    "GRAPH_BACKEND": "Графовый бэкенд, который строит фабрика store (strangler-шов миграции Neo4j→NebulaGraph): 'neo4j' (текущий) или 'nebula'. Дефолт 'neo4j' до прохождения бенчмарка паритета.",
    # ── AgentSettings (AGENT_*) — ручки search-эндпоинтов ────────────
    "AGENT_COMMUNITY_DYNAMIC_SELECTION": "Стратегия выбора сообществ для global/drift: lexical (по умолчанию) | semantic (kNN по report_vec) | descent (спуск по иерархии).",
    "AGENT_COMMUNITY_MAX_LEVELS": "Сколько уровней дендрограммы Leiden материализовать при build сообществ; 1 = одноуровневый, выше = иерархия (offline). Капа 1..10.",
    "AGENT_COMMUNITY_VECTOR_BACKEND": "Где живут community-report вектора для semantic community-select: 'native' (Neo4j in-graph индекс) или 'milvus' (коллекция community_report_vec). Под GRAPH_BACKEND=nebula форсится 'milvus'. Дефолт 'native'.",
    "AGENT_CONVERSATION_HISTORY_ENABLED": "Учитывать историю диалога (мультитёрн): прежние реплики переписывают запрос в standalone-форму перед поиском. Пустая история = single-shot.",
    "AGENT_COVERAGE_CHECK_ENABLED": "Pre-submit coverage-check: перед ответом LLM судит, покрывают ли собранные данные вопрос; при пробеле делается ещё один раунд поиска.",
    "AGENT_ER_ENABLED": "Entity Resolution: дополнительный шаг между merge_kg_extraction и PropertyGraphIndex, схлопывающий семантические дубликаты в один canonical-сущность.",
    "AGENT_ER_JUDGE_BATCH_SIZE": "Сколько пар за один вызов LLM-судьи, когда ER маршрутизирует пограничных кандидатов. Капа 1..50.",
    "AGENT_ER_USE_NATIVE_VECTOR_KNN": "Для ER нативный Neo4j vector-index kNN вместо окна на 5000 сущностей (окно теряет кросс-док совпадения по мере роста графа). По умолчанию ON, fail-safe. Для СУЩЕСТВУЮЩИХ графов прогнать backfill_er_vector.py. false → вернуть legacy-окно.",
    "AGENT_ER_VECTOR_BACKEND": "Где живут ER-kNN вектора: 'native' (Neo4j in-graph vector index, текущий прод-путь) или 'milvus' (коллекция entity_er_vec). Под GRAPH_BACKEND=nebula форсится 'milvus'. Дефолт 'native'.",
    "AGENT_ER_VECTOR_KNN_K": "Сколько соседей тянуть на новую сущность из ER vector-index, когда включён нативный kNN. Капа 1..100.",
    "AGENT_ER_VERDICT_CACHE_ENABLED": "Кэш вердиктов ER в Neo4j (:ERVerdict): повторяющиеся пары пропускают LLM. Опционально и fail-safe: при ошибке Neo4j откат на чистое LLM-судейство.",
    "AGENT_GLOBAL_MAX_COMMUNITIES": "Сколько summary сообществ максимум входит в (параллельный) MAP-шаг global-поиска, чтобы корпус не разлетался безгранично. Капа 1..200.",
    "AGENT_GRAPH_SEARCH_PATH_DEPTH": "path_depth для similarity graph_search: сколько triplet-хопов соседей тянуть вокруг каждой найденной сущности. По умолчанию 1, капа 1..3.",
    "AGENT_GRAPH_SIMILARITY_TOP_K": "top_k кандидатов для graph-ретривера (VectorContextRetriever); поднят, чтобы именованная сущность не выпадала из выдачи на большом графе. Капа 1..100.",
    "AGENT_GRAPH_WALK_DUAL_SEED": "Сеять graph_walk сразу от топ-сущности graph_search И от топ find_entity_by_name (fulltext), когда они различаются — добавляет окрестность fulltext-совпадения.",
    "AGENT_GRAPH_WALK_ENABLED": "Multi-hop seeding: после graph_search авто-засеять bounded graph_walk от топ-сущности (без LLM tool-pick). Fail-open: ошибка walk проглатывается.",
    "AGENT_GRAPH_WALK_FILTER_POLARITY_TEMPORAL": "Фильтрация на retrieval: graph_walk выкидывает отрицаемые (polarity=negated) связи и рёбра с истёкшим valid_to. Opt-out, если мешает.",
    "AGENT_GRAPH_WALK_HOPS": "Запрошенное число хопов для graph_walk (инструмент клампит к GRAPH_WALK_MAX_HOPS). По умолчанию 2, капа 1..3.",
    "AGENT_HISTORY_MAX_CHARS": "Лимит символов истории диалога, передаваемой в контекстуализацию запроса. 0 = без истории. Капа >= 0.",
    "AGENT_HISTORY_MAX_TURNS": "Сколько последних реплик истории учитывать при контекстуализации запроса. 0 = single-shot. Капа 0..40.",
    "AGENT_MAX_COVERAGE_ROUNDS": "Макс. число доп. раундов coverage-check в plan-execute: на найденный пробел запускается ещё один SubQueryRetrievalWorkflow. Капа 0..3.",
    "AGENT_MAX_SUBQUERIES": "Макс. число подвопросов, которые может выдать планировщик — ограничивает параллельный fan-out SubQueryRetrievalWorkflow и стоимость планировщика. Капа 1..20.",
    # ── AnalyticsSettings (ANALYTICS_*) — теги версий ingest-метрик ───
    "ANALYTICS_CYPHER_FALLBACK_ENABLED": "Разрешить фолбэк на text-to-Cypher при отсутствии подходящего примитива (v1c; по умолчанию выключено).",
    "ANALYTICS_DEFAULT_TOP_N": "Максимальное число строк, возвращаемых аналитическим запросом по умолчанию (top-N).",
    "ANALYTICS_DEFAULT_VERSION_TAG": "Версия-тег по умолчанию, если на /ingest не пришёл заголовок X-Version-Tag; пишется в ingest_metrics / Temporal search attributes.",
    "ANALYTICS_ENV_NAME": "Имя окружения-деплоя для меток (Temporal search attributes и строки Postgres ingest_metrics).",
    "ANALYTICS_MAX_STEPS": "Максимальное число примитивных вызовов в одном аналитическом плане.",
    # ── ApiSettings (API_*) — поверхность FastAPI ────────────────────
    "API_CORS_ORIGINS": "Разрешённые CORS-origin через запятую; '*' = любой.",
    "API_ENV": "Окружение приложения (development|production); в production preflight жёстко требует реальные секреты.",
    "API_KEYS": "Ключи API через запятую (клиенты шлют в заголовке X-API-Key). В проде задать реальные, не плейсхолдеры. Секрет.",
    "API_LOG_JSON": "Логировать в JSON (true) или в человекочитаемом виде (false).",
    "API_LOG_LEVEL": "Уровень логирования FastAPI (info|debug|warning|...).",
    "API_UPLOAD_DIR": "Каталог для загруженных файлов на стороне API.",
    # ── ClassifierSettings (CLASSIFIER_*) — фильтр входных документов ─
    "CLASSIFIER_ENABLED": "Включить классификатор входных документов (отсев мусора до пайплайна). По умолчанию off; fail-soft: ошибка → INGEST.",
    "CLASSIFIER_LLM_ENABLED": "Включить LLM-слой классификатора поверх детерминированных правил (оценка по ограниченному preview).",
    "CLASSIFIER_MAX_SIZE_MB": "Максимальный размер документа (МБ); крупнее — отсев детерминированным правилом.",
    "CLASSIFIER_MIN_SIZE_BYTES": "Минимальный размер документа (байт); мельче — отсев (пустышка/мусор).",
    "CLASSIFIER_PREVIEW_CHARS": "Сколько символов превью документа подаётся LLM-слою классификатора.",
    "CLASSIFIER_SKIP_EXTENSIONS": "JSON-список расширений, отсекаемых детерминированным правилом (exe, zip, png, mp4 и т.п.).",
    # ── EventsSettings (EVENTS_*) — детекция событий first_seen ─────────
    "EVENTS_BACKFILL_SENTINEL": "Метка эпохи-дня для узлов, созданных до включения first_seen (маркер бэкфила).",
    "EVENTS_EXTRACTION_ENABLED": "Извлечение структурных LLM-событий в extract_kg (E2; по умолчанию вкл — удлиняет промпт/вывод на каждый чанк, выключать при нехватке LLM-бюджета).",
    "EVENTS_FIRST_SEEN_ENABLED": "Включить простановку метки first_seen при создании узла (переключать ТОЛЬКО после бэкфила).",
    "EVENTS_NEW_WINDOW_DAYS": "Окно в днях для выборки новых событий (new_events) по умолчанию.",
    "EVENTS_TAXONOMY": "Закрытый список типов событий (event_type) для LLM-извлечения; с открытым fallback для длинного хвоста.",
    # ── HFSettings (явные имена без префикса) — offline HF-модели ─────
    "HF_CACHE_DIR": "Путь к локальному HF-кэшу для air-gapped деплоя; пусто = дефолт HF. Связано с download_models.py / configure_hf.",
    "HF_OFFLINE": "Включить offline-режим HuggingFace (читать только из локального кэша, без обращений к Hub).",
    "HF_RERANK_MODEL": "Имя BGE cross-encoder reranker-модели (по умолчанию BAAI/bge-reranker-v2-m3) для unified graph+vector rerank.",
    # ── IngestAdmissionSettings (INGEST_ADMISSION_*) — допуск документов
    "INGEST_ADMISSION_MAX_INFLIGHT": "K в модели K+N: максимум документов в работе одновременно (FIFO-допуск через singleton IngestSchedulerWorkflow). Капа >= 1.",
    # ── IngestionSettings (INGESTION_*) — пайплайн ингеста ───────────
    "INGESTION_BREAKPOINT_PERCENTILE": "Перцентиль расстояния для границ в semantic chunking (выше = реже резать). Действует при INGESTION_SEMANTIC_CHUNKING=true.",
    "INGESTION_CACHE_DIR": "Каталог кэша ingest-пайплайна.",
    "INGESTION_CHUNK_OVERLAP": "Перекрытие соседних чанков (в токенах) для SentenceSplitter.",
    "INGESTION_CHUNK_SIZE": "Размер чанка (в токенах) для SentenceSplitter.",
    "INGESTION_GLINER_MODEL": "GLiNER span-NER модель для opt-in режимов gliner / gliner+llm; требует extra 'gliner'. Дефолтный путь (lightrag) её не трогает.",
    "INGESTION_SEMANTIC_CHUNKING": "Вместо SentenceSplitter использовать SemanticSplitter (резать по сдвигам темы). Доп. embedding-вызовы за лучшую точность retrieval. По умолчанию off.",
    "INGESTION_TRANSLATE_TO_RUSSIAN": "Per-chunk LLM-перевод в metadata['translated_text'] (исходный текст не меняется); KG-экстрактор читает перевод → сущности по-русски для кросс-язычного дедупа.",
    "INGESTION_TRANSLATION_CONCURRENCY": "Сколько LLM-вызовов перевода идут параллельно.",
    "INGESTION_TRANSLATION_DOC_THRESHOLD_CHARS": "Мягкий лимит символов для перевода документа одним вызовом; выше — режется на абзац-выровненные окна. Дефолт 30k под Ollama qwen3 32k-контекст.",
    "INGESTION_TRANSLATION_STRATEGY": "Как делить работу перевода: per_document | per_chunk | auto (по умолчанию; per_document под порогом, иначе per_chunk).",
    # ── LLMPoolSettings (LLM_POOL_*) — пул LLM-конкуренции ───────────
    "LLM_POOL_N": "N в модели K+N: максимум одновременных LLM-вызовов на процесс (единый глобальный семафор на все роли). Капа >= 1.",
    # ── LiteLLMSettings (LITELLM_*) — подключение к LiteLLM-прокси ────
    "LITELLM_API_KEY": "Ключ к LiteLLM-прокси (OpenAI-совместимый). Секрет.",
    "LITELLM_BASE_URL": "Base URL LiteLLM-прокси (или любого OpenAI-совместимого эндпоинта).",
    "LITELLM_EMBEDDING_MODEL": "Имя embedding-модели; её native-dim ДОЛЖНА совпадать с MILVUS_DIM (text-embedding-3-small → 1536).",
    "LITELLM_EXTRA_BODY": 'JSON доп.полей тела КАЖДОГО chat-запроса (через OpenAI extra_body), напр. {"think": false} чтобы выключить chain-of-thought Qwen3. Пусто ⇒ запрос не меняется.',
    "LITELLM_EXTRA_BODY_ROLES": 'JSON per-role override\'ов extra_body; shallow-мёрж поверх LITELLM_EXTRA_BODY, ключи роли побеждают. Напр. {"synthesis": {"think": true}} — оставить thinking только для финального ответа.',
    "LITELLM_LLM_MODEL": "DEPRECATED-алиас no-role legacy-пути; пусто ⇒ откат на model_small. Удалить, когда все читатели перейдут на tier-поля.",
    "LITELLM_MAX_RETRIES": "Сколько повторов на сетевую/временную ошибку LLM-вызова.",
    "LITELLM_MODEL_LARGE": "Имя 'large'-модели: только финальный user-facing synthesis (дефолт gpt-4o-mini).",
    "LITELLM_MODEL_SMALL": "Имя 'small'-модели: локальная высоконагруженная (extraction/judge/search/plan/...).",
    "LITELLM_ROLE_TIERS": 'JSON-override карты роль→tier; мёржится поверх дефолтов, можно эскалировать одну роль, напр. {"plan":"large"}.',
    "LITELLM_TIMEOUT_S": "Таймаут LLM-вызова в секундах (дефолт 900 — под медленный локальный inference).",
    # ── MetricsSettings (METRICS_*) — Prometheus-экспортёр воркера ────
    "METRICS_BIND_ADDRESS": "Адрес:порт, на котором воркер поднимает Prometheus-листенер; Prometheus скрейпит через host.docker.internal:<port>.",
    "METRICS_ENABLED": "Включить worker-side Prometheus-экспортёр (Temporal Runtime + PrometheusConfig).",
    # ── MilvusSettings (MILVUS_*) — векторное хранилище ──────────────
    "MILVUS_COLLECTION": "Имя коллекции чанков в Milvus.",
    "MILVUS_DIM": "Размерность вектора в Milvus; ДОЛЖНА совпадать с native-dim embedding-модели (1536 для text-embedding-3-small, 768 для nomic-embed-text).",
    "MILVUS_HNSW_EF_CONSTRUCTION": "HNSW build-time search width: выше → лучше recall, медленнее build. Действует при создании коллекции с index_type=HNSW.",
    "MILVUS_HNSW_EF_SEARCH": "HNSW query-time search width (ef): выше → лучше recall, медленнее запрос. Должно быть >= search top_k.",
    "MILVUS_HNSW_M": "Степень HNSW-графа (M): выше → лучше recall, больше памяти.",
    "MILVUS_HOST": "Хост Milvus (внутри compose — DNS-имя сервиса, напр. milvus).",
    "MILVUS_INDEX_TYPE": "Тип ANN-индекса коллекции: HNSW (по умолчанию, approximate) или FLAT (точный перебор). Действует только при (пере)создании коллекции.",
    "MILVUS_PORT": "Порт Milvus (по умолчанию 19530).",
    "MILVUS_TIMEOUT_S": "Таймаут обращений к Milvus в секундах.",
    # ── MinioSettings (MINIO_*) — S3-совместимое хранилище загрузок ──
    "MINIO_ACCESS_KEY": "Access key MinIO/S3. Секрет; в проде задать реальный.",
    "MINIO_BUCKET": "Бакет для пользовательских загрузок (kb-uploads).",
    "MINIO_DOWNLOAD_DIR": "Куда воркер стейджит скачанные файлы перед обработкой; чистится активити cleanup_local после run.",
    "MINIO_ENDPOINT": "Endpoint MinIO/S3 (host:port).",
    "MINIO_REGION": "Регион S3 (для совместимых клиентов).",
    "MINIO_SECRET_KEY": "Secret key MinIO/S3. Секрет; в проде задать реальный.",
    "MINIO_SECURE": "Использовать TLS (https) при обращении к MinIO/S3.",
    # ── MonitorSettings (MONITOR_*) — непрерывный мониторинг/алерты ──
    "MONITOR_ACTIVITY_CONCURRENCY": "Параллелизм активностей монитор-свипа. >= 1.",
    "MONITOR_BURST_BASELINE_WINDOWS": "Сколько предыдущих окон усреднять как базовую ставку burst. >= 1.",
    "MONITOR_BURST_ENABLED": "Включить burst-детектор событий в монитор-свипе (E3). По умолчанию off.",
    "MONITOR_BURST_MIN_COUNT": "Мин. число недавних событий, чтобы пара (сущность,тип) считалась всплеском. >= 1.",
    "MONITOR_BURST_RATIO": "Порог burst_score (recent/base) для алерта о всплеске (> 1).",
    "MONITOR_BURST_WINDOW_DAYS": "Окно в днях для подсчёта недавних событий в burst-детекторе. >= 1.",
    "MONITOR_DELIVER_BATCH": "Сколько непушенных алертов доставлять за один свип. >= 1.",
    "MONITOR_ENABLED": "Включить непрерывный мониторинг и алерты (Arc 2). По умолчанию off.",
    "MONITOR_NEW_WINDOW_DAYS": "Окно в днях для детекта новых first_seen-связей при свипе. >= 1.",
    "MONITOR_RISK_RISE_DELTA": "Порог роста risk_score для генерации алерта (0 < значение <= 1).",
    "MONITOR_SWEEP_INTERVAL_MINUTES": "Период Temporal-Schedule монитор-свипа в минутах. >= 1.",
    "MONITOR_TASK_QUEUE": "Имя очереди Temporal для воркера монитор-свипа.",
    "MONITOR_WEBHOOK_TIMEOUT_S": "Таймаут POST на webhook доставки алертов, сек (> 0).",
    "MONITOR_WEBHOOK_URL": "URL генеричного webhook для доставки алертов (пусто — доставка выключена).",
    # ── NebulaSettings (NEBULA_*) — граф (Phase-1 write-path бэкенд) ──
    "NEBULA_HOST": "Хост NebulaGraph graphd.",
    "NEBULA_PASSWORD": "Пароль NebulaGraph. Секрет; в проде сменить дефолт 'nebula'.",
    "NEBULA_PORT": "Порт NebulaGraph graphd (по умолчанию 9669).",
    "NEBULA_SPACE": "Имя графового space в NebulaGraph.",
    "NEBULA_USER": "Пользователь NebulaGraph.",
    # ── Neo4jSettings (NEO4J_*) — граф ───────────────────────────────
    "NEO4J_DATABASE": "Имя базы Neo4j.",
    "NEO4J_PASSWORD": "Пароль Neo4j. Секрет; в проде сменить дефолт 'changeme'.",
    "NEO4J_CONNECTION_ACQUISITION_TIMEOUT_S": "Сколько секунд ждать свободное соединение из пула драйвера Neo4j, прежде чем сдаться (защита от вечного зависания под write-контеншном).",
    "NEO4J_CONNECTION_TIMEOUT_S": "Таймаут установления TCP-соединения с Neo4j, сек.",
    "NEO4J_MAX_CONNECTION_POOL_SIZE": "Размер пула соединений драйвера Neo4j на процесс (Track A write-tune).",
    "NEO4J_QUERY_LOG": "true → логировать каждый Cypher из приложения одной INFO-строкой (свёрнутый запрос, имена параметров, rows, ms) — подтверждать, что поиск ходит в граф.",
    "NEO4J_WRITE_RETRY_BASE_DELAY_S": "Базовая задержка экспоненциального ретрая write-транзакций Neo4j при deadlock/transient-ошибках, сек.",
    "NEO4J_WRITE_RETRY_MAX_ATTEMPTS": "Максимум попыток write-транзакции Neo4j при deadlock/transient-ошибках.",
    "INGEST_QUEUE_BACKEND": "Бэкенд очереди ингеста: temporal (singleton IngestSchedulerWorkflow) | rabbitmq (брокер + ingest-consumer, допуск prefetch=K). Дефолтный путь — rabbitmq.",
    "RABBITMQ_URL": "AMQP-URL брокера RabbitMQ для очереди ингеста (amqp://user:pass@host:5672/).",
    "RABBITMQ_QUEUES": "Список очередей ингеста через запятую (RABBITMQ_QUEUES=a,b); первая — дефолтная, /ingest выбирает явным параметром queue.",
    "RABBITMQ_DLX": "Имя dead-letter exchange для сообщений ингеста, исчерпавших обработку.",
    "RABBITMQ_DLQ": "Имя dead-letter очереди (парная к RABBITMQ_DLX).",
    "RABBITMQ_CONSUMER_TIMEOUT_MS": "x-consumer-timeout очереди, мс: сколько брокер ждёт ack по in-flight документу прежде чем закрыть канал (держать больше максимальной длительности ингеста одного документа).",
    "RABBITMQ_REQUEUE_ON_FAILURE": "true → возвращать сообщение в очередь при падении обработки (после ретраев), false → в DLQ.",
    "NEO4J_URI": "Bolt-URI Neo4j (напр. bolt://localhost:7687).",
    "NEO4J_USER": "Пользователь Neo4j.",
    # ── PostgresSettings (POSTGRES_*) — метаданные / ingest_metrics ──
    "POSTGRES_CONNECT_TIMEOUT_S": "Таймаут подключения к Postgres в секундах.",
    "POSTGRES_DB": "Имя базы Postgres.",
    "POSTGRES_HOST": "Хост Postgres.",
    "POSTGRES_PASSWORD": "Пароль Postgres. Секрет; в проде сменить дефолт 'postgres'.",
    "POSTGRES_POOL_MAX_SIZE": "Макс. размер пула коннектов приложения к Postgres НА ПРОЦЕСС; суммарный спрос ≈ значение × число процессов-воркеров — держать ниже max_connections с запасом под Temporal.",
    "POSTGRES_POOL_MIN_SIZE": "Мин. размер пула коннектов приложения; 0 → не держать простаивающих коннектов (создаются по требованию).",
    "POSTGRES_POOL_TIMEOUT_S": "Сколько секунд ждать свободный коннект из пула, прежде чем упасть с ошибкой.",
    "POSTGRES_PORT": "Порт Postgres (по умолчанию 5432).",
    "POSTGRES_USER": "Пользователь Postgres.",
    # ── SignalsSettings (SIGNALS_*) — качество знаний / actionable-сигналы
    "SIGNALS_EXPECTED_ATTRS": "Ожидаемые идентификаторы для оценки полноты данных по типу сущности (используется в completeness-сигнале).",
    "SIGNALS_LINK_PREDICTION_MIN_SCORE": "Минимальный similarity-score GDS node-similarity для записи ребра :LIKELY_LINK (0.0..1.0).",
    "SIGNALS_LINK_PREDICTION_TOP_K": "top-K соседей на узел для GDS node-similarity (link prediction). >= 1.",
    "SIGNALS_ORPHAN_MIN_DEGREE": "Минимальная степень узла графа, ниже которой он считается изолированным (орфаном).",
    "SIGNALS_RISK_BANDS": "Пороги полос composite risk_score: >=high → 'high', >=medium → 'medium', иначе 'low'. JSON-словарь {\"high\": 0.66, \"medium\": 0.33}.",
    "SIGNALS_RISK_WEIGHTS": "Веса компонентов composite risk_score (affiliation/brokerage/controversy/volatility/opacity); должны суммироваться к 1.0.",
    # ── TemporalSettings (TEMPORAL_*) — воркер/клиент Temporal ───────
    "TEMPORAL_ACTIVITY_CONCURRENCY": "Слотов активити на основной очереди kb-ingest.",
    "TEMPORAL_ANALYTICS_MATERIALIZE_CONCURRENCY": "GDS-воркеры для офлайн-материализации аналитики (centrality/link-prediction) на очереди kb-graph-build. >= 1.",
    "TEMPORAL_COMMUNITY_BACKEND": "Движок детекции сообществ: 'gds' (Leiden в Neo4j, легаси), 'leidenalg' (leidenalg/igraph в воркере, память вне Neo4j) или 'graphscope' (распределённый Leiden через GraphScope, вне Neo4j/igraph). Дефолт 'gds' до прохождения бенчмарка паритета.",
    "TEMPORAL_COMMUNITY_LEIDEN_CONCURRENCY": "Число GDS-потоков для прогона Leiden; держать умеренным, чтобы rebuild не голодил Neo4j. >= 1.",
    "TEMPORAL_COMMUNITY_LEIDEN_GAMMA": "Resolution Leiden: >1 → больше мелких сообществ, <1 → меньше крупных. Только детекция, не query-путь. > 0.",
    "TEMPORAL_COMMUNITY_MIN_SIZE": "Сообщества мельче этого порога игнорируются (слишком мелкие для осмысленного summary — шум).",
    "TEMPORAL_COMMUNITY_SUMMARY_PARALLELISM": "Bounded-параллелизм fan-out суммаризации сообществ внутри CommunityBuildWorkflow (независимо от worker-капа активити).",
    "TEMPORAL_GRAPH_BUILD_ACTIVITY_CONCURRENCY": "Слотов на выделенной очереди kb-graph-build (offline build сообществ). Намеренно низкий, чтобы не флудить LLM-прокси.",
    "TEMPORAL_GRAPH_BUILD_TASK_QUEUE": "Имя очереди для offline graph-community build (GDS Leiden + per-community summary); не трогает query hot-path.",
    "TEMPORAL_HOST": "Хост Temporal.",
    "TEMPORAL_LARGE_ACTIVITY_CONCURRENCY": "Слотов на очереди kb-search-large; намеренно НИЗКИЙ — тяжёлая large-модель не должна обслуживать много параллельных сессий.",
    "TEMPORAL_LARGE_TASK_QUEUE": "Имя очереди для large-tier финального synthesis (synthesize_answer пинится сюда).",
    "TEMPORAL_LLM_ACTIVITY_CONCURRENCY": "Слотов на очереди kb-ingest-llm; должно быть >= LLM_POOL_N, чтобы троттлил пул, а не Temporal.",
    "TEMPORAL_LLM_TASK_QUEUE": "Имя очереди для LLM-bound активити extract_kg (отдельно от основного ингеста).",
    "TEMPORAL_MERGE_ACTIVITY_CONCURRENCY": "Слотов на очереди kb-ingest-merge; должно быть >= LLM_POOL_N (иначе Temporal троттлит раньше пула).",
    "TEMPORAL_MERGE_TASK_QUEUE": "Имя очереди merge-стадии (merge_and_resolve + build_property_graph) — отдельно, чтобы merge не голодал за burst-ом extract.",
    "TEMPORAL_NAMESPACE": "Namespace Temporal.",
    "TEMPORAL_NAMESPACE_RETENTION_DAYS": "Срок хранения истории завершённых воркфлоу (setup_db применяет на init); ограничивает рост Postgres за счёт per-doc историй DocumentIngest/GraphBuild. 0 → не менять namespace.",
    "TEMPORAL_PORT": "Порт Temporal (по умолчанию 7233).",
    "TEMPORAL_RERANK_TOP_N": "top-N bge cross-encoder для unified graph+vector rerank перед дорогим large-tier synthesis.",
    "TEMPORAL_SEARCH_ACTIVITY_CONCURRENCY": "Слотов на очереди kb-search-small (plan-execute поток: planner + параллельный retrieval подвопросов). >= 1.",
    "TEMPORAL_SCHEDULER_TASK_QUEUE": "Имя очереди singleton-планировщика допуска (IngestSchedulerWorkflow); отдельный пул, чтобы его churn не мешал DocumentIngestWorkflow на main. Дети ингеста всё равно идут на TEMPORAL_TASK_QUEUE.",
    "TEMPORAL_SEARCH_TASK_QUEUE": "Имя очереди search-активити small-tier (plan/retrieve/coverage_check/rerank); раньше kb-search-llm.",
    "TEMPORAL_STAGING_BUCKET": "Имя staging-бакета Temporal.",
    "TEMPORAL_TASK_QUEUE": "Имя основной очереди ингеста (kb-ingest).",
    # ── WikiSettings (WIKI_*) — непрерывный редактор wiki-статей ─────
    "WIKI_ACTIVITY_CONCURRENCY": "Слотов активити на очереди kb-wiki (редактор статей). >= 1.",
    "WIKI_CITATIONS_TOP_K": "Сколько источников-цитат подтягивать в статью. >= 1.",
    "WIKI_DOCS_BASE_URL": "Base URL ссылок на документы-источники в секции 'Источники' (GET {docs_base_url}/documents/{doc_id}).",
    "WIKI_ENABLED": "Включить редактор wiki-статей (Project A): генерирует per-entity MediaWiki-страницы из графа Neo4j. По умолчанию off.",
    "WIKI_MAX_RELATIONS": "Капа 1-hop связей в промпте статьи (ранжируются по mention_count соседа). Ограничивает размер промпта для hub-сущностей. >= 1.",
    "WIKI_MEDIAWIKI_API_URL": "URL MediaWiki Action API; пусто → выводится из wikibase.base_url + '/w/api.php'.",
    "WIKI_SITE_GLOBAL_ID": "Site global id MediaWiki для sitelinks; должен совпадать с реальным id вики (дефолт под dev-compose).",
    "WIKI_SWEEP_BATCH": "Размер пачки сущностей за один проход sweep. >= 1.",
    "WIKI_SWEEP_INTERVAL_MINUTES": "Интервал sweep-прохода редактора статей (минуты). >= 1.",
    "WIKI_TASK_QUEUE": "Имя очереди Temporal для редактора wiki-статей (kb-wiki).",
    # ── WikibaseSettings (WIKIBASE_*) — наполнитель Wikibase ─────────
    "WIKIBASE_BASE_URL": "Base URL self-hosted Wikibase.",
    "WIKIBASE_BOT_PASSWORD": "Пароль бота Wikibase (>= 8 символов); им логинится push_wikibase. Создание бота — scripts/setup_wikibase.py. Секрет.",
    "WIKIBASE_BOT_USER": "Имя бот-пользователя Wikibase (по умолчанию KbBot).",
    "WIKIBASE_ENABLED": "Включить push в Wikibase после успешного graph build (canonical-сущности + связи + identifier-statements). По умолчанию off.",
    "WIKIBASE_LANGUAGE": "Язык лейблов/описаний в Wikibase (по умолчанию ru).",
    "WIKIBASE_TIMEOUT_S": "Таймаут обращений к Wikibase API в секундах.",
}


def build_reference() -> str:
    """Render the exhaustive .env.reference from the config.py catalog.

    Each variable is preceded by its Russian description (from
    ``_ENV_DESCRIPTIONS``) as a ``#`` comment line; a var missing an entry is
    emitted without a description (never crashes), but the coverage guard test
    ensures every var is covered.
    """
    rows = iter_app_env_vars()
    out = [_REFERENCE_HEADER.rstrip("\n")]
    group = None
    for r in rows:
        if r.group != group:
            group = r.group
            out.append(f"\n# ── {group} ──")
        desc = _ENV_DESCRIPTIONS.get(r.env)
        if desc:
            out.append(f"# {desc}")
        suffix = "   # secret" if r.secret else ""
        out.append(f"{r.env}={r.default}{suffix}")
    return "\n".join(out) + "\n"


def gen_secret(key: str) -> str:
    """Generate a sensible secret for `key` (opt-in per field)."""
    k = key.upper()
    if k == "WIKIBASE_SECRET_KEY":
        return secrets.token_hex(16)  # exactly 32 hex chars
    if "API_KEY" in k or k == "API_KEYS":
        return "sk-" + secrets.token_urlsafe(32)
    # passwords + everything else: long urlsafe token (>= 12 chars)
    return secrets.token_urlsafe(24)


@dataclass
class Issue:
    level: str  # "ERROR" | "WARN"
    msg: str


def _int(values: dict[str, str], key: str, default: int = 0) -> int:
    try:
        return int(values[key])
    except (KeyError, ValueError):
        return default


def validate(values: dict[str, str]) -> list[Issue]:
    """Cross-field checks; returns ERROR/WARN issues (empty == clean)."""
    issues: list[Issue] = []

    # K + N pool model: N = LLM_POOL_N (global semaphore), K = INGEST_ADMISSION_MAX_INFLIGHT
    pool_n = _int(values, "LLM_POOL_N", 8)
    llm_cap = _int(values, "TEMPORAL_LLM_ACTIVITY_CONCURRENCY", 0)
    if llm_cap and llm_cap < pool_n:
        issues.append(
            Issue(
                "WARN",
                f"TEMPORAL_LLM_ACTIVITY_CONCURRENCY ({llm_cap}) < LLM_POOL_N "
                f"({pool_n}); Temporal will throttle before the pool",
            )
        )
    merge_cap = _int(values, "TEMPORAL_MERGE_ACTIVITY_CONCURRENCY", 0)
    if merge_cap and merge_cap < pool_n:
        issues.append(
            Issue(
                "WARN",
                f"TEMPORAL_MERGE_ACTIVITY_CONCURRENCY ({merge_cap}) < LLM_POOL_N "
                f"({pool_n}); Temporal will throttle before the pool",
            )
        )

    models = (values.get("LITELLM_MODEL_SMALL", ""), values.get("LITELLM_MODEL_LARGE", ""))
    if any(m.startswith("gpt-") for m in models) and not values.get("OPENAI_API_KEY"):
        issues.append(
            Issue("ERROR", "OPENAI_API_KEY is empty but a model tier points at OpenAI (gpt-*)")
        )

    bp = values.get("WIKIBASE_BOT_PASSWORD", "")
    if bp and len(bp) < 8:
        issues.append(
            Issue(
                "ERROR",
                f"WIKIBASE_BOT_PASSWORD must be >= 8 chars (MediaWiki minimum), got {len(bp)}",
            )
        )
    ap = values.get("WIKIBASE_ADMIN_PASS", "")
    if ap and len(ap) < 10:
        issues.append(
            Issue(
                "ERROR",
                f"WIKIBASE_ADMIN_PASS must be >= 10 chars (MediaWiki minimum), got {len(ap)}",
            )
        )

    return issues


def _sections_in_order(lines: list[Line]) -> list[tuple[str, list[KV]]]:
    """Group KV lines by their section, preserving file order."""
    groups: list[tuple[str, list[KV]]] = []
    index: dict[str, int] = {}
    for ln in lines:
        if isinstance(ln, KV):
            if ln.section not in index:
                index[ln.section] = len(groups)
                groups.append((ln.section, []))
            groups[index[ln.section]][1].append(ln)
    return groups


def run_interactive(
    lines: list[Line],
    values: dict[str, str],
    *,
    input_fn=input,
    getpass_fn=None,
) -> dict[str, str]:
    """Section-by-section prompt loop. Mutates and returns `values`.

    Per section: Enter=keep, 'e'=configure (walk its vars), 'q'=stop.
    Per var: Enter keeps the current default; text overrides; for secrets
    'g' generates.  I/O is injected for testing.
    """
    if getpass_fn is None:
        getpass_fn = getpass.getpass

    for title, kvs in _sections_in_order(lines):
        print(f"\n=== {title or '(no section)'} ===")
        for kv in kvs:
            print(f"  {kv.key}={values.get(kv.key, kv.example_val)}")
        choice = input_fn("[Enter] keep  [e] configure  [q] quit: ").strip().lower()
        if choice == "q":
            break
        if choice != "e":
            continue
        for kv in kvs:
            for c in kv.comment_lines:
                print(c)
            cur = values.get(kv.key, kv.example_val)
            if is_secret(kv.key):
                ans = getpass_fn(f"{kv.key} [default kept; 'g'=generate]: ")
                if ans == "g":
                    values[kv.key] = gen_secret(kv.key)
                elif ans != "":
                    values[kv.key] = ans
            else:
                ans = input_fn(f"{kv.key} [{cur}]: ")
                if ans != "":
                    values[kv.key] = ans
    return values


def build_values(lines: list[Line], existing: dict[str, str]) -> dict[str, str]:
    """Initial values: example defaults overlaid with any existing .env values."""
    values = {ln.key: ln.example_val for ln in lines if isinstance(ln, KV)}
    for k, v in existing.items():
        if k in values:
            values[k] = v
    return values


def write_env(out: Path, content: str) -> None:
    """Write `content` to `out`, backing up an existing file to `<out>.bak`."""
    if out.exists():
        bak = out.with_name(out.name + ".bak")
        bak.write_text(out.read_text())
    out.write_text(content)


def _report_issues(issues: list[Issue]) -> bool:
    """Print issues; return True if any ERROR present."""
    has_error = False
    for i in issues:
        print(f"  [{i.level}] {i.msg}")
        has_error = has_error or i.level == "ERROR"
    return has_error


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Build .env from .env.example.")
    p.add_argument("--example", default=".env.example")
    p.add_argument("--out", default=".env")
    p.add_argument(
        "--non-interactive",
        action="store_true",
        help="copy defaults + generate empty secrets, no prompts",
    )
    p.add_argument("--force", action="store_true", help="write despite ERROR-level validation")
    p.add_argument("--no-merge", action="store_true", help="ignore an existing .env")
    p.add_argument(
        "--reference",
        action="store_true",
        help="(re)generate .env.reference from config.py and exit",
    )
    p.add_argument(
        "--check",
        action="store_true",
        help="verify .env.reference is current (+ report .env.example coverage); exit 1 on stale reference",
    )
    args = p.parse_args(argv)

    ref_path = Path(".env.reference")
    if args.reference:
        ref_path.write_text(build_reference())
        print(f"wrote {ref_path}")
        return 0
    if args.check:
        current = build_reference()
        on_disk = ref_path.read_text() if ref_path.exists() else ""
        stale = current != on_disk
        if stale:
            print(
                "  [DRIFT] .env.reference is stale — run `python -m scripts.make_env --reference`"
            )
        # coverage is INFORMATIONAL (do not fail on it in this batch)
        example_keys = {
            ln.key for ln in parse_example(Path(args.example).read_text()) if isinstance(ln, KV)
        }
        missing = sorted({r.env for r in iter_app_env_vars()} - example_keys)
        if missing:
            print(
                f"  [INFO] {len(missing)} app var(s) not in {args.example} "
                f"(documented in .env.reference): {', '.join(missing[:8])}"
                + (" ..." if len(missing) > 8 else "")
            )
        if stale:
            return 1
        print("env check: OK")
        return 0

    example_path = Path(args.example)
    out_path = Path(args.out)
    lines = parse_example(example_path.read_text())

    existing: dict[str, str] = {}
    if not args.no_merge and out_path.exists():
        existing = parse_env(out_path.read_text())
        print(f"merging values from existing {out_path}")
    values = build_values(lines, existing)

    if args.non_interactive:
        for ln in lines:
            if (
                isinstance(ln, KV)
                and is_secret(ln.key)
                and not values[ln.key]
                and ln.key not in _UPSTREAM_CREDENTIALS
            ):
                values[ln.key] = gen_secret(ln.key)
    else:
        if not sys.stdin.isatty():
            print("error: not a TTY; use --non-interactive", file=sys.stderr)
            return 2
        values = run_interactive(lines, values)

    print("\nvalidating…")
    issues = validate(values)
    if _report_issues(issues) and not args.force:
        print("ERRORs found; fix them or re-run with --force.", file=sys.stderr)
        return 1
    if not issues:
        print("  ok")

    write_env(out_path, render(lines, values))
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
