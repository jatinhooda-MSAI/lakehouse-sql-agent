# lakehouse-sql-agent

Ask a question in plain English about the chicago-lakehouse warehouse (Chicago crime and 311 data in DuckDB) and get back one read-only SQL query and its result. Each query passes a sqlglot guard and an EXPLAIN cost check before it runs, and failures go back to the model for up to three attempts. Model calls go through a small gateway with provider fallback, retries, caching, cost logging and OpenTelemetry spans, and an offline replay provider lets it all run without API keys.

## How it works

1. The prompt lists every gold and silver model with column types and descriptions from the dbt `catalog.json` and `manifest.json`, the `semantic_manifest.json` metrics written as formulas, and sample values for text columns with at most 200 distinct values. For a Spider SQLite file it uses the table definitions and foreign keys instead.
2. `sqlagent/guard.py` parses the query with sqlglot and accepts only a single SELECT, WITH or set operation with no DML or DDL inside, every table in `gold` or `silver` (or a CTE), and no table functions, which rules out `read_csv`, `glob` and the like. PRAGMA, ATTACH, COPY, INSTALL, LOAD and SET fail the single-SELECT rule; `tests/test_guard.py` has the allowed and blocked cases.
3. `EXPLAIN (FORMAT JSON)` rejects plans where any operator is estimated above a billion rows, with cross products and nested loop joins charged the product of their inputs since DuckDB often gives no estimate there. The query then runs on a `read_only=True` connection with external access disabled, a timer calls `con.interrupt()` after 10 seconds, and at most 1,000 rows come back.
4. Parse, binder, catalog and runtime errors, guard rejections and cost or timeout rejections go back to the model as a new turn with the failed SQL and the error, for three attempts in total.
5. `sqlagent/gateway.py` exposes `complete()`, which tries OpenAI (`gpt-5.4-mini`) and then Anthropic (`claude-sonnet-5-5`) for whichever keys are set, retrying 429s, 5xx, timeouts and connection errors with exponential backoff and jitter. Responses are cached in SQLite, each call's tokens and cost go to a `calls` table, and each call is an OpenTelemetry span with `gen_ai.*` attributes, printed to stderr and sent over OTLP/HTTP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

With no keys set, an offline provider replays the gold SQL for eval questions, renaming one column on the first attempt for about a quarter of them (seeded) so the retry path runs; any other question gets an error asking for a key.

## Running

```
uv sync
uv run ask "How many homicides occurred in 2025?"
uv run fetch-spider
uv run ask --db data/spider/database/pets_1/pets_1.sqlite "Find the number of dog pets that are raised by female students (with sex F)."
uv run sql-eval chicago
uv run sql-eval spider
uv run pytest
```

Set `OPENAI_API_KEY` and/or `ANTHROPIC_API_KEY` to use real models. `LLM_PROVIDERS=anthropic,openai` changes the order and `LLM_PROVIDERS=offline` forces the replay provider. The warehouse and the dbt target default to `../chicago-lakehouse/data/warehouse.duckdb` and `../chicago-lakehouse/target` and can be pointed elsewhere with `LAKEHOUSE_DB` and `DBT_TARGET`. The cache and cost log live in `data/gateway.sqlite` (`GATEWAY_DB`). Add `2>/dev/null` to hide the spans.

## Eval

`eval/chicago.yaml` has 55 questions with hand-written gold SQL: 15 easy (counts and filters), 20 medium (top-N, date ranges, silver-only columns, medians) and 20 hard (crime and 311 joins, rates, a correlation, window functions). Every question names explicit dates, every gold query was run against the warehouse snapshot (crimes through 2026-09-28, 311 requests through 2026-10-05), and top-N answers were checked for ties at the cutoff.

The public set is 100 Spider dev questions, 20 each from car_1, concert_singer, dog_kennels, pets_1 and world_1, sampled with a fixed seed by `fetch-spider` from a Hugging Face mirror (`prem-research/spider`, pinned to a commit) and run on read-only SQLite. I used Spider over BIRD mini-dev because the mirror serves single database files, about 4 MB instead of an 800 MB zip.

Execution accuracy compares predicted and gold results as sets of tuples, with numbers rounded to four significant figures, whole floats equal to integers and midnight timestamps equal to dates. Each attempt gets one label (correct, guard rejected, parse error, schema error, timeout/cost limit, runtime error, empty result, wrong row count, wrong values); accuracy at 1 attempt uses the first label and at 3 the last, tokens and dollars are summed over all attempts, and everything lands in `results/<set>_<providers>.json`, `.csv` and `.log`.

## Results

There were no API keys, so the only runs are offline, and these numbers are plumbing checks, not model results. The replay provider returns gold SQL, so final accuracy is 1.0 by construction. Every first-attempt failure is one of its renamed columns being rejected by the database as unknown and fixed on the retry. Offline token counts are characters divided by four, and the offline price is zero.

```
chicago: 55 questions, providers offline, 0.6s
accuracy at 1 attempt 0.764, at 3 attempts 1.000
13 questions retried
outcome               first attempt  final
correct                          42     55
schema error                     13      0
per question: 3593 input + 66 output tokens, $0.0000; total $0.0000

spider: 100 questions, providers offline, 0.3s
accuracy at 1 attempt 0.700, at 3 attempts 1.000
30 questions retried
outcome               first attempt  final
correct                          70    100
schema error                     30      0
per question: 1198 input + 43 output tokens, $0.0000; total $0.0000
```

## Notes and limits

- The OpenAI and Anthropic calls have never run against the real APIs. They are written against the installed SDKs (openai 3.26, anthropic 1.11) and were only checked against a local stub server: request shape, a retried 429, and falling back to Anthropic when OpenAI keeps returning 500.
- The cost check is DuckDB only. SQLite's EXPLAIN QUERY PLAN has no row estimates, so Spider queries rely on the timeout and the row cap.
- The eval reports list price per question even when an answer came from the cache. The `calls` table records actual spend, which is zero for cache hits. Point `GATEWAY_DB` at a new file for a clean cost run.
- The full schema goes into every prompt. That is fine for seven tables; a larger warehouse would need retrieval over the manifest.
