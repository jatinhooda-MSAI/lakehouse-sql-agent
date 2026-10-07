# lakehouse-sql-agent

Ask a question in plain English about the chicago-lakehouse warehouse (Chicago crime and 311 data in DuckDB) and get back one read-only SQL query and its result. Every query passes a sqlglot guard and an EXPLAIN cost check before it runs, and failures go back to the model for another try, three attempts at most. All model calls go through a small gateway with provider fallback, retries, a response cache, cost logging and OpenTelemetry spans. An offline replay provider lets everything run without API keys.

## How it works

The prompt is built from the lakehouse's dbt artifacts: every gold and silver model with column types from `catalog.json` and descriptions from `manifest.json`, the metrics in `semantic_manifest.json` written out as formulas, and the values of text columns (all of them when a column has at most 50, a dozen examples up to 200). For a SQLite file it is the table definitions, foreign keys and the same column values instead. The model is asked for one query in a sql block.

The guard (`sqlagent/guard.py`) parses the query with sqlglot. It accepts a single SELECT, WITH or set operation with no DML or DDL inside, where every table is in `gold` or `silver` or is a CTE, and nothing is a table function. That last rule is what stops `read_csv`, `read_parquet`, `glob` and the rest; a quoted file path in FROM fails the schema rule, and PRAGMA, ATTACH, COPY, INSTALL, LOAD and SET fail the single-SELECT rule. `tests/test_guard.py` lists the allowed and blocked cases.

A query that passes the guard is planned with `EXPLAIN (FORMAT JSON)` and rejected if any operator is estimated at more than a billion rows. Cross products and nested loop joins are charged the product of their input sizes, since that is the work they do and DuckDB often reports no estimate for them. It then runs on a connection opened with `read_only=True` and `enable_external_access=false`, a timer calls `con.interrupt()` after 10 seconds, and at most 1,000 rows come back.

Parse errors, binder and catalog errors, runtime errors, guard rejections and cost or timeout rejections go back to the model as a new turn with the failed SQL and the error text.

`sqlagent/gateway.py` has one entry point, `complete()`. It tries OpenAI (`gpt-5.4-mini`, reasoning effort low) and then Anthropic (`claude-sonnet-5-5`, effort low), skipping providers whose key is unset. Each provider gets up to four retries with exponential backoff and jitter on 429s, 5xx, timeouts and connection errors; the SDKs' own retries are off. Responses are cached in SQLite by provider, model and prompt. Every completed call is logged to a `calls` table with tokens and cost (tokens times the price table at the top of the file). Every call is also an OpenTelemetry span with `gen_ai.*` attributes (provider, request and response model, finish reason, token usage) under one parent span per question. Spans print to stderr as JSON lines and also go to an OTLP/HTTP collector when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

With no keys the chain falls back to the offline provider. It only knows the eval questions and returns their gold SQL, except that for about a quarter of them (seeded) it renames one column on the first attempt so the error and retry path gets used. Any other question gets an error asking for a key.

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

Set `OPENAI_API_KEY` and/or `ANTHROPIC_API_KEY` to use real models. `LLM_PROVIDERS=anthropic,openai` changes the order and `LLM_PROVIDERS=offline` forces the replay provider. The warehouse and the dbt target default to `../chicago-lakehouse/data/warehouse.duckdb` and `../chicago-lakehouse/target` and can be pointed elsewhere with `LAKEHOUSE_DB` and `DBT_TARGET`. The cache and cost log live in `data/gateway.sqlite` (`GATEWAY_DB`). Add `2>/dev/null` to hide the spans. An offline run that hits a renamed column looks like this:

```
-- attempt 1: offline/offline-replay, 2863 in / 41 out tokens, $0.0000
SELECT SUM(incidents) FROM gold.fct_crimes_daily
WHERE primary_type = 'HOMICIDE'
  AND crime_date_x >= DATE '2025-01-01' AND crime_date < DATE '2026-01-01'
-- schema error: Binder Error: Referenced column "crime_date_x" not found in FROM clause!
...
-- attempt 2: offline/offline-replay, 2986 in / 41 out tokens, $0.0000
SELECT SUM(incidents) FROM gold.fct_crimes_daily
WHERE primary_type = 'HOMICIDE'
  AND crime_date >= DATE '2025-01-01' AND crime_date < DATE '2026-01-01'

sum(incidents)
440
```

## Eval

`eval/chicago.yaml` has 55 questions with hand-written gold SQL: 15 easy (counts and filters), 20 medium (top-N, date ranges, silver-only columns, medians) and 20 hard (crime and 311 joins, rates, a correlation, window functions). Every question names explicit dates. Every gold query was run against the warehouse snapshot (crimes through 2026-09-28, 311 requests through 2026-10-05), and top-N answers were checked for ties at the cutoff.

The public set is 100 Spider dev questions, 20 each from car_1, concert_singer, dog_kennels, pets_1 and world_1, sampled with a fixed seed by `fetch-spider` from a Hugging Face mirror of Spider (`prem-research/spider`, pinned to a commit) and run on SQLite opened with `mode=ro`. I used Spider over BIRD mini-dev because the mirror serves single database files, so the download is about 4 MB instead of an 800 MB zip.

Scoring is execution accuracy: predicted and gold results are compared as sets of tuples, with numbers rounded to four significant figures, whole floats equal to integers and midnight timestamps equal to dates. Column order matters, column names don't. Each attempt is labelled correct, guard rejected, parse error, schema error, timeout/cost limit, runtime error, empty result, wrong row count or wrong values. Accuracy at 1 attempt uses the first label and accuracy at 3 the last. Tokens and dollars are summed over all attempts. `sql-eval` writes `results/<set>_<providers>.json` (summary plus every attempt's SQL and error) and a `.csv`; the `.log` files are its stdout.

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
- The eval reports list price per question even when an answer came from the cache. The `calls` table records actual spend, which is zero for cache hits. Point `GATEWAY_DB` at a new file for a clean cost run.
- The cost check is DuckDB only. SQLite's EXPLAIN QUERY PLAN has no row estimates, so Spider queries rely on the timeout and the row cap.
- The guard also rejects harmless table functions like `range()`, and unqualified table names in the warehouse.
- Set comparison can't tell an extra column from a wrong answer, and ties at an ORDER BY ... LIMIT cutoff can make a Spider gold answer ambiguous. The prompt asks for only the requested columns, which helps but won't remove these misses.
- The full schema goes into every prompt. That is fine for seven tables; a larger warehouse would need retrieval over the manifest.
