import argparse
import json
import os
import re
import sqlite3
import threading
from pathlib import Path

import duckdb
import sqlglot

from sqlagent import gateway
from sqlagent.guard import GuardError, check

ROOT = Path(__file__).resolve().parent.parent
LAKEHOUSE_DB = os.environ.get("LAKEHOUSE_DB", str(ROOT.parent / "chicago-lakehouse" / "data" / "warehouse.duckdb"))
DBT_TARGET = Path(os.environ.get("DBT_TARGET", ROOT.parent / "chicago-lakehouse" / "target"))
MAX_ATTEMPTS = 3
MAX_ROWS = 1000
TIMEOUT_S = 10
MAX_ESTIMATED_ROWS = 100_000_000

SYSTEM = """You write one {dialect} SQL query that answers a question about this database.
{context}

Rules:
- Reply with the query in a single ```sql block and nothing else.
- One read-only SELECT statement (WITH is fine). No table functions, PRAGMA, ATTACH, COPY, INSTALL or SET.
- {tables}
- Select only the columns the question asks for, in the order it asks for them."""


class CostLimit(Exception):
    pass


def text_values(con, table: str, column: str) -> str:
    distinct = con.execute(f'SELECT COUNT(DISTINCT "{column}") FROM {table}').fetchone()[0]
    if distinct > 200:
        return ""
    values = con.execute(
        f'SELECT "{column}" FROM {table} WHERE "{column}" IS NOT NULL '
        f"GROUP BY 1 ORDER BY COUNT(*) DESC, 1 LIMIT {50 if distinct <= 50 else 12}"
    ).fetchall()
    shown = json.dumps([v for (v,) in values], ensure_ascii=False, default=str)
    return f" Values: {shown}" if distinct <= 50 else f" {distinct} distinct values, e.g. {shown}"


def warehouse_context(con) -> str:
    manifest = json.loads((DBT_TARGET / "manifest.json").read_text())
    catalog = json.loads((DBT_TARGET / "catalog.json").read_text())["nodes"]
    models = sorted(
        (n for n in manifest["nodes"].values() if n["resource_type"] == "model" and n["schema"] in ("gold", "silver")),
        key=lambda n: (n["schema"] != "gold", n["name"]),
    )
    lines = []
    for node in models:
        table = f"{node['schema']}.{node['name']}"
        lines.append(f"\nTable {table}: {' '.join(node['description'].split())}")
        for col in sorted(catalog[node["unique_id"]]["columns"].values(), key=lambda c: c["index"]):
            description = node["columns"].get(col["name"], {}).get("description", "")
            values = text_values(con, table, col["name"]) if col["type"] == "VARCHAR" else ""
            lines.append(f"  {col['name']} {col['type']}: {description}{values}")
    semantic_path = DBT_TARGET / "semantic_manifest.json"
    if semantic_path.exists():
        semantic = json.loads(semantic_path.read_text())
        measures = {}
        for model in semantic["semantic_models"]:
            relation = f"{model['node_relation']['schema_name']}.{model['node_relation']['alias']}"
            for m in model["measures"]:
                time_dimension = m["agg_time_dimension"] or (model["defaults"] or {}).get("agg_time_dimension")
                measures[m["name"]] = f"{m['agg']}({m['expr'] or m['name']}) from {relation} by {time_dimension}"
        lines.append("\nMetrics defined in the dbt semantic layer:")
        for metric in semantic["metrics"]:
            params = metric["type_params"]
            if metric["type"] == "ratio":
                formula = f"{params['numerator']['name']} / {params['denominator']['name']}"
            else:
                formula = measures[params["measure"]["name"]]
            filters = [
                re.sub(r"\{\{\s*Dimension\('\w+__(\w+)'\)\s*\}\}", r"\1", f["where_sql_template"])
                for f in (metric["filter"] or {}).get("where_filters", [])
            ]
            where = f" where {' and '.join(filters)}" if filters else ""
            lines.append(f"  {metric['name']}: {metric['description']} = {formula}{where}")
    return "\n".join(lines)


def sqlite_context(con) -> str:
    lines = []
    tables = con.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY 1")
    for (table,) in tables.fetchall():
        lines.append(f"\nTable {table}:")
        for _, name, type_, _, _, pk in con.execute(f'PRAGMA table_info("{table}")').fetchall():
            values = text_values(con, f'"{table}"', name) if re.search("char|text", type_, re.I) else ""
            lines.append(f"  {name} {type_}{' primary key' if pk else ''}{values}")
        for fk in con.execute(f'PRAGMA foreign_key_list("{table}")').fetchall():
            lines.append(f"  foreign key {fk[3]} references {fk[2]}({fk[4] or ''})")
    return "\n".join(lines)


def estimated_rows(con, sql: str) -> int:
    nodes = json.loads(con.execute("EXPLAIN (FORMAT JSON) " + sql).fetchall()[0][1])
    most = 0
    while nodes:
        node = nodes.pop()
        most = max(most, int(node.get("extra_info", {}).get("Estimated Cardinality", 0)))
        nodes += node["children"]
    return most


class Database:
    def __init__(self, path: str):
        if path.endswith(".sqlite"):
            self.con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            self.dialect, self.schemas, self.catalog = "sqlite", {"", "main"}, ""
            tables = "Use only the tables above."
            context = sqlite_context(self.con)
        else:
            self.con = duckdb.connect(path, read_only=True, config={"enable_external_access": False})
            self.dialect, self.schemas, self.catalog = "duckdb", {"gold", "silver"}, Path(path).stem
            tables = (
                "Use only the tables above, always schema-qualified (gold.name or silver.name). "
                "Prefer gold tables; use silver only when gold lacks what the question needs."
            )
            context = warehouse_context(self.con)
        dialect = "SQLite" if self.dialect == "sqlite" else "DuckDB"
        self.system = SYSTEM.format(dialect=dialect, context=context, tables=tables)

    def run(self, sql: str) -> tuple[list[str], list[tuple], bool]:
        check(sql, self.dialect, self.schemas, self.catalog)
        if self.dialect == "duckdb" and (estimate := estimated_rows(self.con, sql)) > MAX_ESTIMATED_ROWS:
            raise CostLimit(f"the plan estimates {estimate:,} rows at one step, over the limit of {MAX_ESTIMATED_ROWS:,}")
        timer = threading.Timer(TIMEOUT_S, self.con.interrupt)
        timer.start()
        try:
            cursor = self.con.execute(sql)
            columns = [d[0] for d in cursor.description]
            rows = cursor.fetchmany(MAX_ROWS + 1)
        finally:
            timer.cancel()
        return columns, rows[:MAX_ROWS], len(rows) > MAX_ROWS


def extract_sql(text: str) -> str:
    match = re.search(r"```(?:sql)?\s*(.*?)```", text, re.S | re.I)
    return (match.group(1) if match else text).strip()


def classify(e: Exception) -> str:
    message = str(e).lower()
    if isinstance(e, (sqlglot.errors.SqlglotError, duckdb.ParserException)) or "syntax error" in message:
        return "parse error"
    if isinstance(e, GuardError):
        return "guard rejected"
    if isinstance(e, (CostLimit, duckdb.InterruptException)) or "interrupted" in message:
        return "timeout/cost limit"
    if isinstance(e, (duckdb.BinderException, duckdb.CatalogException)) or "no such" in message:
        return "schema error"
    return "runtime error"


def answer(question: str, db: Database) -> list[dict]:
    messages = [{"role": "user", "content": f"Question: {question}"}]
    attempts = []
    with gateway.tracer.start_as_current_span("sql_agent.answer") as span:
        for _ in range(MAX_ATTEMPTS):
            reply = gateway.complete(db.system, messages)
            attempt = {
                "sql": extract_sql(reply.text),
                "provider": reply.provider,
                "model": reply.model,
                "input_tokens": reply.input_tokens,
                "output_tokens": reply.output_tokens,
                "cost_usd": reply.cost_usd,
                "cached": reply.cached,
            }
            attempts.append(attempt)
            try:
                attempt["columns"], attempt["rows"], attempt["truncated"] = db.run(attempt["sql"])
                break
            except (GuardError, CostLimit, sqlglot.errors.SqlglotError, duckdb.Error, sqlite3.Error) as e:
                attempt["kind"], attempt["error"] = classify(e), str(e).strip()[:1000]
                feedback = f"That query failed ({attempt['kind']}): {attempt['error']}\nReply with a corrected query."
                messages += [{"role": "assistant", "content": reply.text}, {"role": "user", "content": feedback}]
        span.set_attributes({"sql_agent.attempts": len(attempts), "sql_agent.outcome": attempts[-1].get("kind", "ran")})
    return attempts


def show(columns: list[str], rows: list[tuple]) -> None:
    table = [columns] + [[str(v) for v in row] for row in rows]
    widths = [max(len(row[i]) for row in table) for i in range(len(columns))]
    for row in table:
        print("  ".join(value.ljust(width) for value, width in zip(row, widths)).rstrip())


def main():
    parser = argparse.ArgumentParser(prog="ask", description="Answer a question with one read-only SQL query.")
    parser.add_argument("question")
    parser.add_argument("--db", default=LAKEHOUSE_DB, help="the DuckDB warehouse (default) or a .sqlite file")
    args = parser.parse_args()
    db = Database(args.db)
    try:
        attempts = answer(args.question, db)
    except gateway.OfflineError as e:
        raise SystemExit(f"offline: {e}")
    for i, a in enumerate(attempts, 1):
        cached = ", cached" if a["cached"] else ""
        print(f"-- attempt {i}: {a['provider']}/{a['model']}, {a['input_tokens']} in / {a['output_tokens']} out tokens, "
              f"${a['cost_usd']:.4f}{cached}")
        print(a["sql"])
        if "error" in a:
            print(f"-- {a['kind']}: {a['error']}")
        print()
    final = attempts[-1]
    if "error" in final:
        raise SystemExit(f"no answer after {len(attempts)} attempts")
    show(final["columns"], final["rows"])
    if final["truncated"]:
        print(f"(stopped at {MAX_ROWS} rows)")
