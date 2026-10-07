import csv
import json
import sys
import time
from collections import Counter
from datetime import date, datetime
from datetime import time as clock
from decimal import Decimal

import yaml

from sqlagent import agent, gateway

OUTCOMES = [
    "correct",
    "guard rejected",
    "parse error",
    "schema error",
    "timeout/cost limit",
    "runtime error",
    "empty result",
    "wrong row count",
    "wrong values",
]


def norm(v):
    if isinstance(v, Decimal):
        v = float(v)
    if isinstance(v, float):
        return int(v) if v.is_integer() else float(f"{v:.4g}")
    if isinstance(v, datetime) and v.time() == clock(0):
        v = v.date()
    if isinstance(v, date):
        return v.isoformat()
    return v if isinstance(v, (int, str, type(None))) else str(v)


def outcome(attempt: dict, gold: list[tuple]) -> str:
    if "error" in attempt:
        return attempt["kind"]
    if attempt["truncated"]:
        return "wrong row count"
    if {tuple(map(norm, row)) for row in attempt["rows"]} == {tuple(map(norm, row)) for row in gold}:
        return "correct"
    if not attempt["rows"]:
        return "empty result"
    return "wrong row count" if len(attempt["rows"]) != len(gold) else "wrong values"


def load(name: str):
    if name == "chicago":
        warehouse = agent.Database(agent.LAKEHOUSE_DB)
        return yaml.safe_load((agent.ROOT / "eval" / "chicago.yaml").read_text()), lambda item: warehouse
    spider = agent.ROOT / "data" / "spider"
    items = json.loads((spider / "subset.json").read_text())
    names = sorted({item["db_id"] for item in items})
    dbs = {db: agent.Database(str(spider / "database" / db / f"{db}.sqlite")) for db in names}
    return items, lambda item: dbs[item["db_id"]]


def share(records, key, value="correct"):
    return round(sum(r[key] == value for r in records) / len(records), 3)


def main():
    name = sys.argv[1] if len(sys.argv) > 1 else "chicago"
    items, database = load(name)
    providers = "+".join(gateway.chain())
    started = time.perf_counter()
    records = []
    for item in items:
        db = database(item)
        gold = db.run(item["sql"])[1]
        clock_start = time.perf_counter()
        attempts = agent.answer(item["question"], db)
        path = [outcome(a, gold) for a in attempts]
        records.append(
            {
                "id": item["id"],
                "group": item.get("level") or item["db_id"],
                "first": path[0],
                "final": path[-1],
                "attempts": len(attempts),
                "input_tokens": sum(a["input_tokens"] for a in attempts),
                "output_tokens": sum(a["output_tokens"] for a in attempts),
                "cost_usd": round(sum(a["cost_usd"] for a in attempts), 6),
                "seconds": round(time.perf_counter() - clock_start, 3),
                "question": item["question"],
                "path": path,
                "sql": [a["sql"] for a in attempts],
                "errors": [a.get("error") for a in attempts],
            }
        )
        print(f"{item['id']:8} {' -> '.join(path)}")

    n = len(records)
    groups = sorted({r["group"] for r in records})
    summary = {
        "set": name,
        "providers": providers,
        "questions": n,
        "accuracy_at_1": share(records, "first"),
        "accuracy_at_3": share(records, "final"),
        "retried": sum(r["attempts"] > 1 for r in records),
        "outcomes_first_attempt": {k: v for k, v in Counter(r["first"] for r in records).most_common()},
        "outcomes_final": {k: v for k, v in Counter(r["final"] for r in records).most_common()},
        "by_group": {
            g: {
                "questions": sum(r["group"] == g for r in records),
                "accuracy_at_1": share([r for r in records if r["group"] == g], "first"),
                "accuracy_at_3": share([r for r in records if r["group"] == g], "final"),
            }
            for g in groups
        },
        "mean_input_tokens": round(sum(r["input_tokens"] for r in records) / n),
        "mean_output_tokens": round(sum(r["output_tokens"] for r in records) / n),
        "mean_cost_usd": round(sum(r["cost_usd"] for r in records) / n, 6),
        "total_cost_usd": round(sum(r["cost_usd"] for r in records), 4),
        "seconds": round(time.perf_counter() - started, 1),
    }

    out = agent.ROOT / "results" / f"{name}_{providers}"
    out.parent.mkdir(exist_ok=True)
    out.with_suffix(".json").write_text(json.dumps({"summary": summary, "questions": records}, indent=1, default=str))
    columns = ["id", "group", "first", "final", "attempts", "input_tokens", "output_tokens", "cost_usd", "seconds"]
    with out.with_suffix(".csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    print(f"\n{name}: {n} questions, providers {providers}, {summary['seconds']}s")
    print(f"accuracy at 1 attempt {summary['accuracy_at_1']:.3f}, at 3 attempts {summary['accuracy_at_3']:.3f}")
    print(f"{summary['retried']} questions retried")
    print(f"{'outcome':20} {'first attempt':>14} {'final':>6}")
    for kind in OUTCOMES:
        first, final = summary["outcomes_first_attempt"].get(kind, 0), summary["outcomes_final"].get(kind, 0)
        if first or final:
            print(f"{kind:20} {first:>14} {final:>6}")
    print(f"{'group':20} {'n':>4} {'acc@1':>6} {'acc@3':>6}")
    for g, s in summary["by_group"].items():
        print(f"{g:20} {s['questions']:>4} {s['accuracy_at_1']:>6.3f} {s['accuracy_at_3']:>6.3f}")
    print(
        f"per question: {summary['mean_input_tokens']} input + {summary['mean_output_tokens']} output tokens, "
        f"${summary['mean_cost_usd']:.4f}; total ${summary['total_cost_usd']:.4f}"
    )
