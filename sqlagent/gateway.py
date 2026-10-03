import functools
import hashlib
import json
import os
import random
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import anthropic
import openai
import sqlglot
import yaml
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SimpleSpanProcessor
from sqlglot import exp

ROOT = Path(__file__).resolve().parent.parent
GATEWAY_DB = Path(os.environ.get("GATEWAY_DB", ROOT / "data" / "gateway.sqlite"))
MODELS = {"openai": "gpt-5.4-mini", "anthropic": "claude-sonnet-5-5", "offline": "offline-replay"}
PRICES = {"gpt-5.4-mini": (0.75, 4.50), "claude-sonnet-5-5": (2.00, 10.00), "offline-replay": (0.0, 0.0)}
MAX_RETRIES = 4
CORRUPT_SHARE = 0.25
SEED = 7

tracer_provider = TracerProvider(resource=Resource.create({"service.name": "lakehouse-sql-agent"}))
tracer_provider.add_span_processor(
    SimpleSpanProcessor(ConsoleSpanExporter(out=sys.stderr, formatter=lambda s: s.to_json(indent=None) + "\n"))
)
if os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
tracer = tracer_provider.get_tracer("sqlagent.gateway")


class OfflineError(RuntimeError):
    pass


@dataclass
class Completion:
    text: str
    provider: str
    model: str
    input_tokens: int
    output_tokens: int
    cost_usd: float
    cached: bool


@functools.cache
def client(name: str):
    if name == "openai":
        return openai.OpenAI(max_retries=0, timeout=120)
    return anthropic.Anthropic(max_retries=0, timeout=120)


def call_openai(model, system, messages, max_tokens):
    r = client("openai").chat.completions.create(
        model=model,
        messages=[{"role": "system", "content": system}, *messages],
        max_completion_tokens=max_tokens,
        reasoning_effort="low",
    )
    choice = r.choices[0]
    return choice.message.content or "", r.usage.prompt_tokens, r.usage.completion_tokens, choice.finish_reason, r.model


def call_anthropic(model, system, messages, max_tokens):
    r = client("anthropic").messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=messages,
        output_config={"effort": "low"},
    )
    text = "".join(block.text for block in r.content if block.type == "text")
    return text, r.usage.input_tokens, r.usage.output_tokens, r.stop_reason, r.model


@functools.cache
def gold_sql() -> dict[str, str]:
    items = yaml.safe_load((ROOT / "eval" / "chicago.yaml").read_text())
    spider = ROOT / "data" / "spider" / "subset.json"
    if spider.exists():
        items += json.loads(spider.read_text())
    return {item["question"]: item["sql"].strip() for item in items}


def corrupt(sql: str, rng: random.Random) -> str:
    tree = sqlglot.parse_one(sql)
    names = {c.name for c in tree.find_all(exp.Column)} or {t.name for t in tree.find_all(exp.Table)}
    name = rng.choice(sorted(n for n in names if re.fullmatch(r"\w+", n)))
    return re.sub(rf"\b{name}\b", f"{name}_x", sql, count=1)


def call_offline(model, system, messages, max_tokens):
    question = messages[0]["content"].removeprefix("Question: ")
    if question not in gold_sql():
        raise OfflineError(
            "no API key is set, and the offline provider only replays gold SQL for the eval questions "
            "(eval/chicago.yaml and the Spider subset). Set OPENAI_API_KEY or ANTHROPIC_API_KEY to ask anything else."
        )
    sql = gold_sql()[question]
    rng = random.Random(f"{SEED}:{question}")
    if len(messages) == 1 and rng.random() < CORRUPT_SHARE:
        sql = corrupt(sql, rng)
    text = f"```sql\n{sql}\n```"
    prompt_chars = len(system) + sum(len(m["content"]) for m in messages)
    return text, prompt_chars // 4, len(text) // 4, "stop", model


CALLS = {"openai": call_openai, "anthropic": call_anthropic, "offline": call_offline}


@functools.cache
def store() -> sqlite3.Connection:
    GATEWAY_DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(GATEWAY_DB)
    con.execute("CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, response TEXT)")
    con.execute(
        "CREATE TABLE IF NOT EXISTS calls (ts REAL, provider TEXT, model TEXT, input_tokens INT, "
        "output_tokens INT, cost_usd REAL, cached INT, latency_ms REAL)"
    )
    return con


def retryable(e: Exception) -> bool:
    status = getattr(e, "status_code", 0)
    return isinstance(e, (openai.APIConnectionError, anthropic.APIConnectionError)) or status == 429 or status >= 500


def call_with_retries(name, model, system, messages, max_tokens, span):
    for attempt in range(MAX_RETRIES + 1):
        try:
            return CALLS[name](model, system, messages, max_tokens)
        except (openai.APIError, anthropic.APIError) as e:
            if attempt == MAX_RETRIES or not retryable(e):
                raise
            delay = min(30, 2**attempt) * random.uniform(0.5, 1.5)
            span.add_event("retry", {"attempt": attempt + 1, "error": type(e).__name__, "delay_s": delay})
            time.sleep(delay)


def complete_with(name: str, system: str, messages: list[dict], max_tokens: int) -> Completion:
    model = MODELS[name]
    key = hashlib.sha256(json.dumps([name, model, system, messages, max_tokens]).encode()).hexdigest()
    hit = store().execute("SELECT response FROM cache WHERE key = ?", (key,)).fetchone()
    start = time.perf_counter()
    with tracer.start_as_current_span(f"chat {model}") as span:
        span.set_attributes(
            {
                "gen_ai.operation.name": "chat",
                "gen_ai.provider.name": name,
                "gen_ai.request.model": model,
                "gen_ai.request.max_tokens": max_tokens,
                "gateway.cache_hit": hit is not None,
            }
        )
        response = json.loads(hit[0]) if hit else call_with_retries(name, model, system, messages, max_tokens, span)
        text, input_tokens, output_tokens, finish_reason, response_model = response
        cost = (input_tokens * PRICES[model][0] + output_tokens * PRICES[model][1]) / 1e6
        span.set_attributes(
            {
                "gen_ai.response.model": response_model,
                "gen_ai.response.finish_reasons": [finish_reason or ""],
                "gen_ai.usage.input_tokens": input_tokens,
                "gen_ai.usage.output_tokens": output_tokens,
                "gateway.cost_usd": cost,
            }
        )
    latency_ms = (time.perf_counter() - start) * 1000
    with store() as con:
        if not hit:
            con.execute("INSERT OR REPLACE INTO cache VALUES (?, ?)", (key, json.dumps(response)))
        con.execute(
            "INSERT INTO calls VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (time.time(), name, model, input_tokens, output_tokens, 0.0 if hit else cost, int(bool(hit)), latency_ms),
        )
    return Completion(text, name, model, input_tokens, output_tokens, cost, hit is not None)


def chain() -> list[str]:
    names = os.environ.get("LLM_PROVIDERS", "openai,anthropic").split(",")
    return [n for n in names if n == "offline" or os.environ.get(f"{n.upper()}_API_KEY")] or ["offline"]


def complete(system: str, messages: list[dict], max_tokens: int = 4096) -> Completion:
    error = None
    for name in chain():
        try:
            return complete_with(name, system, messages, max_tokens)
        except (openai.APIError, anthropic.APIError) as e:
            error = e
    raise error
