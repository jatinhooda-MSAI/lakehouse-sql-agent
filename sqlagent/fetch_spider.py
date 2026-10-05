import json
import random
import sqlite3
import urllib.request

import sqlglot

from sqlagent import agent
from sqlagent.guard import GuardError

REVISION = "8698f764f0547eb7136b3be9569093c7f4217be4"
BASE = f"https://huggingface.co/datasets/prem-research/spider/resolve/{REVISION}"
DATABASES = ["car_1", "concert_singer", "dog_kennels", "pets_1", "world_1"]
PER_DATABASE = 20
OUT = agent.ROOT / "data" / "spider"


def download(path: str):
    target = OUT / path
    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(f"{BASE}/{path}", target)
    return target


def main():
    dev = json.loads(download("validation.json").read_text())
    subset, seen = [], set()
    for name in DATABASES:
        db = agent.Database(str(download(f"database/{name}/{name}.sqlite")))
        usable = []
        for i, item in enumerate(dev):
            if item["db_id"] != name or item["question"] in seen:
                continue
            seen.add(item["question"])
            try:
                truncated = db.run(item["query"])[2]
            except (GuardError, sqlglot.errors.SqlglotError, sqlite3.Error) as e:
                print(f"  skip dev{i:04d}: {type(e).__name__}: {e}")
                continue
            if not truncated:
                usable.append({"id": f"dev{i:04d}", "db_id": name, "question": item["question"], "sql": item["query"]})
        subset += sorted(random.Random(0).sample(usable, PER_DATABASE), key=lambda x: x["id"])
        print(f"{name}: {len(usable)} usable dev questions, kept {PER_DATABASE}")
    (OUT / "subset.json").write_text(json.dumps(subset, indent=1))
    print(f"wrote {len(subset)} questions to {OUT / 'subset.json'}")
