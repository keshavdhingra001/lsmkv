#!/usr/bin/env python3
"""Planted-bug (mutation) runner: proves the tests catch real mistakes.

Each mutant in scripts/mutants.toml replaces one exact snippet of source
with a buggy version, builds, and runs the tests named for it. A mutant is
"caught" if those tests fail, "survived" if they pass. The source is always
restored, even on Ctrl-C.

    python3 scripts/mutate.py                 # every mutant
    python3 scripts/mutate.py durability      # names containing "durability"
    python3 scripts/mutate.py --list
    python3 scripts/mutate.py --control       # each mutant's tests, unmutated

Each run's test output goes to target/mutants/<n>.log, so a "caught" can be
checked against the assertion that actually failed. `--control` runs every
mutant's tests on the unmodified source: they must all pass, or a "caught"
could be the tests failing for some other reason.

Exits non-zero if any mutant not marked `equivalent = true` survives, or
(with --control) if any test command fails unmutated.
"""

import os
import re
import shlex
import subprocess
import sys
import time
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TIMEOUT = 600  # seconds per test run; a hang counts as caught, and is flagged


LOGS = ROOT / "target/mutants"


def run(cmd, env=None, timeout=None, log=None):
    out = open(log, "w") if log else subprocess.DEVNULL
    try:
        return subprocess.run(
            cmd, cwd=ROOT, env=env, timeout=timeout, stdout=out, stderr=subprocess.STDOUT,
        ).returncode
    finally:
        if log:
            out.close()


def log_path(i, m):
    slug = re.sub(r"[^a-z0-9]+", "-", m["name"].lower()).strip("-")[:60]
    return LOGS / f"{i:02}-{slug}.log"


def control(catalog):
    """Every distinct test command, on the unmodified source: all must pass."""
    failed = 0
    seen = set()
    for m in catalog:
        env = m.get("env", {})
        key = (m["tests"], tuple(sorted(env.items())))
        if key in seen:
            continue
        seen.add(key)
        cmd = ["cargo", "test", *shlex.split(m["tests"])]
        code = run(cmd, dict(os.environ, **env), TIMEOUT, LOGS / f"control-{len(seen):02}.log")
        print(f"{'pass' if code == 0 else 'FAIL':6} {' '.join(cmd)} {env or ''}", flush=True)
        failed += code != 0
    return 1 if failed else 0


def main():
    catalog = tomllib.loads((ROOT / "scripts/mutants.toml").read_text())["mutant"]
    args = sys.argv[1:]
    if args == ["--list"]:
        for m in catalog:
            print(f"{m['name']:45} {m['file']}")
        return 0
    LOGS.mkdir(parents=True, exist_ok=True)
    if args and args[0] == "--control":
        return control([m for m in catalog if not args[1:] or any(a in m["name"] for a in args[1:])])
    if args:
        catalog = [m for m in catalog if any(a in m["name"] for a in args)]

    # Every snippet must be present exactly once before anything is touched,
    # so a stale catalog fails loudly instead of "surviving".
    for m in catalog:
        n = (ROOT / m["file"]).read_text().count(m["find"])
        if n != 1:
            print(f"catalog error: {m['name']}: snippet found {n} times in {m['file']}")
            return 2

    results = []
    for i, m in enumerate(catalog, 1):
        path = ROOT / m["file"]
        original = path.read_text()
        env = dict(os.environ, **m.get("env", {}))
        cmd = ["cargo", "test", *shlex.split(m["tests"])]
        try:
            path.write_text(original.replace(m["find"], m["replace"], 1))
            # Build first, untimed: a slow build (or a lock held by another
            # cargo) must never be scored as "caught".
            if run([*cmd, "--no-run"], env) != 0:
                verdict = "doesn't compile"
            else:
                start = time.time()
                try:
                    code = run(cmd, env, TIMEOUT, log_path(i, m))
                    verdict = "caught" if code != 0 else "SURVIVED"
                except subprocess.TimeoutExpired:
                    verdict = "caught (hang)"
                verdict += f"  {time.time() - start:.0f}s"
        finally:
            path.write_text(original)
            os.utime(path)  # make cargo rebuild the restored file
        if m.get("equivalent") and verdict.startswith("SURVIVED"):
            verdict = "equivalent" + verdict[len("SURVIVED"):]
        print(f"{verdict:22} {m['name']}", flush=True)
        results.append((m, verdict))

    survived = [m["name"] for m, v in results if v.startswith("SURVIVED")]
    broken = [m["name"] for m, v in results if v.startswith("doesn't")]
    caught = sum(v.startswith("caught") for _, v in results)
    print(f"\n{caught} caught, {len(survived)} survived, "
          f"{len(results) - caught - len(survived) - len(broken)} equivalent, "
          f"{len(broken)} don't compile, of {len(results)}")
    return 1 if survived or broken else 0


if __name__ == "__main__":
    sys.exit(main())
