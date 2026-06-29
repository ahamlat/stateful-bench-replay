#!/usr/bin/env python3
"""Web console for the stateful replay benchmark.

A single-file, stdlib-only HTTP server that:

  * lists the ~550 tests with the facets parsed from their names
    (file, opcode, gas, value_sent, account_mode, ...),
  * shows the latest known per-test performance (Mgas/s, latency) gathered
    from previous runs (compare reports + per-test Besu logs),
  * launches `run.py` to execute an arbitrary multi-selection of tests
    (a plain sweep) or to compare two Besu images on that selection,
  * streams the live log of running jobs and exposes the results / reports
    that `run.py` writes under `runs/`.

It reuses `run.py` itself (imported as a module) for config loading, test
discovery and Besu-log parsing, so the two stay in sync.

Run it ON THE VM where `run.py` lives (the tests, snapshot and docker are
there), then port-forward to your laptop, e.g.:

    ssh -N -L 8765:127.0.0.1:8765 <vm>
    # on the VM:
    ./runWebUI.sh            # or: ./venv/bin/python webui/server.py --port 8765

Then open http://127.0.0.1:8765 locally.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# --------------------------------------------------------------------------
# Paths / module import
# --------------------------------------------------------------------------

WEBUI_DIR = Path(__file__).resolve().parent
REPO = WEBUI_DIR.parent
STATIC_DIR = WEBUI_DIR / "static"
JOBS_DIR = WEBUI_DIR / "jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)


def _import_runner():
    """Import the sibling run.py as a module so we can reuse its helpers."""
    path = REPO / "run.py"
    spec = importlib.util.spec_from_file_location("benchrunner", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot import {path}")
    mod = importlib.util.module_from_spec(spec)
    # Register before exec so dataclasses (which look the module up via
    # sys.modules[cls.__module__]) resolve correctly.
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


try:
    runner = _import_runner()
except Exception as exc:  # pragma: no cover - only on broken env
    runner = None
    _RUNNER_ERR = repr(exc)
else:
    _RUNNER_ERR = None


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

class AppState:
    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.cfg = None
        self.config_error: str | None = None
        self.reload_config()

    def reload_config(self) -> None:
        if runner is None:
            self.config_error = f"could not import run.py: {_RUNNER_ERR}"
            return
        try:
            self.cfg = runner.load_config(self.config_path)
            self.config_error = None
        except Exception as exc:
            self.cfg = None
            self.config_error = f"{type(exc).__name__}: {exc}"

    def config_summary(self) -> dict:
        cfg = self.cfg
        if cfg is None:
            return {"ok": False, "error": self.config_error,
                    "config_path": str(self.config_path)}
        return {
            "ok": True,
            "config_path": str(self.config_path),
            "image": cfg.besu.image,
            "reset_backend": cfg.run.reset_backend,
            "input_dir": str(cfg.input.dir),
            "snapshot_dir": str(cfg.besu.data_snapshot_dir),
            "skip_gas_bump": cfg.run.skip_gas_bump,
            "persist_prelude": cfg.run.persist_prelude,
            "log_dir": str(cfg.run.log_dir),
        }

    @property
    def runs_dir(self) -> Path:
        if self.cfg is not None:
            return self.cfg.run.log_dir
        return REPO / "runs"


# --------------------------------------------------------------------------
# Test-name -> facets parsing
# --------------------------------------------------------------------------

# Boilerplate params that carry no useful filtering signal.
_PARAM_NOISE = {"benchmark_test", "fork_Cancun"}

_GAS_RE = re.compile(r"^(\d+)M$")
_ENUM_RE = re.compile(r"^[A-Za-z]+\.(.+)$")  # AccountMode.NON_EXISTING_ACCOUNT
_FILE_RE = re.compile(r"^(?:test_)?(.+?)\.py__")
_FUNC_RE = re.compile(r"\.py__test_([A-Za-z0-9_]+?)\[")


def _looks_like_value(part: str) -> bool:
    if not part:
        return False
    c = part[0]
    return c.isupper() or c.isdigit() or part in ("True", "False") or "." in part


def _split_param(seg: str) -> tuple[str, str]:
    """Split one `key_value` parameter into (key, value).

    Walks the underscore-separated words left-to-right and treats the first
    "value-looking" word (uppercase/digit/enum/bool) as the start of the
    value; everything before it is the key. Enum prefixes like
    ``AccountMode.`` are stripped from the value.
    """
    parts = seg.split("_")
    key_words: list[str] = []
    val_words: list[str] = []
    for i, part in enumerate(parts):
        if _looks_like_value(part):
            val_words = parts[i:]
            break
        key_words.append(part)
    else:
        # No value-looking token: whole thing is a flag-ish key.
        return ("_".join(parts), "")
    key = "_".join(key_words)
    value = "_".join(val_words)
    m = _ENUM_RE.match(value)
    if m:
        value = m.group(1)
    if not key:
        # Positional param like `10GB` -> generic 'variant' dimension.
        key = "variant"
    return (key, value)


def parse_test(name: str) -> dict:
    """Parse a test basename into a flat dict of facet dimensions."""
    dims: dict[str, str] = {}

    m = _FILE_RE.search(name)
    if m:
        dims["file"] = m.group(1)
    m = _FUNC_RE.search(name)
    if m:
        dims["test"] = m.group(1)

    inside = ""
    if "[" in name and "]" in name:
        inside = name[name.index("[") + 1: name.rindex("]")]
    for seg in inside.split("-"):
        seg = seg.strip()
        if not seg or seg in _PARAM_NOISE:
            continue
        key, value = _split_param(seg)
        if not value:
            continue
        # benchmark_<N>M is really the gas target -> call it 'gas'.
        if key == "benchmark" and _GAS_RE.match(value):
            key = "gas"
        # Last write wins; names don't repeat keys in practice.
        dims[key] = value

    return {"name": name, "dims": dims}


# --------------------------------------------------------------------------
# Test discovery (with graceful fallback when the input dir is absent)
# --------------------------------------------------------------------------

def discover_test_names(state: AppState) -> tuple[list[str], str, str | None]:
    """Return (names, source, warning).

    Tries run.py's discover_tests first (authoritative, needs the input dir).
    Falls back to names harvested from previous runs / stored reports so the
    UI still works on a machine without the test corpus mounted.
    """
    if state.cfg is not None and runner is not None:
        try:
            names = runner.discover_tests(state.cfg, None, None)
            if names:
                return names, "input_dir", None
        except Exception as exc:
            warn = f"discover_tests failed ({type(exc).__name__}: {exc}); using fallback list"
        else:
            warn = "discover_tests returned no tests; using fallback list"
    else:
        warn = state.config_error or "no config; using fallback list"

    names = sorted(_fallback_names(state))
    return names, "fallback", (warn if names else (warn + " (and fallback empty)"))


def _fallback_names(state: AppState) -> set[str]:
    names: set[str] = set()
    # selected_tests.txt from any run.
    runs_dir = state.runs_dir
    if runs_dir.is_dir():
        for sel in runs_dir.glob("*/selected_tests.txt"):
            try:
                for ln in sel.read_text().splitlines():
                    ln = ln.strip()
                    if ln:
                        names.add(ln)
            except OSError:
                pass
        for cj in runs_dir.glob("*/comparison.json"):
            try:
                data = json.loads(cj.read_text())
                for row in data.get("rows", []):
                    if row.get("test"):
                        names.add(row["test"])
            except (OSError, json.JSONDecodeError):
                pass
    # Combined-results store at the repo root.
    store = REPO / "combined-results.html.data.json"
    if store.is_file():
        try:
            data = json.loads(store.read_text())
            names.update(data.get("tests", {}).keys())
        except (OSError, json.JSONDecodeError):
            pass
    return names


# --------------------------------------------------------------------------
# Metrics: latest per-test Mgas/s + latency from previous runs
# --------------------------------------------------------------------------

def gather_metrics(state: AppState) -> dict:
    """Map test name -> latest {mgas, lat, gas, run, ts}. Newest run wins."""
    out: dict[str, dict] = {}
    runs_dir = state.runs_dir
    if not runs_dir.is_dir():
        return {"tests": {}, "range": None, "runs_scanned": 0}

    # Newest first so the first value we record for a test is the freshest.
    run_dirs = sorted([p for p in runs_dir.iterdir() if p.is_dir()],
                      key=lambda p: p.name, reverse=True)
    scanned = 0
    for rd in run_dirs:
        scanned += 1
        cmp_json = rd / "comparison.json"
        if cmp_json.is_file():
            _absorb_comparison(cmp_json, rd.name, out)
        else:
            _absorb_sweep(rd, out)

    mgas_vals = [v["mgas"] for v in out.values() if isinstance(v.get("mgas"), (int, float))]
    rng = {"min": min(mgas_vals), "max": max(mgas_vals)} if mgas_vals else None
    return {"tests": out, "range": rng, "runs_scanned": scanned}


def _absorb_comparison(cmp_json: Path, run_id: str, out: dict) -> None:
    try:
        data = json.loads(cmp_json.read_text())
    except (OSError, json.JSONDecodeError):
        return
    label_y = (data.get("version_y") or {}).get("label")
    label_x = (data.get("version_x") or {}).get("label")
    for row in data.get("rows", []):
        name = row.get("test")
        if not name or name in out:
            continue
        mgas = row.get("y_mgas")
        lat = row.get("y_lat_ms")
        if not isinstance(mgas, (int, float)):
            mgas = row.get("x_mgas")
            lat = row.get("x_lat_ms")
        out[name] = {
            "mgas": mgas if isinstance(mgas, (int, float)) else None,
            "lat": lat if isinstance(lat, (int, float)) else None,
            "gas": row.get("gas_used"),
            "run": run_id,
            "kind": "compare",
            "labels": [label_x, label_y],
        }


def _absorb_sweep(rd: Path, out: dict) -> None:
    if runner is None:
        return
    sel = rd / "selected_tests.txt"
    names: list[str] = []
    if sel.is_file():
        try:
            names = [ln.strip() for ln in sel.read_text().splitlines() if ln.strip()]
        except OSError:
            names = []
    for log in rd.glob("besu-*.log"):
        m = re.match(r"besu-(\d+)-", log.name)
        if not m:
            continue
        idx = int(m.group(1))
        name = names[idx - 1] if 0 < idx <= len(names) else None
        if not name or name in out:
            continue
        try:
            metrics = runner._parse_last_imported(log)
        except Exception:
            metrics = None
        if not metrics:
            continue
        out[name] = {
            "mgas": metrics.get("mgas_s"),
            "lat": (metrics.get("exec_s") * 1000.0
                    if isinstance(metrics.get("exec_s"), (int, float)) else None),
            "gas": metrics.get("gas_used"),
            "run": rd.name,
            "kind": "sweep",
            "labels": [],
        }


# --------------------------------------------------------------------------
# Runs listing / details
# --------------------------------------------------------------------------

def list_runs(state: AppState) -> list[dict]:
    runs_dir = state.runs_dir
    if not runs_dir.is_dir():
        return []
    runs = []
    for rd in sorted([p for p in runs_dir.iterdir() if p.is_dir()],
                     key=lambda p: p.name, reverse=True):
        runs.append(_run_brief(rd))
    return runs


def _read_json(p: Path) -> dict | None:
    try:
        return json.loads(p.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def _run_brief(rd: Path) -> dict:
    summary = _read_json(rd / "summary.json") or {}
    is_compare = rd.name.endswith("-compare") or summary.get("mode") == "compare"
    sel = rd / "selected_tests.txt"
    n_selected = 0
    if sel.is_file():
        try:
            n_selected = len([x for x in sel.read_text().splitlines() if x.strip()])
        except OSError:
            n_selected = 0
    info = {
        "id": rd.name,
        "kind": "compare" if is_compare else "sweep",
        "selected": n_selected,
        "has_comparison": (rd / "comparison.json").is_file(),
        "finished_at": summary.get("finished_at"),
        "totals": summary.get("totals"),
        "fail_fast_tripped": summary.get("fail_fast_tripped"),
    }
    if is_compare:
        cs = summary.get("comparison_summary") or {}
        info["version_x"] = summary.get("version_x")
        info["version_y"] = summary.get("version_y")
        info["compare_summary"] = cs
    return info


def run_details(state: AppState, run_id: str) -> dict | None:
    rd = state.runs_dir / run_id
    if not rd.is_dir() or ".." in run_id or "/" in run_id:
        return None
    detail = _run_brief(rd)
    detail["summary"] = _read_json(rd / "summary.json")
    cmp_json = _read_json(rd / "comparison.json")
    if cmp_json:
        detail["comparison"] = cmp_json
    # selected tests
    sel = rd / "selected_tests.txt"
    if sel.is_file():
        try:
            detail["selected_tests"] = [x for x in sel.read_text().splitlines() if x.strip()]
        except OSError:
            detail["selected_tests"] = []
    # events tail
    ev = rd / "events.log"
    if ev.is_file():
        try:
            detail["events"] = ev.read_text().splitlines()[-400:]
        except OSError:
            detail["events"] = []
    # failures
    fj = rd / "failures.jsonl"
    if fj.is_file():
        rows = []
        try:
            for ln in fj.read_text().splitlines():
                ln = ln.strip()
                if ln:
                    try:
                        rows.append(json.loads(ln))
                    except json.JSONDecodeError:
                        pass
        except OSError:
            pass
        detail["failures"] = rows[:500]
    # downloadable files
    files = []
    for p in sorted(rd.iterdir()):
        if p.is_file():
            files.append({"name": p.name, "size": p.stat().st_size})
    detail["files"] = files
    return detail


# --------------------------------------------------------------------------
# Job manager (subprocess running run.py)
# --------------------------------------------------------------------------

class Job:
    def __init__(self, job_id: str, kind: str, cmd: list[str], tests: list[str],
                 log_path: Path):
        self.id = job_id
        self.kind = kind            # 'run' | 'compare'
        self.cmd = cmd
        self.tests = tests
        self.log_path = log_path
        self.status = "running"      # running | done | failed | cancelled
        self.returncode: int | None = None
        self.started = time.time()
        self.finished: float | None = None
        self.run_dir: str | None = None
        self.proc: subprocess.Popen | None = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "kind": self.kind,
            "status": self.status,
            "returncode": self.returncode,
            "started": self.started,
            "started_iso": datetime.fromtimestamp(self.started).isoformat(timespec="seconds"),
            "finished": self.finished,
            "elapsed_s": (self.finished or time.time()) - self.started,
            "n_tests": len(self.tests),
            "run_dir": self.run_dir,
            "cmd": " ".join(shlex.quote(c) for c in self.cmd),
        }


class JobManager:
    def __init__(self, state: AppState):
        self.state = state
        self.jobs: dict[str, Job] = {}
        self.lock = threading.Lock()

    def start(self, kind: str, tests: list[str], extra_args: list[str]) -> Job:
        job_id = datetime.now().strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:6]
        tests_file = JOBS_DIR / f"{job_id}.tests.txt"
        tests_file.write_text("\n".join(tests) + "\n")
        log_path = JOBS_DIR / f"{job_id}.log"

        python = sys.executable or "python3"
        cmd = [python, str(REPO / "run.py"),
               "--config", str(self.state.config_path),
               "--tests-from", str(tests_file)]
        cmd += extra_args

        job = Job(job_id, kind, cmd, tests, log_path)
        with self.lock:
            self.jobs[job_id] = job

        threading.Thread(target=self._run, args=(job,), daemon=True).start()
        return job

    def _run(self, job: Job) -> None:
        with job.log_path.open("w", buffering=1) as logf:
            logf.write(f"# {datetime.now().isoformat(timespec='seconds')} "
                       f"launching: {' '.join(shlex.quote(c) for c in job.cmd)}\n\n")
            logf.flush()
            try:
                proc = subprocess.Popen(
                    job.cmd, cwd=str(REPO),
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, bufsize=1, start_new_session=True,
                )
            except Exception as exc:
                logf.write(f"\n!! failed to launch: {exc!r}\n")
                job.status = "failed"
                job.returncode = -1
                job.finished = time.time()
                return
            job.proc = proc
            assert proc.stdout is not None
            for line in proc.stdout:
                logf.write(line)
                if job.run_dir is None:
                    m = re.search(r"log dir = (\S+)", line)
                    if m:
                        job.run_dir = Path(m.group(1)).name
            proc.wait()
            job.returncode = proc.returncode
            job.finished = time.time()
            if job.status != "cancelled":
                job.status = "done" if proc.returncode == 0 else "failed"
            logf.write(f"\n# exit code: {proc.returncode} "
                       f"({job.status}) at {datetime.now().isoformat(timespec='seconds')}\n")

    def cancel(self, job_id: str) -> bool:
        job = self.jobs.get(job_id)
        if not job or job.proc is None or job.status != "running":
            return False
        job.status = "cancelled"
        try:
            os.killpg(os.getpgid(job.proc.pid), signal.SIGINT)
        except (ProcessLookupError, PermissionError):
            try:
                job.proc.terminate()
            except Exception:
                return False
        return True

    def log_tail(self, job_id: str, max_bytes: int = 200_000) -> str | None:
        job = self.jobs.get(job_id)
        if not job:
            return None
        try:
            with job.log_path.open("rb") as fh:
                fh.seek(0, os.SEEK_END)
                size = fh.tell()
                fh.seek(max(0, size - max_bytes))
                data = fh.read()
            return data.decode("utf-8", errors="replace")
        except OSError:
            return ""

    def list(self) -> list[dict]:
        with self.lock:
            jobs = list(self.jobs.values())
        return [j.to_dict() for j in sorted(jobs, key=lambda j: j.started, reverse=True)]


# --------------------------------------------------------------------------
# HTTP handler
# --------------------------------------------------------------------------

_CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon",
    ".txt": "text/plain; charset=utf-8",
    ".log": "text/plain; charset=utf-8",
}


class Handler(BaseHTTPRequestHandler):
    server_version = "StatefulBenchWeb/1.0"
    state: AppState = None       # set on the server instance
    jobs: JobManager = None

    def log_message(self, fmt, *args):  # quieter logs
        sys.stderr.write("[web] %s - %s\n" % (self.address_string(), fmt % args))

    # ---- helpers ----
    def _send_json(self, obj, code: int = 200) -> None:
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, text: str, code: int = 200, ctype: str = "text/plain; charset=utf-8") -> None:
        body = text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: Path) -> None:
        if not path.is_file():
            self._send_text("not found", 404)
            return
        ctype = _CONTENT_TYPES.get(path.suffix.lower(), "application/octet-stream")
        data = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {}

    # ---- routing ----
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        q = parse_qs(parsed.query)
        try:
            if path == "/" or path == "/index.html":
                return self._send_file(STATIC_DIR / "index.html")
            if path.startswith("/static/"):
                rel = path[len("/static/"):]
                target = (STATIC_DIR / rel).resolve()
                if STATIC_DIR.resolve() in target.parents:
                    return self._send_file(target)
                return self._send_text("forbidden", 403)

            if path == "/api/state":
                return self._send_json(self._api_state())
            if path == "/api/tests":
                return self._send_json(self._api_tests())
            if path == "/api/metrics":
                return self._send_json(gather_metrics(self.state))
            if path == "/api/runs":
                return self._send_json({"runs": list_runs(self.state)})
            if path.startswith("/api/runs/"):
                run_id = path[len("/api/runs/"):]
                detail = run_details(self.state, run_id)
                if detail is None:
                    return self._send_json({"error": "run not found"}, 404)
                return self._send_json(detail)
            if path == "/api/jobs":
                return self._send_json({"jobs": self.jobs.list()})
            if path.startswith("/api/jobs/") and path.endswith("/log"):
                job_id = path[len("/api/jobs/"):-len("/log")]
                tail = self.jobs.log_tail(job_id)
                if tail is None:
                    return self._send_json({"error": "job not found"}, 404)
                job = self.jobs.jobs.get(job_id)
                return self._send_json({"log": tail, "job": job.to_dict() if job else None})

            # Serve files from a run dir (reports, logs): /runs/<id>/<file>
            if path.startswith("/runs/"):
                return self._serve_run_file(path)
            # Serve root-level generated reports: /report/<name>
            if path.startswith("/report/"):
                name = path[len("/report/"):]
                if "/" in name or ".." in name:
                    return self._send_text("forbidden", 403)
                return self._send_file(REPO / name)

            return self._send_text("not found", 404)
        except BrokenPipeError:
            pass
        except Exception as exc:  # pragma: no cover
            self._send_json({"error": f"{type(exc).__name__}: {exc}"}, 500)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path == "/api/run":
                return self._send_json(self._api_run(self._body_json()))
            if path == "/api/compare":
                return self._send_json(self._api_compare(self._body_json()))
            if path == "/api/reload-config":
                self.state.reload_config()
                return self._send_json(self.state.config_summary())
            if path.startswith("/api/jobs/") and path.endswith("/cancel"):
                job_id = path[len("/api/jobs/"):-len("/cancel")]
                ok = self.jobs.cancel(job_id)
                return self._send_json({"cancelled": ok})
            return self._send_text("not found", 404)
        except BrokenPipeError:
            pass
        except Exception as exc:  # pragma: no cover
            self._send_json({"error": f"{type(exc).__name__}: {exc}"}, 500)

    # ---- API implementations ----
    def _api_state(self) -> dict:
        return {
            "config": self.state.config_summary(),
            "jobs": self.jobs.list(),
            "runner_ok": runner is not None,
            "runner_error": _RUNNER_ERR,
        }

    def _api_tests(self) -> dict:
        names, source, warning = discover_test_names(self.state)
        parsed = [parse_test(n) for n in names]
        # Build facet -> value -> count.
        facets: dict[str, dict[str, int]] = {}
        for t in parsed:
            for k, v in t["dims"].items():
                facets.setdefault(k, {})[v] = facets.setdefault(k, {}).get(v, 0) + 1
        return {
            "tests": parsed,
            "facets": facets,
            "count": len(parsed),
            "source": source,
            "warning": warning,
        }

    def _common_flags(self, body: dict) -> list[str]:
        args: list[str] = []
        backend = body.get("reset_backend")
        if backend in ("overlayfs", "schelk"):
            args += ["--reset-backend", backend]
        if body.get("skip_gas_bump"):
            args.append("--skip-gas-bump")
        if body.get("persist_prelude"):
            args.append("--persist-prelude")
        if body.get("profile"):
            args.append("--profile")
        if body.get("dry_run"):
            args.append("--dry-run")
        limit = body.get("limit")
        if isinstance(limit, int) and limit > 0:
            args += ["--limit", str(limit)]
        return args

    def _api_run(self, body: dict) -> dict:
        tests = [t for t in (body.get("tests") or []) if isinstance(t, str) and t.strip()]
        if not tests:
            return {"error": "no tests selected"}
        job = self.jobs.start("run", tests, self._common_flags(body))
        return {"job": job.to_dict()}

    def _api_compare(self, body: dict) -> dict:
        tests = [t for t in (body.get("tests") or []) if isinstance(t, str) and t.strip()]
        if not tests:
            return {"error": "no tests selected"}
        image_y = (body.get("image_y") or "").strip()
        if not image_y:
            return {"error": "image_y is required for a comparison"}
        args = ["--compare", "--image-y", image_y]
        image_x = (body.get("image_x") or "").strip()
        if image_x:
            args += ["--image-x", image_x]
        if (body.get("label_x") or "").strip():
            args += ["--label-x", body["label_x"].strip()]
        if (body.get("label_y") or "").strip():
            args += ["--label-y", body["label_y"].strip()]
        args += self._common_flags(body)
        job = self.jobs.start("compare", tests, args)
        return {"job": job.to_dict()}

    def _serve_run_file(self, path: str) -> None:
        rel = path[len("/runs/"):]
        parts = rel.split("/", 1)
        if len(parts) != 2:
            return self._send_text("not found", 404)
        run_id, fname = parts
        if ".." in run_id or ".." in fname or "/" in fname:
            return self._send_text("forbidden", 403)
        target = (self.state.runs_dir / run_id / fname).resolve()
        try:
            base = (self.state.runs_dir / run_id).resolve()
        except OSError:
            return self._send_text("not found", 404)
        if base not in target.parents and base != target.parent:
            return self._send_text("forbidden", 403)
        return self._send_file(target)


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", "-c", default=None,
                        help="config file (default: config.yaml, else config.example.yaml)")
    parser.add_argument("--host", default="127.0.0.1",
                        help="bind host (default 127.0.0.1; use 0.0.0.0 to expose)")
    parser.add_argument("--port", "-p", type=int, default=8765, help="bind port (default 8765)")
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    if args.config:
        config_path = Path(args.config).expanduser().resolve()
    else:
        cand = REPO / "config.yaml"
        config_path = cand if cand.is_file() else (REPO / "config.example.yaml")
        config_path = config_path.resolve()

    state = AppState(config_path)
    jobs = JobManager(state)
    Handler.state = state
    Handler.jobs = jobs

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    summary = state.config_summary()
    print(f"Stateful-bench web console")
    print(f"  repo:    {REPO}")
    print(f"  config:  {config_path} ({'ok' if summary.get('ok') else 'ERROR: ' + str(summary.get('error'))})")
    print(f"  runs:    {state.runs_dir}")
    print(f"  serving: http://{args.host}:{args.port}")
    if args.host == "127.0.0.1":
        print(f"  tip:     port-forward from your laptop:  ssh -N -L {args.port}:127.0.0.1:{args.port} <vm>")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down")
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
