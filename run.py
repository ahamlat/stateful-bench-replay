#!/usr/bin/env python3
"""Stateful replay benchmark orchestrator.

Boots a Besu container against an OverlayFS-mounted snapshot and replays
JSON-RPC newPayload/forkchoiceUpdated lines through the Engine API.

Per sweep:
  1. Reset both overlay layers (prelude + test) and start Besu.
  2. Replay prelude (gas-bump.txt then funding.txt).
  3. Stop Besu.
  4. For each selected test:
       - reset ONLY the test overlay layer (prelude writes are preserved by
         the two-layer overlay, so gas-bump+funding don't have to be replayed)
       - start Besu, wait for Engine API
       - replay setup/<name>.txt then testing/<name>.txt
       - stop Besu
  5. Write summary.

Usage:
    python3 run.py --config config.yaml [--filter '*BALANCE*'] [--limit 1] [--dry-run]

See config.example.yaml for the schema.
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import fnmatch
import json
import os
import random
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path

import jwt
import requests
import yaml


# ---------------------------------------------------------------------------
# Config dataclasses
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class BesuConfig:
    image: str
    container_name: str
    data_snapshot_dir: Path
    overlay_dir: Path
    jwt_secret_path: Path
    engine_url: str
    extra_args: list[str]
    extra_mounts: list[str]
    startup_timeout_s: int
    container_data_path: str
    entrypoint: str | None


@dataclasses.dataclass
class InputConfig:
    dir: Path
    prelude: list[str]


@dataclasses.dataclass
class TestsConfig:
    setup_subdir: str
    testing_subdir: str
    filter: str
    order: str


@dataclasses.dataclass
class RunConfig:
    reset_overlay: bool
    log_dir: Path
    request_timeout_s: int
    fail_fast: bool
    stop_container_on_exit: bool


@dataclasses.dataclass
class ProfileConfig:
    """Async-profiler integration. When enabled (via config or --profile),
    the runner brackets the LAST newPayload+FCU pair of each phase with
    `asprof start`/`asprof stop` calls, producing one flame graph per phase
    in runs/<ts>/.
    """
    enabled: bool
    host_dir: Path                 # host path with bin/asprof
    container_dir: str             # mount target inside container
    event: str                     # cpu | wall | itimer | alloc | lock | ...
    interval: str                  # asprof -i value (e.g. "1ms", "500us")
    output_format: str             # html | jfr | flamegraph (-> .html)
    extra_args: list[str]          # extra flags passed to asprof start


@dataclasses.dataclass
class Config:
    besu: BesuConfig
    input: InputConfig
    tests: TestsConfig
    run: RunConfig
    profile: ProfileConfig


def _abs_path(p: str | os.PathLike) -> Path:
    return Path(p).expanduser().resolve()


def load_config(path: Path) -> Config:
    raw = yaml.safe_load(path.read_text())
    b, i, t, r = raw["besu"], raw["input"], raw["tests"], raw["run"]
    if "isolation" in r:
        print(
            "warn: run.isolation is no longer used (only the per-test restart mode "
            "is supported); ignoring it",
            file=sys.stderr,
        )
    return Config(
        besu=BesuConfig(
            image=b["image"],
            container_name=b.get("container_name", "besu-bench"),
            data_snapshot_dir=_abs_path(b["data_snapshot_dir"]),
            overlay_dir=_abs_path(b["overlay_dir"]),
            jwt_secret_path=_abs_path(b["jwt_secret_path"]),
            engine_url=b["engine_url"].rstrip("/"),
            extra_args=list(b.get("extra_args") or []),
            extra_mounts=list(b.get("extra_mounts") or []),
            startup_timeout_s=int(b.get("startup_timeout_s", 120)),
            container_data_path=str(b.get("container_data_path", "/opt/besu/data")),
            entrypoint=(str(b["entrypoint"]) if b.get("entrypoint") else None),
        ),
        input=InputConfig(
            dir=_abs_path(i["dir"]),
            prelude=list(i.get("prelude") or []),
        ),
        tests=TestsConfig(
            setup_subdir=t.get("setup_subdir", "setup"),
            testing_subdir=t.get("testing_subdir", "testing"),
            filter=str(t.get("filter", "*")),
            order=str(t.get("order", "alphabetical")),
        ),
        run=RunConfig(
            reset_overlay=bool(r.get("reset_overlay", True)),
            log_dir=_abs_path(r.get("log_dir", "./runs")),
            request_timeout_s=int(r.get("request_timeout_s", 120)),
            fail_fast=bool(r.get("fail_fast", False)),
            stop_container_on_exit=bool(r.get("stop_container_on_exit", True)),
        ),
        profile=_load_profile(raw.get("profile")),
    )


def _load_profile(raw: dict | None) -> ProfileConfig:
    raw = raw or {}
    return ProfileConfig(
        enabled=bool(raw.get("enabled", False)),
        host_dir=_abs_path(raw.get("host_dir", "~/async-profiler")),
        container_dir=str(raw.get("container_dir", "/opt/async-profiler")),
        event=str(raw.get("event", "cpu")),
        interval=str(raw.get("interval", "1ms")),
        output_format=str(raw.get("output_format", "html")),
        extra_args=list(raw.get("extra_args") or []),
    )


# ---------------------------------------------------------------------------
# Logging helper
# ---------------------------------------------------------------------------

class SweepLog:
    """Append-only failure log + summary counters for a sweep."""

    def __init__(self, root: Path):
        root.mkdir(parents=True, exist_ok=True)
        self.root = root
        self.failures_path = root / "failures.jsonl"
        self.summary_path = root / "summary.json"
        self.events_path = root / "events.log"
        self._failures = self.failures_path.open("a", buffering=1)
        self._events = self.events_path.open("a", buffering=1)
        self.counters: dict[str, dict[str, int]] = {}

    def event(self, msg: str) -> None:
        ts = dt.datetime.now().isoformat(timespec="seconds")
        line = f"[{ts}] {msg}"
        print(line, flush=True)
        self._events.write(line + "\n")

    def _bucket(self, name: str) -> dict[str, int]:
        return self.counters.setdefault(name, {"ok": 0, "fail": 0, "total": 0})

    def record_ok(self, source: str) -> None:
        b = self._bucket(source)
        b["ok"] += 1
        b["total"] += 1

    def record_fail(self, source: str, line_no: int, kind: str, detail: dict) -> None:
        b = self._bucket(source)
        b["fail"] += 1
        b["total"] += 1
        rec = {
            "ts": dt.datetime.now().isoformat(timespec="milliseconds"),
            "source": source,
            "line": line_no,
            "kind": kind,
            **detail,
        }
        self._failures.write(json.dumps(rec) + "\n")

    def flush_summary(self, extra: dict | None = None) -> None:
        summary = {
            "finished_at": dt.datetime.now().isoformat(timespec="seconds"),
            "files": self.counters,
            "totals": {
                "ok": sum(b["ok"] for b in self.counters.values()),
                "fail": sum(b["fail"] for b in self.counters.values()),
                "total": sum(b["total"] for b in self.counters.values()),
            },
        }
        if extra:
            summary.update(extra)
        self.summary_path.write_text(json.dumps(summary, indent=2))

    def close(self) -> None:
        try:
            self._failures.close()
        finally:
            self._events.close()


# ---------------------------------------------------------------------------
# Shell / docker helpers
# ---------------------------------------------------------------------------

def _run(cmd: list[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    if capture:
        return subprocess.run(cmd, check=check, text=True, capture_output=True)
    return subprocess.run(cmd, check=check)


# Always invoke docker via passwordless sudo. This avoids relying on docker
# group membership and matches the project's "everything privileged goes
# through sudoers NOPASSWD" model (overlay.sh + docker).
DOCKER = ["sudo", "-n", "docker"]

# Where the runner bind-mounts the per-run profile output dir inside the
# Besu container. asprof writes flame graphs there; because it is a bind
# mount, files appear directly under runs/<ts>/profiles/ on the host.
PROFILE_OUTPUT_CONTAINER_DIR = "/opt/besu/profile-output"


def _container_exists(name: str) -> bool:
    res = _run(
        DOCKER + ["ps", "-a", "--filter", f"name=^{name}$", "--format", "{{.Names}}"],
        capture=True,
    )
    return name in res.stdout.split()


def _container_running(name: str) -> bool:
    res = _run(
        DOCKER + ["ps", "--filter", f"name=^{name}$", "--format", "{{.Names}}"],
        capture=True,
    )
    return name in res.stdout.split()


def _dump_container_logs(name: str, log: SweepLog, tail: int = 200) -> None:
    if not _container_exists(name):
        log.event(f"container {name} no longer exists; cannot dump logs")
        return
    res = _run(
        DOCKER + ["logs", "--tail", str(tail), name],
        check=False,
        capture=True,
    )
    out = (res.stdout or "") + (res.stderr or "")
    log.event(f"--- last {tail} lines of `sudo docker logs {name}` ---")
    for line in out.splitlines():
        log.event(f"  | {line}")
    log.event("--- end of container logs ---")


def stop_container(name: str) -> None:
    if not _container_exists(name):
        return
    _run(DOCKER + ["rm", "-f", name], check=False, capture=True)


def save_container_logs(name: str, dest: Path, log: SweepLog) -> None:
    """Save the full container log to `dest` before we tear the container
    down. Done with `docker logs` (no --tail) so the file is canonical."""
    if not _container_exists(name):
        log.event(f"save_container_logs: {name} no longer exists, skipping")
        return
    res = _run(DOCKER + ["logs", name], check=False, capture=True)
    out = (res.stdout or "") + (res.stderr or "")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(out)
    log.event(f"saved {len(out.splitlines())} log lines to {dest}")


# ---------------------------------------------------------------------------
# Async-profiler integration
# ---------------------------------------------------------------------------

class ProfilerSession:
    """Brackets a critical section with `asprof start` / `asprof stop`.

    asprof is invoked via `docker exec` inside the besu-bench container, so
    we don't have to fight host/container PID namespacing. PID 1 inside the
    container is the JVM (Besu's launcher exec's java in place).

    We choose the output filename when the session is created; that way the
    flame graph appears in runs/<ts>/<output_name> directly (the per-run
    log dir is bind-mounted into the container).
    """

    def __init__(self, cfg: ProfileConfig, container: str, output_name: str,
                 log: SweepLog):
        self.cfg = cfg
        self.container = container
        self.output_name = output_name
        self.log = log
        self.started = False

    @property
    def _asprof(self) -> str:
        return f"{self.cfg.container_dir}/bin/asprof"

    @property
    def _container_output_path(self) -> str:
        return f"{PROFILE_OUTPUT_CONTAINER_DIR}/{self.output_name}"

    def _exec(self, *args: str) -> subprocess.CompletedProcess:
        cmd = DOCKER + ["exec", self.container] + list(args)
        return _run(cmd, capture=True, check=False)

    def start(self) -> None:
        # Note on PID: the entrypoint we use (/opt/besu/bin/besu) is a shell
        # script that exec's java, so PID 1 is the JVM by the time we get
        # here. If you use a different entrypoint, adjust accordingly.
        args = [
            self._asprof, "start",
            "-e", self.cfg.event,
            "-i", self.cfg.interval,
            *self.cfg.extra_args,
            "1",
        ]
        res = self._exec(*args)
        if res.returncode != 0:
            err = (res.stderr or res.stdout or "").strip()
            self.log.event(f"asprof start FAILED ({res.returncode}): {err}")
            self.log.event(
                "  hint: async-profiler needs kernel.perf_event_paranoid<=1 "
                "for `cpu` event. Try: sudo sysctl -w kernel.perf_event_paranoid=1, "
                "or set profile.event=wall to avoid perf_events entirely."
            )
            return
        self.started = True
        self.log.event(
            f"asprof started (event={self.cfg.event}, "
            f"output -> runs/.../{self.output_name})"
        )

    def stop(self) -> None:
        if not self.started:
            return
        args = [
            self._asprof, "stop",
            "-f", self._container_output_path,
            "1",
        ]
        res = self._exec(*args)
        if res.returncode != 0:
            err = (res.stderr or res.stdout or "").strip()
            self.log.event(f"asprof stop FAILED ({res.returncode}): {err}")
        else:
            self.log.event(f"asprof stopped, wrote {self.output_name}")
        self.started = False


def _profile_output_filename(idx: int, phase: str, fmt: str) -> str:
    """e.g. profile-0001-setup.html"""
    ext_map = {"html": "html", "flamegraph": "html", "jfr": "jfr"}
    ext = ext_map.get(fmt, fmt)
    return f"profile-{idx:04d}-{phase}.{ext}"


def start_besu(
    cfg: BesuConfig,
    log: SweepLog,
    *,
    profile: ProfileConfig | None = None,
    profile_output_dir: Path | None = None,
) -> None:
    """Boot Besu with its data-path bound to the test overlay layer.

    Each benchmark run does the entire flow (gas-bump + funding + setup +
    testing) in this single container, so there is no need to stage writes
    across multiple overlay layers anymore. We always mount
    <overlay_dir>/test/merged; the prelude overlay layer remains in the
    on-disk layout for backwards compatibility but no longer holds state.

    If `profile` is enabled, also bind-mount async-profiler read-only and
    a writable output dir for flame graphs.
    """
    stop_container(cfg.container_name)
    merged = cfg.overlay_dir / "test" / "merged"
    # NOTE: no --rm here. We want the container to stick around if Besu
    # crashes, so wait_for_engine can dump `docker logs` on failure.
    # stop_container() above and at end-of-test cleans it up.
    docker_cmd: list[str] = list(DOCKER) + [
        "run", "-d",
        "--name", cfg.container_name,
        "--network", "host",
        "-v", f"{merged}:{cfg.container_data_path}",
    ]
    if cfg.entrypoint:
        docker_cmd += ["--entrypoint", cfg.entrypoint]
    for spec in cfg.extra_mounts:
        docker_cmd += ["-v", spec]
    if profile is not None and profile.enabled:
        if not (profile.host_dir / "bin" / "asprof").exists():
            raise RuntimeError(
                f"profile.enabled=true but {profile.host_dir / 'bin' / 'asprof'} "
                "does not exist. Run scripts/install-async-profiler.sh first."
            )
        if profile_output_dir is None:
            raise RuntimeError("start_besu: profile.enabled but no profile_output_dir given")
        profile_output_dir.mkdir(parents=True, exist_ok=True)
        docker_cmd += [
            "-v", f"{profile.host_dir}:{profile.container_dir}:ro",
            "-v", f"{profile_output_dir}:{PROFILE_OUTPUT_CONTAINER_DIR}",
        ]
    docker_cmd.append(cfg.image)
    docker_cmd.append(f"--data-path={cfg.container_data_path}")
    docker_cmd += cfg.extra_args
    log.event("docker run: " + " ".join(shlex.quote(a) for a in docker_cmd))
    _run(docker_cmd, capture=True)


# ---------------------------------------------------------------------------
# Overlay helpers
# ---------------------------------------------------------------------------

OVERLAY_SCRIPT = Path(__file__).resolve().parent / "scripts" / "overlay.sh"


def _overlay(action: str, cfg: BesuConfig, log: SweepLog, *extra_args: str) -> None:
    cmd = ["sudo", "-n", str(OVERLAY_SCRIPT), action]
    if action in ("init", "mount-all", "reset-all", "reset-test"):
        cmd += [str(cfg.data_snapshot_dir), str(cfg.overlay_dir)]
    else:
        cmd += [str(cfg.overlay_dir)]
    cmd += list(extra_args)
    log.event(f"overlay {action}: " + " ".join(shlex.quote(a) for a in cmd))
    try:
        _run(cmd)
    except subprocess.CalledProcessError:
        # The two-layer actions (mount-all, reset-all, reset-test) were added
        # together; if any of them is rejected with "unknown action", the
        # installed overlay.sh is from before that change.
        if action in ("mount-all", "reset-all", "reset-test"):
            print(
                "\nbench: overlay.sh rejected this action. The installed copy is "
                "probably an older single-layer version.\n"
                "       Refresh it with:\n"
                f"         sudo install -m 0755 {OVERLAY_SCRIPT} /usr/local/sbin/besu-overlay.sh\n"
                f"         ln -sf /usr/local/sbin/besu-overlay.sh {OVERLAY_SCRIPT}\n"
                "       (and make sure your sudoers allowlists the new script path).\n",
                file=sys.stderr,
            )
        raise


def overlay_reset_all(cfg: BesuConfig, log: SweepLog) -> None:
    _overlay("reset-all", cfg, log)


def overlay_mount_all(cfg: BesuConfig, log: SweepLog) -> None:
    _overlay("mount-all", cfg, log)


def overlay_reset_test(cfg: BesuConfig, log: SweepLog) -> None:
    _overlay("reset-test", cfg, log)


# ---------------------------------------------------------------------------
# JWT + Engine API
# ---------------------------------------------------------------------------

def load_jwt_secret(path: Path) -> bytes:
    raw = path.read_text().strip()
    if raw.startswith("0x"):
        raw = raw[2:]
    return bytes.fromhex(raw)


def make_jwt(secret: bytes) -> str:
    return jwt.encode({"iat": int(time.time())}, secret, algorithm="HS256")


def wait_for_engine(cfg: BesuConfig, secret: bytes, log: SweepLog) -> None:
    deadline = time.monotonic() + cfg.startup_timeout_s
    payload = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "engine_exchangeCapabilities", "params": [[]],
    })
    log.event(f"waiting for Engine API at {cfg.engine_url} (timeout {cfg.startup_timeout_s}s)")
    last_err: str | None = None
    while time.monotonic() < deadline:
        if not _container_running(cfg.container_name):
            log.event(f"container {cfg.container_name} exited before Engine API came up")
            _dump_container_logs(cfg.container_name, log)
            raise RuntimeError(
                f"Besu container {cfg.container_name} exited during startup; "
                "see container logs above (also in events.log)"
            )
        try:
            r = requests.post(
                cfg.engine_url,
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {make_jwt(secret)}",
                },
                timeout=5,
            )
            if r.status_code == 200 and "result" in r.json():
                log.event("Engine API is up")
                return
            last_err = f"HTTP {r.status_code}: {r.text[:200]}"
        except (requests.RequestException, ValueError) as e:
            last_err = repr(e)
        time.sleep(2)
    _dump_container_logs(cfg.container_name, log)
    raise RuntimeError(
        f"Engine API did not become ready in {cfg.startup_timeout_s}s; "
        f"last error: {last_err} (see container logs above)"
    )


# For deterministic replay against a prepared snapshot we expect every
# block we send to be fully imported, so the only acceptable status is
# VALID. ACCEPTED means "looks ok but parent unknown", SYNCING means
# "parent missing, please backfill" - both indicate the chain is not
# being built where we think it is and silently accepting them masks
# bugs (e.g. wrong overlay layer => prelude state missing).
_NEWPAYLOAD_OK = {"VALID"}
_FCU_OK = {"VALID"}


def _rpc_http_url(cfg: BesuConfig) -> str:
    """Best-effort: derive the unauthenticated JSON-RPC URL from extra_args.
    Falls back to http://127.0.0.1:8545."""
    port = "8545"
    for a in cfg.extra_args:
        if a.startswith("--rpc-http-port="):
            port = a.split("=", 1)[1]
    return f"http://127.0.0.1:{port}"


def query_chain_head(cfg: BesuConfig) -> tuple[int, str] | None:
    """Ask Besu for its current chain head over the unauthenticated JSON-RPC
    port. Returns (block_number, block_hash) or None on error.

    Note: we use the plain RPC port here (not the Engine API) because it
    needs no JWT and serves the same chain view.
    """
    url = _rpc_http_url(cfg)
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBlockByNumber",
        "params": ["latest", False],
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            return None
        body = r.json()
        result = body.get("result") or {}
        num_hex = result.get("number")
        h = result.get("hash")
        if not num_hex or not h:
            return None
        return int(num_hex, 16), h
    except (requests.RequestException, ValueError):
        return None


def log_chain_head(cfg: BesuConfig, log: SweepLog, prefix: str) -> None:
    head = query_chain_head(cfg)
    if head is None:
        log.event(f"{prefix}: could not read chain head over RPC")
    else:
        n, h = head
        log.event(f"{prefix}: head = #{n:,} ({h})")


def _classify(method: str, body: dict) -> tuple[bool, str, dict]:
    """Return (ok, kind, detail). kind is empty when ok."""
    if "error" in body:
        return False, "rpc_error", {"error": body["error"]}
    result = body.get("result")
    if not isinstance(result, dict):
        return False, "no_result", {"body": body}
    if method.startswith("engine_newPayload"):
        status = (result.get("status") or "").upper()
        if status in _NEWPAYLOAD_OK:
            return True, "", {}
        return False, "newpayload_not_valid", {"result": result}
    if method.startswith("engine_forkchoiceUpdated"):
        ps = result.get("payloadStatus") or {}
        status = (ps.get("status") or "").upper()
        if status in _FCU_OK:
            return True, "", {}
        return False, "fcu_not_valid", {"result": result}
    return True, "", {}


def post_engine_line(
    cfg: Config,
    secret: bytes,
    session: requests.Session,
    raw: str,
) -> tuple[int, dict | None, str | None]:
    """Low-level POST. Returns (http_status, json_body, transport_error_repr)."""
    try:
        resp = session.post(
            cfg.besu.engine_url,
            data=raw,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {make_jwt(secret)}",
            },
            timeout=cfg.run.request_timeout_s,
        )
    except requests.RequestException as e:
        return -1, None, repr(e)
    try:
        body = resp.json()
    except ValueError as e:
        return resp.status_code, None, f"bad_json:{e!r}:{resp.text[:200]}"
    return resp.status_code, body, None


def _scan_lines(file_path: Path) -> list[tuple[int, str, str]]:
    """Return [(line_no, method, raw_line)] for non-empty, json-decodable
    lines. Bad lines are surfaced later through the normal failure path; we
    only use this scan to find the index of the last newPayload."""
    out: list[tuple[int, str, str]] = []
    with file_path.open("r") as fh:
        for line_no, raw in enumerate(fh, start=1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                method = json.loads(raw).get("method", "?")
            except json.JSONDecodeError:
                method = ""
            out.append((line_no, method, raw))
    return out


def replay_file(
    cfg: Config,
    secret: bytes,
    session: requests.Session,
    file_path: Path,
    log: SweepLog,
    phase: str | None = None,
    profiler: ProfilerSession | None = None,
) -> bool:
    """Replay one .txt file line-by-line. Returns False iff fail_fast tripped.

    `phase` is an optional human label ("setup", "testing", ...) prepended to
    the event log so that paired files (setup/<name>.txt and testing/<name>.txt
    sharing the same basename) can be told apart at a glance.

    If `profiler` is given, async-profiler is started just before the file's
    LAST newPayload call and stopped after the LAST line of the file has been
    processed. That brackets exactly the heavy block (and its trailing FCU)
    in setup/, and the single measured block in testing/.
    """
    label = file_path.name
    prefix = f"replay [{phase}] " if phase else "replay "
    log.event(f"{prefix}{label}")

    items = _scan_lines(file_path)

    # Index of the LAST engine_newPayload* line, or -1 if none.
    last_np_idx = -1
    for i, (_ln, method, _raw) in enumerate(items):
        if method.startswith("engine_newPayload"):
            last_np_idx = i

    profile_active = False
    for i, (line_no, method, raw) in enumerate(items):
        if not method:
            log.record_fail(label, line_no, "bad_json", {})
            if cfg.run.fail_fast:
                return False
            continue

        if profiler is not None and i == last_np_idx and not profile_active:
            profiler.start()
            profile_active = True

        status, body, err = post_engine_line(cfg, secret, session, raw)
        if err is not None and body is None:
            log.record_fail(label, line_no, "http_error", {"method": method, "error": err})
            if cfg.run.fail_fast:
                if profile_active:
                    profiler.stop()
                return False
            continue
        if status != 200:
            log.record_fail(label, line_no, "http_status",
                            {"method": method, "status": status,
                             "body": json.dumps(body) if body is not None else err})
            if cfg.run.fail_fast:
                if profile_active:
                    profiler.stop()
                return False
            continue

        ok, kind, detail = _classify(method, body or {})
        if ok:
            log.record_ok(label)
        else:
            log.record_fail(label, line_no, kind, {"method": method, **detail})
            if cfg.run.fail_fast:
                if profile_active:
                    profiler.stop()
                return False

    if profile_active:
        profiler.stop()
    return True


# ---------------------------------------------------------------------------
# Test discovery
# ---------------------------------------------------------------------------

def discover_tests(cfg: Config, filter_override: str | None, limit: int | None) -> list[str]:
    """Return ordered list of basenames present in BOTH setup/ and testing/ that match the filter."""
    pattern = filter_override or cfg.tests.filter
    setup_dir = cfg.input.dir / cfg.tests.setup_subdir
    testing_dir = cfg.input.dir / cfg.tests.testing_subdir
    if not setup_dir.is_dir():
        raise FileNotFoundError(f"setup dir missing: {setup_dir}")
    if not testing_dir.is_dir():
        raise FileNotFoundError(f"testing dir missing: {testing_dir}")

    setup_names = {p.name for p in setup_dir.iterdir() if p.is_file()}
    testing_names = {p.name for p in testing_dir.iterdir() if p.is_file()}
    paired = setup_names & testing_names
    only_setup = setup_names - testing_names
    only_testing = testing_names - setup_names
    if only_setup:
        print(f"warn: {len(only_setup)} files in setup/ have no testing/ pair (skipped)", file=sys.stderr)
    if only_testing:
        print(f"warn: {len(only_testing)} files in testing/ have no setup/ pair (skipped)", file=sys.stderr)

    matched = sorted(n for n in paired if fnmatch.fnmatch(n, pattern))
    order = cfg.tests.order
    if order == "alphabetical":
        pass
    elif order == "as_listed":
        listing = [p.name for p in setup_dir.iterdir() if p.is_file()]
        order_index = {n: i for i, n in enumerate(listing)}
        matched.sort(key=lambda n: order_index.get(n, 0))
    elif order == "shuffled":
        random.shuffle(matched)
    else:
        raise ValueError(f"unknown tests.order: {order}")
    if limit is not None and limit >= 0:
        matched = matched[:limit]
    return matched


# ---------------------------------------------------------------------------
# Sweep orchestration
# ---------------------------------------------------------------------------

def _run_test_pair(cfg: Config, secret: bytes, session: requests.Session,
                   setup_dir: Path, testing_dir: Path, name: str, log: SweepLog,
                   *,
                   setup_profiler: ProfilerSession | None = None,
                   testing_profiler: ProfilerSession | None = None) -> bool:
    if not replay_file(cfg, secret, session, setup_dir / name, log,
                       phase="setup", profiler=setup_profiler):
        log.event(f"fail-fast tripped during setup of {name}")
        return False
    if not replay_file(cfg, secret, session, testing_dir / name, log,
                       phase="testing", profiler=testing_profiler):
        log.event(f"fail-fast tripped during testing of {name}")
        return False
    return True


def _interactive_pick(tests: list[str], log: SweepLog) -> list[str]:
    """List matched tests and let the user pick exactly one."""
    if not tests:
        return tests
    if len(tests) == 1:
        log.event(f"--pick: only one match, running it: {tests[0]}")
        return tests
    if not sys.stdin.isatty():
        raise RuntimeError(
            "--pick requires an interactive terminal but stdin is not a TTY. "
            "Tighten --filter or use --limit 1 for non-interactive selection."
        )
    print()
    print(f"Pick a test ({len(tests)} match the filter):")
    for i, name in enumerate(tests, start=1):
        print(f"  [{i:>4}] {name}")
    print()
    while True:
        raw = input(f"Enter a number 1..{len(tests)} (q to abort): ").strip()
        if raw.lower() in ("q", "quit", "exit"):
            raise KeyboardInterrupt
        try:
            choice = int(raw)
        except ValueError:
            print(f"  not a number: {raw!r}")
            continue
        if 1 <= choice <= len(tests):
            picked = tests[choice - 1]
            log.event(f"--pick: selected {choice}/{len(tests)} -> {picked}")
            return [picked]
        print(f"  out of range, must be 1..{len(tests)}")


def run_sweep(cfg: Config, filter_override: str | None, limit: int | None,
              pick: bool, dry_run: bool) -> int:
    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    log_root = cfg.run.log_dir / timestamp
    log = SweepLog(log_root)
    log.event(f"sweep start, log dir = {log_root}")

    tests = discover_tests(cfg, filter_override, limit)
    log.event(
        f"matched {len(tests)} tests "
        f"(filter={filter_override or cfg.tests.filter}, order={cfg.tests.order}, limit={limit})"
    )

    if pick and not dry_run:
        tests = _interactive_pick(tests, log)

    # Always record the resolved selection so a real run can be audited later
    # (and so `--pick --dry-run` still prints a stable preview file).
    (log_root / "selected_tests.txt").write_text("\n".join(tests) + "\n")

    if dry_run:
        log.event("dry-run: wrote selected_tests.txt and exiting")
        if pick:
            print()
            print(f"Pick preview ({len(tests)} match):")
            for i, name in enumerate(tests, start=1):
                print(f"  [{i:>4}] {name}")
        log.flush_summary({"dry_run": True, "selected": len(tests)})
        log.close()
        return 0

    for name in cfg.input.prelude:
        p = cfg.input.dir / name
        if not p.is_file():
            raise FileNotFoundError(f"prelude file missing: {p}")
    if not cfg.besu.jwt_secret_path.is_file():
        raise FileNotFoundError(f"jwt secret missing: {cfg.besu.jwt_secret_path}")

    # Validate every host bind-mount source up front: if it doesn't exist
    # `docker run` silently creates an empty directory there, the container
    # then sees a directory where it expected a file (e.g. genesis), Besu
    # fails before log4j and `docker logs` is empty. We've been bitten.
    for spec in cfg.besu.extra_mounts:
        host = spec.split(":", 1)[0]
        if not host or not Path(host).exists():
            raise FileNotFoundError(
                f"besu.extra_mounts host path does not exist: {host!r} "
                f"(from spec {spec!r}). Create it before running, or fix the "
                "config: a missing host path makes docker silently create an "
                "empty directory and Besu will fail to start with no logs."
            )

    # Preflight: passwordless sudo for both helpers we depend on.
    for probe, hint in (
        (DOCKER + ["version", "--format", "{{.Server.Version}}"],
         "sudo -n docker version"),
        (["sudo", "-n", str(OVERLAY_SCRIPT), "--help"],
         f"sudo -n {OVERLAY_SCRIPT} --help"),
    ):
        try:
            _run(probe, capture=True)
        except subprocess.CalledProcessError as e:
            stderr = (e.stderr or "").strip()
            raise RuntimeError(
                f"`{hint}` failed with exit {e.returncode}: {stderr}\n"
                "Add a passwordless sudo entry; see README 'AWS VM bootstrap'."
            ) from None

    secret = load_jwt_secret(cfg.besu.jwt_secret_path)
    setup_dir = cfg.input.dir / cfg.tests.setup_subdir
    testing_dir = cfg.input.dir / cfg.tests.testing_subdir

    started_container = False
    sweep_ok = True

    try:
        with requests.Session() as session:
            # Each test runs end-to-end in ONE Besu container:
            #   reset overlay -> start Besu -> gas-bump -> funding ->
            #   setup -> testing -> stop Besu
            # No mid-flight restarts: every test sees the exact same warm-up
            # path before the measured block.
            for idx, name in enumerate(tests, start=1):
                log.event(f"[{idx}/{len(tests)}] {name}")

                # Wipe both overlay layers (prelude + test) and remount.
                # Everything below writes into the test layer.
                overlay_reset_all(cfg.besu, log)

                start_besu(
                    cfg.besu, log,
                    profile=cfg.profile if cfg.profile.enabled else None,
                    profile_output_dir=log.root if cfg.profile.enabled else None,
                )
                started_container = True
                wait_for_engine(cfg.besu, secret, log)
                log_chain_head(
                    cfg.besu, log,
                    f"[{idx}/{len(tests)}] head BEFORE prelude"
                )

                # Build profiler sessions for this test (one per phase).
                # The runner brackets the LAST newPayload+FCU pair of each
                # phase, which corresponds to the heavy setup block and the
                # measured testing block respectively.
                setup_profiler: ProfilerSession | None = None
                testing_profiler: ProfilerSession | None = None
                if cfg.profile.enabled:
                    setup_profiler = ProfilerSession(
                        cfg.profile,
                        cfg.besu.container_name,
                        _profile_output_filename(idx, "setup", cfg.profile.output_format),
                        log,
                    )
                    testing_profiler = ProfilerSession(
                        cfg.profile,
                        cfg.besu.container_name,
                        _profile_output_filename(idx, "testing", cfg.profile.output_format),
                        log,
                    )

                test_ok = True

                # Phase A: prelude (gas-bump.txt then funding.txt) in this
                # same container. Writes go into the test overlay layer.
                # Prelude is NOT profiled - it is identical across tests
                # and not what we are measuring.
                for fname in cfg.input.prelude:
                    if not replay_file(
                        cfg, secret, session,
                        cfg.input.dir / fname, log, phase="prelude",
                    ):
                        test_ok = False
                        log.event(f"fail-fast tripped during prelude {fname}")
                        break

                if test_ok:
                    log_chain_head(
                        cfg.besu, log,
                        f"[{idx}/{len(tests)}] head AFTER prelude"
                    )
                    # Phase B: setup + testing in the SAME container.
                    if not _run_test_pair(
                        cfg, secret, session,
                        setup_dir, testing_dir, name, log,
                        setup_profiler=setup_profiler,
                        testing_profiler=testing_profiler,
                    ):
                        test_ok = False
                    else:
                        log_chain_head(
                            cfg.besu, log,
                            f"[{idx}/{len(tests)}] head AFTER replay"
                        )

                # Persist full container log before we tear it down. Failed
                # runs get a -FAIL suffix to make them easy to spot.
                suffix = "" if test_ok else "-FAIL"
                save_container_logs(
                    cfg.besu.container_name,
                    log.root / f"besu-{idx:04d}{suffix}.log",
                    log,
                )
                stop_container(cfg.besu.container_name)
                started_container = False

                if not test_ok:
                    sweep_ok = False
                    break

        log.event(f"sweep end: ok={sweep_ok}")
    finally:
        log.flush_summary({
            "config": {
                "image": cfg.besu.image,
                "snapshot": str(cfg.besu.data_snapshot_dir),
                "input_dir": str(cfg.input.dir),
                "filter": filter_override or cfg.tests.filter,
                "order": cfg.tests.order,
                "limit": limit,
                "selected_tests": len(tests),
            },
            "fail_fast_tripped": not sweep_ok,
        })
        if started_container and cfg.run.stop_container_on_exit:
            log.event(f"stopping container {cfg.besu.container_name}")
            stop_container(cfg.besu.container_name)
        log.close()

    return 0 if sweep_ok else 1


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stateful replay benchmark for Besu.")
    p.add_argument("--config", "-c", default="config.yaml", help="path to YAML config")
    p.add_argument("--filter", "-f", default=None,
                   help="override tests.filter glob (e.g. '*BALANCE*30M*')")
    p.add_argument("--limit", "-n", type=int, default=None,
                   help="run at most N tests after filtering (use --limit 1 for a single test)")
    p.add_argument("--pick", "-p", action="store_true",
                   help="list matched tests and prompt to pick exactly one (interactive)")
    p.add_argument("--dry-run", action="store_true",
                   help="resolve config + selected tests, then exit without touching the system")
    p.add_argument("--profile", action="store_true",
                   help="enable async-profiler around the last newPayload+FCU pair "
                        "of setup/ and testing/ (overrides profile.enabled in yaml)")
    return p.parse_args(argv)


def _install_sigint_handler() -> None:
    def _handle(signum, frame):  # noqa: ARG001
        print("\nbench: caught SIGINT, propagating", file=sys.stderr)
        raise KeyboardInterrupt
    signal.signal(signal.SIGINT, _handle)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    cfg = load_config(_abs_path(args.config))
    if args.profile:
        cfg.profile.enabled = True
    _install_sigint_handler()
    return run_sweep(cfg, filter_override=args.filter, limit=args.limit,
                     pick=args.pick, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
