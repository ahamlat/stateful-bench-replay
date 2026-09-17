#!/usr/bin/env python3
"""Stateful replay benchmark orchestrator.

Boots a Besu container against an OverlayFS-mounted snapshot and replays
JSON-RPC newPayload/forkchoiceUpdated lines through the Engine API.

Isolation (`run.isolation`):
  restart (stateful, default): reset disk, start Besu, prelude, one test, stop.
  rewind (compute): start Besu once, prelude once, then for each test replay
    setup+testing and FCU (optional debug_setHead) back to the pre-run head.

Usage:
    python3 run.py --config config.yaml [--filter '*BALANCE*'] [--limit 1] [--dry-run]

Compare mode (run the suite on two Besu images and diff the testing-block
times into an HTML report under runs/<ts>-compare/):
    python3 run.py --compare --image-x <imgX> --image-y <imgY> [--filter ...]

See config.example.yaml for the schema.
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import fnmatch
import functools
import json
import os
import random
import re
import shutil
import shlex
import signal
import subprocess
import sys
import time
from collections.abc import Iterable
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
    bumped_snapshot_dir: Path | None  # pre-bumped snapshot used when gas-bump is
                                      # skipped (default <data_snapshot_dir>-bumped)
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
    gas_bump_file: str   # prelude entry treated as the gas-bump (skipped when
                         # run.skip_gas_bump / --skip-gas-bump is set)


@dataclasses.dataclass
class TestsConfig:
    setup_subdir: str
    testing_subdir: str
    format: str
    fixtures_subdir: str
    filter: str
    order: str
    match_chain_head: bool  # stateful_engine: keep only fixtures that
                            # chain onto the head after the pre-run


@dataclasses.dataclass
class SchelkConfig:
    """Parameters for the schelk (dm-era) reset backend. Only consulted when
    run.reset_backend == 'schelk'. The mount point is NOT configured here: it
    is always <overlay_dir>/test/merged so Besu's existing bind mount keeps
    working regardless of backend.
    """
    bin: str                # path to the schelk binary (absolute recommended;
                            # sudo sanitises PATH so ~/.cargo/bin is invisible)
    virgin: str             # pristine baseline block device (holds the snapshot)
    scratch: str            # working block device Besu mounts and writes to
    ramdisk: str            # block device for dm-era metadata (e.g. /dev/ram0)
    fstype: str             # filesystem on the volumes (ext4, xfs, ...)
    granularity: int | None # dm-era block granularity in bytes (default 4096)
    ramdisk_size_kb: int | None  # size to modprobe brd with if ramdisk absent
    no_copy: bool           # init-from --no-copy (volumes already identical)


@dataclasses.dataclass
class RunConfig:
    reset_overlay: bool
    reset_backend: str
    isolation: str        # restart | rewind (see ISOLATION_MODES)
    rewind_debug_sethead: bool  # also call debug_setHead (Besu compute: false)
    rewind_fcu_version: int     # engine_forkchoiceUpdatedV* for rewind FCU
    post_test_sleep_s: float    # pause after rewind (benchmarkoor compute: 0.2)
    log_dir: Path
    request_timeout_s: int
    fail_fast: bool
    stop_container_on_exit: bool
    skip_gas_bump: bool   # drop input.gas_bump_file from the prelude (snapshot
                          # already contains the gas-bumped blocks)
    persist_prelude: bool # OverlayFS only: bake input.gas_bump_file ONCE into a
                          # persistent prelude overlay layer at sweep start, then
                          # keep it across per-test resets (reset-test). The
                          # gas-bump is dropped from the per-test prelude.


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
    log_level: str                 # asprof --log level (TRACE..NONE), default warn
    alloc: str | None              # --alloc <bytes> (allocation/memory profiling); needs jfr
    lock: str | None               # --lock <duration> (lock contention profiling); needs jfr


@dataclasses.dataclass
class Config:
    besu: BesuConfig
    input: InputConfig
    tests: TestsConfig
    run: RunConfig
    profile: ProfileConfig
    schelk: SchelkConfig


def _abs_path(p: str | os.PathLike) -> Path:
    return Path(p).expanduser().resolve()


def load_config(path: Path) -> Config:
    raw = yaml.safe_load(path.read_text())
    b, i, t, r = raw["besu"], raw["input"], raw["tests"], raw["run"]
    isolation = _validate_isolation(r.get("isolation", "restart"))
    post_sleep = r.get("post_test_sleep_s")
    if post_sleep is None:
        post_sleep = 0.2 if isolation == "rewind" else 0.0
    return Config(
        besu=BesuConfig(
            image=b["image"],
            container_name=b.get("container_name", "besu-bench"),
            data_snapshot_dir=_abs_path(b["data_snapshot_dir"]),
            bumped_snapshot_dir=(_abs_path(b["bumped_snapshot_dir"])
                                 if b.get("bumped_snapshot_dir") else None),
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
            gas_bump_file=str(i.get("gas_bump_file", "gas-bump.txt")),
        ),
        tests=TestsConfig(
            setup_subdir=t.get("setup_subdir", "setup"),
            testing_subdir=t.get("testing_subdir", "testing"),
            format=str(t.get("format", "auto")),
            fixtures_subdir=str(t.get("fixtures_subdir", ".")),
            filter=str(t.get("filter", "*")),
            order=str(t.get("order", "alphabetical")),
            match_chain_head=bool(t.get("match_chain_head", True)),
        ),
        run=RunConfig(
            reset_overlay=bool(r.get("reset_overlay", True)),
            reset_backend=_validate_backend(r.get("reset_backend", "overlayfs")),
            isolation=isolation,
            rewind_debug_sethead=bool(r.get("rewind_debug_sethead", False)),
            rewind_fcu_version=int(r.get("rewind_fcu_version", 4)),
            post_test_sleep_s=float(post_sleep),
            log_dir=_abs_path(r.get("log_dir", "./runs")),
            request_timeout_s=int(r.get("request_timeout_s", 120)),
            fail_fast=bool(r.get("fail_fast", False)),
            stop_container_on_exit=bool(r.get("stop_container_on_exit", True)),
            skip_gas_bump=bool(r.get("skip_gas_bump", False)),
            persist_prelude=bool(r.get("persist_prelude", False)),
        ),
        profile=_load_profile(raw.get("profile")),
        schelk=_load_schelk(raw.get("schelk")),
    )


RESET_BACKENDS = ("overlayfs", "schelk")
ISOLATION_MODES = ("restart", "rewind")


def _validate_backend(value: str) -> str:
    value = str(value).lower()
    if value not in RESET_BACKENDS:
        raise ValueError(
            f"run.reset_backend must be one of {RESET_BACKENDS}, got {value!r}"
        )
    return value


def _validate_isolation(value: str) -> str:
    value = str(value).lower()
    if value not in ISOLATION_MODES:
        raise ValueError(
            f"run.isolation must be one of {ISOLATION_MODES}, got {value!r}"
        )
    return value


def warn_if_rewind_fixture_mix(cfg: Config) -> None:
    """Compute isolation must not point at the stateful fixture tree."""
    if cfg.run.isolation != "rewind":
        return
    sub = cfg.tests.fixtures_subdir.replace("\\", "/").lower()
    if "compute" in sub:
        return
    print(
        "warn: run.isolation=rewind is the compute loop, but "
        f"tests.fixtures_subdir={cfg.tests.fixtures_subdir!r} does not contain "
        "'compute'. Extract the compute tarball into a separate directory "
        "(see config.compute.example.yaml). Do not mix with stateful fixtures.",
        file=sys.stderr,
    )


def _load_schelk(raw: dict | None) -> SchelkConfig:
    raw = raw or {}
    return SchelkConfig(
        bin=str(raw.get("bin", "schelk")),
        virgin=str(raw.get("virgin", "")),
        scratch=str(raw.get("scratch", "")),
        ramdisk=str(raw.get("ramdisk", "")),
        fstype=str(raw.get("fstype", "ext4")),
        granularity=(int(raw["granularity"]) if raw.get("granularity") else None),
        ramdisk_size_kb=(int(raw["ramdisk_size_kb"]) if raw.get("ramdisk_size_kb") else None),
        no_copy=bool(raw.get("no_copy", False)),
    )


def _load_profile(raw: dict | None) -> ProfileConfig:
    raw = raw or {}
    alloc = (str(raw["alloc"]) if raw.get("alloc") else None)
    lock = (str(raw["lock"]) if raw.get("lock") else None)
    output_format = str(raw.get("output_format", "html"))
    # Recording more than one event at once (the primary -e event plus alloc
    # and/or lock) only works with JFR output. Auto-upgrade and warn instead of
    # failing at asprof start with "Only JFR output supports multiple events".
    if (alloc or lock) and output_format != "jfr":
        print(
            "warn: profile.alloc/lock enable multi-event profiling, which "
            "requires JFR; forcing profile.output_format=jfr "
            f"(was {output_format!r})",
            file=sys.stderr,
        )
        output_format = "jfr"
    # Default to wall-clock profiling with per-thread split: works without
    # kernel.perf_event_paranoid tuning, and `-t` makes it easy to see which
    # vert.x worker thread did the heavy lifting vs. main / GC threads.
    return ProfileConfig(
        enabled=bool(raw.get("enabled", False)),
        host_dir=_abs_path(raw.get("host_dir", "~/async-profiler")),
        container_dir=str(raw.get("container_dir", "/opt/async-profiler")),
        event=str(raw.get("event", "wall")),
        interval=str(raw.get("interval", "1ms")),
        output_format=output_format,
        extra_args=list(raw.get("extra_args") or ["-t"]),
        # async-profiler 4.x logs `io_uring_wait_cqe failed: -4` (EINTR)
        # on every signal-interrupted poll of its sample queue. The retries
        # are silent at WARN level. Set NONE to silence everything, INFO
        # for the original chatty default.
        log_level=str(raw.get("log_level", "warn")),
        alloc=alloc,
        lock=lock,
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
        self.failure_total = 0

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
        self.failure_total += 1
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


def _dump_container_logs(name: str, log: SweepLog, tail: int = 200) -> str:
    if not _container_exists(name):
        log.event(f"container {name} no longer exists; cannot dump logs")
        return ""
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
    return out


def _rejected_options(container_logs: str) -> str | None:
    """Return Besu's own 'Unknown options: ...' line, if it printed one.

    Besu exits before its first log line when a flag is not in the image's
    CLI, so the startup failure otherwise looks like a generic timeout.
    """
    for line in container_logs.splitlines():
        line = line.strip()
        if line.startswith("Unknown options:"):
            return line
    return None


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
                 host_output_dir: Path, log: SweepLog):
        self.cfg = cfg
        self.container = container
        self.output_name = output_name
        self.host_output_dir = host_output_dir
        self.log = log
        self.started = False

    @property
    def host_output_path(self) -> Path:
        return self.host_output_dir / self.output_name

    @property
    def _asprof(self) -> str:
        return f"{self.cfg.container_dir}/bin/asprof"

    @property
    def _container_output_path(self) -> str:
        return f"{PROFILE_OUTPUT_CONTAINER_DIR}/{self.output_name}"

    def _exec(self, *args: str) -> subprocess.CompletedProcess:
        cmd = DOCKER + ["exec", self.container] + list(args)
        return _run(cmd, capture=True, check=False)

    def _event_args(self) -> list[str]:
        """Extra async-profiler event selectors from the convenience config
        keys. `--alloc`/`--lock` turn on allocation (memory) and lock
        contention profiling alongside the primary `-e` event; recording more
        than one event at once requires JFR output (see start())."""
        out: list[str] = []
        if self.cfg.alloc:
            out += ["--alloc", self.cfg.alloc]
        if self.cfg.lock:
            out += ["--lock", self.cfg.lock]
        return out

    def start(self) -> None:
        # Note on PID: the entrypoint we use (/opt/besu/bin/besu) is a shell
        # script that exec's java, so PID 1 is the JVM by the time we get
        # here. If you use a different entrypoint, adjust accordingly.
        args = [
            self._asprof, "start",
            "--log", self.cfg.log_level,
            "-e", self.cfg.event,
            "-i", self.cfg.interval,
            *self._event_args(),
            *self.cfg.extra_args,
        ]
        # async-profiler picks the output format from the -f extension. JFR is
        # the only format that can hold MULTIPLE concurrent events (cpu/wall +
        # alloc + lock), and it rejects >1 event unless it already knows at
        # START time that the recording is JFR. So for jfr we pass the target
        # file here; flat formats (html) are rendered from the in-memory buffer
        # at stop, so they keep passing -f there instead.
        if self.cfg.output_format == "jfr":
            args += ["-f", self._container_output_path]
        args.append("1")
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
            f"output -> {self.host_output_path})"
        )

    def stop(self) -> None:
        if not self.started:
            return
        args = [self._asprof, "stop", "--log", self.cfg.log_level]
        # For JFR the file was opened at start and is finalised by stop, so we
        # must NOT pass -f again. Flat formats are dumped here.
        if self.cfg.output_format != "jfr":
            args += ["-f", self._container_output_path]
        args.append("1")
        res = self._exec(*args)
        if res.returncode != 0:
            err = (res.stderr or res.stdout or "").strip()
            self.log.event(f"asprof stop FAILED ({res.returncode}): {err}")
        else:
            self.log.event(f"asprof stopped, wrote {self.host_output_path}")
        self.started = False


_MAX_SLUG_LEN = 60

# Pytest test basenames look like:
#   test_single_opcode.py__test_sload_bloated[10GB-fork_Amsterdam-
#       benchmark_test-cache_strategy_CacheStrategy.CACHE_PREVIOUS_BLOCK-
#       existing_slots_False-benchmark_120M].txt
# Most of the bracket tokens are identical across the whole suite. We
# drop them so the slug only carries what actually varies between tests.
_SLUG_NOISE = (
    "fork_Amsterdam",
    "fork_Osaka",
    "fork_Cancun",
    "fork_Prague",
    "benchmark_test",
    "initial_storage_True",
    "initial_storage_False",
    "initial_balance_True",
    "initial_balance_False",
    "empty_code_True",
    "empty_code_False",
    "existing_slots_True",
    "existing_slots_False",
    "cache_strategy_CacheStrategy.",
)


def _slugify(name: str) -> str:
    """Compact, filename-safe slug for a pytest test basename.

    Examples:
      test_single_opcode.py__test_sload_bloated[10GB-fork_Amsterdam-benchmark_test-
          cache_strategy_CacheStrategy.CACHE_PREVIOUS_BLOCK-existing_slots_False-
          benchmark_120M].txt
        -> sload_bloated-10GB-CACHE_PREVIOUS_BLOCK-benchmark_120M

      test_account_query.py__test_ext_account_query_warm[fork_Amsterdam-
          benchmark_test-initial_storage_True-initial_balance_True-empty_code_True-
          opcode_DELEGATECALL-benchmark_120M].txt
        -> ext_account_query_warm-opcode_DELEGATECALL-benchmark_120M
    """
    s = name
    # Drop trailing extension if any.
    if s.endswith(".txt"):
        s = s[:-4]
    # Strip `<file>.py__test_` prefix so we keep only the function name +
    # parametrisation. Falls back to `<file>.py__` if there is no test_ prefix.
    if ".py__test_" in s:
        s = s.split(".py__test_", 1)[1]
    elif ".py__" in s:
        s = s.split(".py__", 1)[1]
    # Drop the always-present boilerplate parameters (see _SLUG_NOISE).
    for tok in _SLUG_NOISE:
        s = s.replace(tok, "")
    # Replace anything not [A-Za-z0-9_] with a single dash; collapse repeats.
    out: list[str] = []
    last_dash = False
    for ch in s:
        if ch.isalnum() or ch == "_":
            out.append(ch)
            last_dash = False
        else:
            if not last_dash:
                out.append("-")
            last_dash = True
    slug = "".join(out).strip("-")
    if len(slug) > _MAX_SLUG_LEN:
        slug = slug[:_MAX_SLUG_LEN].rstrip("-")
    return slug or "test"


def _besu_log_filename(idx: int, name: str, failed: bool = False) -> str:
    """e.g. besu-0001-sload_bloated-10GB-CACHE_PREVIOUS_BLOCK-benchmark_120M.log"""
    suffix = "-FAIL" if failed else ""
    return f"besu-{idx:04d}-{_slugify(name)}{suffix}.log"


def _profile_output_filename(run_id: str, idx: int, name: str, phase: str, fmt: str) -> str:
    """e.g. 20260428-135916-0001-sload_bloated-10GB-CACHE_PREVIOUS_BLOCK-benchmark_120M-setup.html

    The `.html` (or `.jfr`) extension already identifies the file as a
    flame graph, so we drop the "profile-" word prefix to keep the name
    short. The run-id keeps the file self-identifying once it leaves
    the run dir.
    """
    ext_map = {"html": "html", "flamegraph": "html", "jfr": "jfr"}
    ext = ext_map.get(fmt, fmt)
    return f"{run_id}-{idx:04d}-{_slugify(name)}-{phase}.{ext}"


def start_besu(
    cfg: BesuConfig,
    log: SweepLog,
    *,
    profile: ProfileConfig | None = None,
    profile_output_dir: Path | None = None,
    data_mount: Path | None = None,
) -> None:
    """Boot Besu with its data-path bound to an overlay layer.

    Each benchmark run does the entire flow (gas-bump + funding + setup +
    testing) in this single container, so there is no need to stage writes
    across multiple overlay layers anymore. We normally mount
    <overlay_dir>/test/merged.

    `data_mount` overrides which directory is bound to the container's
    data-path. The persist-prelude bake phase passes
    <overlay_dir>/prelude/merged so the gas-bump's writes land in the
    persistent prelude layer instead of the per-test layer.

    If `profile` is enabled, also bind-mount async-profiler read-only and
    a writable output dir for flame graphs.
    """
    stop_container(cfg.container_name)
    merged = data_mount if data_mount is not None else (cfg.overlay_dir / "test" / "merged")
    # NOTE: no --rm here. We want the container to stick around if Besu
    # crashes, so wait_for_engine can dump `docker logs` on failure.
    # stop_container() above and at end-of-test cleans it up.
    docker_cmd: list[str] = list(DOCKER) + [
        "run", "-d",
        "--name", cfg.container_name,
        "--network", "host",
        # Disable Docker's default seccomp profile. async-profiler needs
        # perf_event_open / ptrace / mmap with PROT_EXEC for trampolines,
        # all of which are restricted (or outright blocked) by the default
        # profile on many distros. Even without profiling, Besu's JVM
        # benefits from unrestricted syscalls (jvm-perf events, vmstat
        # introspection). The container is already trusted (we built or
        # pulled the image ourselves and run on a private bench host),
        # so there is no security regression.
        "--security-opt", "seccomp=unconfined",
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
    res = _run(docker_cmd, capture=True, check=False)
    if res.returncode != 0:
        # Surface docker's own error message (image missing, pull failure,
        # bad flag, ...) instead of a bare CalledProcessError whose captured
        # stderr is never printed.
        err = (res.stderr or res.stdout or "").strip()
        log.event(f"docker run FAILED ({res.returncode}): {err}")
        raise RuntimeError(
            f"docker run exited {res.returncode} for image {cfg.image}: {err}"
        )


# ---------------------------------------------------------------------------
# Reset backend helpers (OverlayFS or schelk/dm-era)
# ---------------------------------------------------------------------------

OVERLAY_SCRIPT = Path(__file__).resolve().parent / "scripts" / "overlay.sh"
SCHELK_SCRIPT = Path(__file__).resolve().parent / "scripts" / "schelk.sh"


def reset_script(cfg: Config) -> Path:
    """The sudo entrypoint script for the configured reset backend."""
    return SCHELK_SCRIPT if cfg.run.reset_backend == "schelk" else OVERLAY_SCRIPT


def _overlay(action: str, cfg: BesuConfig, log: SweepLog, *extra_args: str) -> None:
    cmd = ["sudo", "-n", str(OVERLAY_SCRIPT), action]
    if action in ("init", "mount-all", "reset-all", "reset-test", "bake-prelude"):
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
        if action in ("mount-all", "reset-all", "reset-test", "bake-prelude"):
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


def overlay_bake_prelude(cfg: BesuConfig, log: SweepLog) -> None:
    """Wipe both overlay layers and mount ONLY the prelude layer.

    Leaves <overlay_dir>/prelude/merged mounted with the test layer
    intentionally unmounted, so Besu started on it writes into the
    persistent prelude upper dir. Used by the persist-prelude bake phase.
    """
    _overlay("bake-prelude", cfg, log)


def _schelk_flags(cfg: Config) -> list[str]:
    """Common schelk.sh flags. The mount point is fixed at <overlay_dir>/test/
    merged (set at init time) so it is not repeated here; mount/restore/recover
    read it from schelk's saved state."""
    s = cfg.schelk
    flags = ["--bin", s.bin]
    if s.virgin:
        flags += ["--virgin", s.virgin]
    if s.scratch:
        flags += ["--scratch", s.scratch]
    if s.ramdisk:
        flags += ["--ramdisk", s.ramdisk]
    if s.fstype:
        flags += ["--fstype", s.fstype]
    if s.granularity:
        flags += ["--granularity", str(s.granularity)]
    if s.ramdisk_size_kb:
        flags += ["--ramdisk-size-kb", str(s.ramdisk_size_kb)]
    return flags


def _schelk(action: str, cfg: Config, log: SweepLog) -> None:
    cmd = ["sudo", "-n", str(SCHELK_SCRIPT), action] + _schelk_flags(cfg)
    log.event(f"schelk {action}: " + " ".join(shlex.quote(a) for a in cmd))
    _run(cmd)


# --- Backend-neutral dispatch --------------------------------------------------
# run.py only needs two operations during a sweep: "reset to the pristine
# baseline (and mount it)" before each test, and (rarely) "just mount". Both
# call sites go through these dispatchers so the chosen backend is the only
# thing that changes between an OverlayFS run and a schelk run.

def reset_to_baseline(cfg: Config, log: SweepLog) -> None:
    """Roll on-disk state back to the pristine snapshot and (re)mount it."""
    if cfg.run.reset_backend == "schelk":
        _schelk("reset-all", cfg, log)
    else:
        overlay_reset_all(cfg.besu, log)


def mount_baseline(cfg: Config, log: SweepLog) -> None:
    """Mount the current baseline without resetting it."""
    if cfg.run.reset_backend == "schelk":
        _schelk("mount-all", cfg, log)
    else:
        overlay_mount_all(cfg.besu, log)


def schelk_promote(cfg: Config, log: SweepLog) -> None:
    """Make the current scratch state the new schelk (virgin) baseline.

    schelk-only. Copies the blocks the gas-bump wrote onto the virgin device,
    so every later `restore` rolls back to the gas-bumped state. Used by
    --prepare-baseline; it OVERWRITES virgin in place.
    """
    _schelk("promote", cfg, log)


def per_test_reset(cfg: Config, log: SweepLog) -> None:
    """Reset on-disk state before a single test.

    With persist-prelude (OverlayFS only) the gas-bump lives in a persistent
    prelude layer baked once at sweep start, so we only wipe the test layer
    (reset-test) and keep the prelude. Otherwise fall back to the full
    baseline reset.
    """
    if cfg.run.persist_prelude and cfg.run.reset_backend == "overlayfs":
        overlay_reset_test(cfg.besu, log)
    else:
        reset_to_baseline(cfg, log)


def drop_page_cache(log: SweepLog) -> None:
    """Flush dirty pages and drop the kernel page cache (+ dentries/inodes):

        sudo sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches

    Called before each test in compare mode so both versions start every
    test from an equally cold page cache instead of whatever the previous
    test left behind. Failures are logged but non-fatal: the sweep is still
    valid, just with a warmer cache (add `sync` and
    `tee /proc/sys/vm/drop_caches` to the sudoers allowlist to fix).
    """
    log.event("drop caches: sudo sync && echo 3 | sudo tee /proc/sys/vm/drop_caches")
    res = subprocess.run(["sudo", "-n", "sync"], capture_output=True, text=True)
    if res.returncode != 0:
        log.event(
            f"warn: `sudo -n sync` failed ({res.returncode}): "
            f"{(res.stderr or res.stdout or '').strip()}"
        )
        return
    res = subprocess.run(
        ["sudo", "-n", "tee", "/proc/sys/vm/drop_caches"],
        input="3", capture_output=True, text=True,
    )
    if res.returncode != 0:
        log.event(
            f"warn: `echo 3 | sudo -n tee /proc/sys/vm/drop_caches` failed "
            f"({res.returncode}): {(res.stderr or res.stdout or '').strip()}"
        )


# ---------------------------------------------------------------------------
# JWT + Engine API
# ---------------------------------------------------------------------------

_JWT_FALLBACK_SOURCES = (
    Path("/data/jwt.hex"),
    Path.home() / ".besu" / "jwt.hex",
)


def _genesis_host_path(cfg: Config) -> Path | None:
    """Host path that extra_mounts binds onto --genesis-file, if any."""
    genesis_in_container = None
    for arg in cfg.besu.extra_args:
        if arg.startswith("--genesis-file="):
            genesis_in_container = arg.split("=", 1)[1]
            break
    if not genesis_in_container:
        return None
    for spec in cfg.besu.extra_mounts:
        parts = spec.split(":")
        host = parts[0]
        container = parts[1] if len(parts) > 1 else ""
        if host and container == genesis_in_container:
            return Path(host)
    return None


def _genesis_search_roots(cfg: Config) -> list[Path]:
    roots: list[Path] = []
    for candidate in (
        Path("/data/genesis.json"),
        cfg.besu.data_snapshot_dir.parent / "genesis.json",
        Path(__file__).resolve().parent / "genesis.json",
        cfg.input.dir / "genesis.json",
        cfg.input.dir / "state-actor" / "geth" / "genesis.json",
        cfg.input.dir / "pre-runs" / "geth" / "genesis.json",
        cfg.input.dir / "pre-runs" / "geth" / "pre_run_bundle" / "genesis.json",
        cfg.input.dir / "eest-payloads" / "geth" / "genesis.json",
    ):
        if candidate not in roots:
            roots.append(candidate)
    return roots


def ensure_baseline_out_dir(out: Path, log: SweepLog) -> None:
    """Create the pre-bumped snapshot dir, with sudo when the parent is root-owned.

    /data is usually root-owned, so a plain mkdir raises PermissionError. The
    rsync that fills this dir already runs under sudo, so create it the same
    way. Called during preflight: a permission problem must surface before
    the replay, not after it.
    """
    if out.is_dir():
        return
    try:
        out.mkdir(parents=True, exist_ok=True)
        return
    except PermissionError:
        pass
    res = _run(["sudo", "-n", "mkdir", "-p", str(out)], check=False, capture=True)
    if res.returncode != 0 or not out.is_dir():
        raise PermissionError(
            f"cannot create the pre-bumped snapshot dir {out}: no write access "
            f"and `sudo -n mkdir -p {out}` failed ({(res.stderr or '').strip()}). "
            f"Create it once with `sudo mkdir -p {out}`, or add mkdir + rsync "
            "to the NOPASSWD sudoers entry."
        )
    log.event(f"created pre-bumped snapshot dir {out} with sudo")


def _find_genesis_file(cfg: Config) -> Path | None:
    for path in _genesis_search_roots(cfg):
        if path.is_file():
            return path
    for subdir in ("state-actor", "pre-runs"):
        root = cfg.input.dir / subdir
        if not root.is_dir():
            continue
        matches = sorted(root.rglob("genesis.json"))
        if matches:
            return matches[0]
    return None


def ensure_genesis_file(cfg: Config, log: SweepLog) -> None:
    """Copy a found genesis.json onto the extra_mounts host path if missing."""
    dest = _genesis_host_path(cfg)
    if dest is None or dest.is_file():
        return
    src = _find_genesis_file(cfg)
    if src is None:
        searched = ", ".join(str(p) for p in _genesis_search_roots(cfg))
        raise FileNotFoundError(
            f"genesis file missing at {dest} and no genesis.json was found. "
            f"Looked at: {searched}. Copy the jochemnet Besu genesis to "
            f"{dest} (or next to the snapshot as /data/genesis.json)."
        )
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    log.event(f"genesis missing at {dest}; copied from {src}")


def prepare_host_bind_mounts(cfg: Config, log: SweepLog) -> None:
    """Create JWT/genesis host files, then fail if any extra_mounts path is missing.

    A missing host path makes `docker run -v` silently create a directory.
    Besu then sees a directory where it expected a file and dies with empty logs.
    """
    ensure_jwt_secret(cfg.besu.jwt_secret_path, log)
    ensure_genesis_file(cfg, log)
    for spec in cfg.besu.extra_mounts:
        host = spec.split(":", 1)[0]
        if not host or not Path(host).exists():
            raise FileNotFoundError(
                f"besu.extra_mounts host path does not exist: {host!r} "
                f"(from spec {spec!r}). Create it before running, or fix the "
                "config: a missing host path makes docker silently create an "
                "empty directory and Besu will fail to start with no logs."
            )


def ensure_jwt_secret(path: Path, log: SweepLog) -> None:
    """Make sure `path` exists; if not, populate it.

    Order of preference:
      1. Copy from a well-known fallback (/data/jwt.hex, ~/.besu/jwt.hex).
      2. Generate a fresh 32-byte hex secret.

    Either way the file ends up with the JWT content the Besu container
    will read via its bind-mount, so the runner and Besu agree on the
    same secret.
    """
    if path.is_file():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    for src in _JWT_FALLBACK_SOURCES:
        if src.is_file() and src.resolve() != path.resolve():
            shutil.copy2(src, path)
            log.event(f"jwt secret missing at {path}; copied from {src}")
            return
    import secrets as _secrets
    path.write_text(_secrets.token_hex(32))
    try:
        path.chmod(0o644)
    except OSError:
        pass
    log.event(f"jwt secret missing at {path}; generated a fresh one")


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
            logs = _dump_container_logs(cfg.container_name, log)
            rejected = _rejected_options(logs)
            if rejected:
                raise RuntimeError(
                    f"Besu image {cfg.image} rejected flags from besu.extra_args: "
                    f"{rejected} Remove them from the config; this image does not "
                    "support them."
                )
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


def rewind_forkchoice_line(head_hash: str, version: int) -> str:
    """Engine FCU that sets canonical head back to `head_hash`."""
    return json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": f"engine_forkchoiceUpdatedV{version}",
        "params": [{
            "headBlockHash": head_hash,
            "safeBlockHash": ZERO_HASH,
            "finalizedBlockHash": ZERO_HASH,
        }, None],
    }, separators=(",", ":"))


def debug_set_head(cfg: BesuConfig, block_number: int) -> tuple[bool, str]:
    """Besu/Geth debug_setHead on the HTTP JSON-RPC port. Hex block number."""
    url = _rpc_http_url(cfg)
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "debug_setHead",
        "params": [hex(block_number)],
    }
    try:
        r = requests.post(url, json=payload, timeout=30)
        if r.status_code != 200:
            return False, f"HTTP {r.status_code}: {r.text[:200]}"
        body = r.json()
        if "error" in body:
            return False, json.dumps(body["error"])
        return True, ""
    except (requests.RequestException, ValueError) as e:
        return False, repr(e)


def rewind_canonical_head(
    cfg: Config,
    secret: bytes,
    session: requests.Session,
    log: SweepLog,
    block_number: int,
    block_hash: str,
) -> bool:
    """Move forkchoice (and optionally debug_setHead) back to the pre-run head.

    Compute isolation keeps the Besu process. The next fixture's parentHash is
    this head. A failed rewind is logged; the previous test's measurement
    still stands.
    """
    raw = rewind_forkchoice_line(block_hash, cfg.run.rewind_fcu_version)
    status, body, err = post_engine_line(cfg, secret, session, raw)
    if err is not None and body is None:
        log.event(f"rewind FCU: transport error {err}")
        return False
    if status != 200:
        log.event(f"rewind FCU: HTTP {status} {body}")
        return False
    ok, kind, detail = _classify(
        f"engine_forkchoiceUpdatedV{cfg.run.rewind_fcu_version}", body or {}
    )
    if not ok:
        log.event(f"rewind FCU: {_fail_detail(kind, detail)}")
        return False
    log.event(
        f"rewind FCU(VALID) head {block_hash[:10]}… "
        f"(engine_forkchoiceUpdatedV{cfg.run.rewind_fcu_version})"
    )

    if cfg.run.rewind_debug_sethead:
        d_ok, d_err = debug_set_head(cfg.besu, block_number)
        if not d_ok:
            log.event(f"rewind debug_setHead #{block_number:,} failed: {d_err}")
            return False
        log.event(f"rewind debug_setHead #{block_number:,}")

    head = query_chain_head(cfg.besu)
    if head is None:
        log.event("rewind: could not read chain head after FCU")
        return False
    n, h = head
    if h.lower() != block_hash.lower() or n != block_number:
        log.event(
            f"rewind: head is #{n:,} ({h}), expected #{block_number:,} "
            f"({block_hash})"
        )
        return False
    if cfg.run.post_test_sleep_s > 0:
        time.sleep(cfg.run.post_test_sleep_s)
        log.event(f"rewind: slept {cfg.run.post_test_sleep_s}s")
    return True


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


_RPC_METHOD_RE = re.compile(r'"method"\s*:\s*"((?:\\.|[^"\\])*)"')


def _rpc_method(raw: str) -> str:
    """Read the JSON-RPC method without parsing the whole payload object."""
    match = _RPC_METHOD_RE.search(raw, 0, min(len(raw), 4096))
    if match:
        return match.group(1)
    try:
        return json.loads(raw).get("method", "?")
    except json.JSONDecodeError:
        return ""


def _iter_rpc_lines(raw_lines) -> Iterable[tuple[int, str, str]]:
    """Yield (line_no, method, raw) for non-empty JSON-RPC lines."""
    for line_no, raw in enumerate(raw_lines, start=1):
        raw = raw.strip()
        if not raw:
            continue
        yield line_no, _rpc_method(raw), raw


def _scan_requests(raw_lines: list[str]) -> list[tuple[int, str, str]]:
    """Return decoded request metadata for non-empty JSON-RPC lines."""
    return list(_iter_rpc_lines(raw_lines))


def _last_newpayload_item_index(raw_lines) -> int:
    """Return the 0-based index among non-empty lines of the last newPayload."""
    last = -1
    i = -1
    for _line_no, method, _raw in _iter_rpc_lines(raw_lines):
        i += 1
        if method.startswith("engine_newPayload"):
            last = i
    return last


_MAX_LOGGED_FAILURES = 3


def _fail_detail(kind: str, detail: dict) -> str:
    """One-line reason for the event log, plus a hint for the usual causes."""
    result = detail.get("result") or {}
    status = (result.get("status")
              or (result.get("payloadStatus") or {}).get("status") or "")
    parts = [kind]
    if status:
        parts.append(f"status={status}")
    if status.upper() == "SYNCING":
        parts.append("(parent block unknown: this payload does not chain onto "
                     "the current head)")
    error = detail.get("error")
    if error:
        parts.append(f"error={error}")
    return " ".join(str(p) for p in parts)


def replay_requests(
    cfg: Config,
    secret: bytes,
    session: requests.Session,
    raw_lines,
    label: str,
    log: SweepLog,
    phase: str | None = None,
    profiler: ProfilerSession | None = None,
    require_all_valid: bool = False,
    last_newpayload_idx: int | None = None,
) -> bool:
    """Replay JSON-RPC requests in order.

    `phase` is an optional human label ("setup", "testing", ...) prepended to
    the event log.

    `raw_lines` may be a list or any line iterator. Do not materialize a
    whole pre-run file; stream it from disk.

    By default, failures only return False when fail-fast stops replay.
    `require_all_valid` returns False after any failed request.

    If `profiler` is given, async-profiler is started just before the file's
    LAST newPayload call and stopped after the LAST line of the file has been
    processed.
    """
    prefix = f"replay [{phase}] " if phase else "replay "
    log.event(f"{prefix}{label}")

    last_np_idx = -1
    if profiler is not None:
        if last_newpayload_idx is None:
            last_np_idx = _last_newpayload_item_index(raw_lines)
        else:
            last_np_idx = last_newpayload_idx

    profile_active = False
    all_valid = True
    sent = 0
    failed = 0

    def record_failure(line_no: int, kind: str, detail: dict) -> None:
        """Count the failure AND say so in the event log.

        Without this, a phase where every newPayload came back SYNCING only
        showed up in failures.jsonl and the run still looked successful.
        """
        nonlocal failed
        failed += 1
        log.record_fail(label, line_no, kind, detail)
        if failed <= _MAX_LOGGED_FAILURES:
            log.event(f"{prefix}{label}: line {line_no} "
                      f"{detail.get('method', '?')} FAILED: "
                      f"{_fail_detail(kind, detail)}")

    for i, (line_no, method, raw) in enumerate(_iter_rpc_lines(raw_lines)):
        if not method:
            all_valid = False
            record_failure(line_no, "bad_json", {})
            if cfg.run.fail_fast:
                return False
            continue

        if profiler is not None and i == last_np_idx and not profile_active:
            profiler.start()
            profile_active = True

        status, body, err = post_engine_line(cfg, secret, session, raw)
        sent += 1
        if sent == 1 or sent % 1000 == 0:
            log.event(f"{prefix}{label}: sent {sent} requests")
        if err is not None and body is None:
            all_valid = False
            record_failure(line_no, "http_error", {"method": method, "error": err})
            if cfg.run.fail_fast:
                if profile_active:
                    profiler.stop()
                return False
            continue
        if status != 200:
            all_valid = False
            record_failure(line_no, "http_status",
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
            all_valid = False
            record_failure(line_no, kind, {"method": method, **detail})
            if cfg.run.fail_fast:
                if profile_active:
                    profiler.stop()
                return False

    if profile_active:
        profiler.stop()
    if sent:
        log.event(f"{prefix}{label}: sent {sent} requests (done)")
    if failed:
        log.event(f"{prefix}{label}: {failed} of {sent} requests FAILED "
                  "(full detail in failures.jsonl)")
    return all_valid if require_all_valid else True


def replay_file(
    cfg: Config,
    secret: bytes,
    session: requests.Session,
    file_path: Path,
    log: SweepLog,
    phase: str | None = None,
    profiler: ProfilerSession | None = None,
    require_all_valid: bool = False,
) -> bool:
    """Replay one line-delimited JSON-RPC file without loading it all."""
    size = file_path.stat().st_size
    log.event(f"open {file_path} ({size:,} bytes) as a stream")
    last_np_idx = None
    if profiler is not None:
        with file_path.open(encoding="utf-8") as fh:
            last_np_idx = _last_newpayload_item_index(fh)
    with file_path.open(encoding="utf-8") as fh:
        return replay_requests(
            cfg, secret, session, fh, file_path.name,
            log, phase=phase, profiler=profiler,
            require_all_valid=require_all_valid,
            last_newpayload_idx=last_np_idx,
        )


# ---------------------------------------------------------------------------
# Test discovery
# ---------------------------------------------------------------------------

FIXTURE_FORMATS = ("auto", "legacy", "stateful_engine")
ZERO_HASH = "0x" + "00" * 32


def _test_format(cfg: Config) -> str:
    value = cfg.tests.format.lower()
    if value not in FIXTURE_FORMATS:
        raise ValueError(
            f"tests.format must be one of {FIXTURE_FORMATS}, got {value!r}"
        )
    if value != "auto":
        return value
    setup_dir = cfg.input.dir / cfg.tests.setup_subdir
    testing_dir = cfg.input.dir / cfg.tests.testing_subdir
    if setup_dir.is_dir() and testing_dir.is_dir():
        return "legacy"
    return "stateful_engine"


def _fixture_root(cfg: Config) -> Path:
    return cfg.input.dir / cfg.tests.fixtures_subdir


def _read_fixture_cases(path: Path) -> list[tuple[str, dict]]:
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid fixture JSON {path}: {exc}") from exc
    if not isinstance(data, dict):
        return []
    return [
        (name, fixture)
        for name, fixture in data.items()
        if isinstance(name, str)
        and isinstance(fixture, dict)
        and "setupEngineNewPayloads" in fixture
        and "engineNewPayloads" in fixture
    ]


@functools.lru_cache(maxsize=8)
def _stateful_fixture_index_for_root(root_text: str) -> dict[str, tuple[Path, str]]:
    root = Path(root_text)
    if not root.is_dir():
        raise FileNotFoundError(f"stateful fixture dir missing: {root}")

    found: list[tuple[str, Path]] = []
    for path in sorted(root.rglob("*.json")):
        for name, _fixture in _read_fixture_cases(path):
            found.append((name, path))

    counts: dict[str, int] = {}
    for name, _path in found:
        counts[name] = counts.get(name, 0) + 1

    index: dict[str, tuple[Path, str]] = {}
    for name, path in found:
        display = name
        if counts[name] > 1:
            display = f"{path.relative_to(root).as_posix()}::{name}"
        index[display] = (path, name)
    return index


def _stateful_fixture_index(cfg: Config) -> dict[str, tuple[Path, str]]:
    """Map display names to their JSON file and dictionary key."""
    return _stateful_fixture_index_for_root(str(_fixture_root(cfg).resolve()))


def _normalize_block_hash(value: str) -> str:
    raw = value.strip().lower()
    if not raw.startswith("0x"):
        raw = "0x" + raw
    return raw


def _payload_list_parent_hash(payloads) -> str | None:
    """Parent hash of the first Engine payload in a fixture list, if present."""
    if not isinstance(payloads, list) or not payloads:
        return None
    entry = payloads[0]
    if not isinstance(entry, dict):
        return None
    params = entry.get("params")
    if not isinstance(params, list) or not params or not isinstance(params[0], dict):
        return None
    parent = params[0].get("parentHash")
    if isinstance(parent, str) and parent.startswith("0x"):
        return _normalize_block_hash(parent)
    return None


def _fixture_expected_parent_hash(fixture: dict) -> str | None:
    """Hash the first setup (else testing) payload must have as its parent."""
    parent = _payload_list_parent_hash(fixture.get("setupEngineNewPayloads"))
    if parent:
        return parent
    return _payload_list_parent_hash(fixture.get("engineNewPayloads"))


def _stateful_test_parent_hash(cfg: Config, name: str) -> str | None:
    index = _stateful_fixture_index(cfg)
    path, case_name = index[name]
    cases = dict(_read_fixture_cases(path))
    return _fixture_expected_parent_hash(cases[case_name])


def _head_sidecar_path(snapshot_dir: Path) -> Path:
    """Sibling file that stores the snapshot chain head as a hex hash.

    Example: snapshot /data/besu-bumped -> /data/besu-bumped.head
    Keep it outside the datadir so Besu does not see an extra file.
    """
    return snapshot_dir.parent / f"{snapshot_dir.name}.head"


def write_head_sidecar(snapshot_dir: Path, block_hash: str, log: SweepLog) -> None:
    path = _head_sidecar_path(snapshot_dir)
    body = _normalize_block_hash(block_hash) + "\n"
    try:
        path.write_text(body)
        log.event(f"wrote chain-head sidecar {path}")
        return
    except OSError:
        pass
    res = subprocess.run(
        ["sudo", "-n", "tee", str(path)],
        input=body,
        text=True,
        capture_output=True,
        check=False,
    )
    if res.returncode == 0 and path.is_file():
        log.event(f"wrote chain-head sidecar {path} with sudo")
    else:
        log.event(
            f"could not write chain-head sidecar {path}: "
            f"{(res.stderr or res.stdout or '').strip()}"
        )


def read_head_sidecar(snapshot_dir: Path) -> str | None:
    path = _head_sidecar_path(snapshot_dir)
    if not path.is_file():
        return None
    try:
        raw = path.read_text().strip().splitlines()[0]
    except OSError:
        return None
    if raw.startswith("0x") and len(raw) >= 66:
        return _normalize_block_hash(raw)
    return None


def filter_stateful_tests_for_head(
    cfg: Config,
    names: list[str],
    head_hash: str,
    log: SweepLog,
) -> list[str]:
    """Keep fixtures whose first payload parent is `head_hash`."""
    head = _normalize_block_hash(head_hash)
    kept: list[str] = []
    skipped = 0
    examples: list[str] = []
    for name in names:
        parent = _stateful_test_parent_hash(cfg, name)
        if parent == head:
            kept.append(name)
            continue
        skipped += 1
        if len(examples) < 5:
            examples.append(f"{name} parent={parent or 'missing'}")
    if skipped:
        log.event(
            f"chain-head filter: kept {len(kept)}, skipped {skipped} "
            f"(first parent != {head})"
        )
        for line in examples:
            log.event(f"  skip {line}")
    else:
        log.event(f"chain-head filter: kept {len(kept)} fixture(s) on {head}")
    return kept


def probe_snapshot_head(
    cfg: Config, secret: bytes, log: SweepLog
) -> str | None:
    """Start Besu on the current baseline, optionally replay prelude, read head."""
    log.event("chain-head filter: no sidecar; starting Besu once to read head")
    try:
        reset_to_baseline(cfg, log)
        start_besu(cfg.besu, log)
        wait_for_engine(cfg.besu, secret, log)
        with requests.Session() as session:
            for fname in cfg.input.prelude:
                src = cfg.input.dir / fname
                if src.is_file():
                    replay_file(cfg, secret, session, src, log, phase="head-probe")
        head = query_chain_head(cfg.besu)
    finally:
        stop_container(cfg.besu.container_name)
    if head is None:
        log.event("chain-head filter: could not read chain head over RPC")
        return None
    _num, block_hash = head
    log.event(f"chain-head filter: probed head = #{_num:,} ({block_hash})")
    # Only persist when the head is already in the snapshot (empty prelude).
    if not cfg.input.prelude:
        write_head_sidecar(cfg.besu.data_snapshot_dir, block_hash, log)
    return _normalize_block_hash(block_hash)


def _match_chain_head_enabled(cfg: Config) -> bool:
    return _test_format(cfg) == "stateful_engine" and cfg.tests.match_chain_head


def apply_chain_head_filter(
    cfg: Config,
    tests: list[str],
    log: SweepLog,
    *,
    dry_run: bool,
    secret: bytes | None = None,
) -> list[str]:
    """Drop genesis-style fixtures that do not chain onto the pre-run head."""
    if not _match_chain_head_enabled(cfg):
        return tests
    head = read_head_sidecar(cfg.besu.data_snapshot_dir)
    if head is not None:
        log.event(
            f"chain-head filter: using sidecar {_head_sidecar_path(cfg.besu.data_snapshot_dir)}"
        )
    elif dry_run:
        log.event(
            "chain-head filter: no <snapshot>.head sidecar; dry-run keeps all "
            "matches. Run without --dry-run once, or re-run --prepare-baseline."
        )
        return tests
    elif secret is not None:
        head = probe_snapshot_head(cfg, secret, log)
    if head is None:
        log.event("chain-head filter: no head hash; not filtering")
        return tests
    return filter_stateful_tests_for_head(cfg, tests, head, log)


def _rpc_version(value, field: str, source: str) -> int:
    try:
        version = int(str(value), 0)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{source}: invalid {field}: {value!r}") from exc
    if version < 1:
        raise ValueError(f"{source}: invalid {field}: {value!r}")
    return version


def _fixture_payload_requests(payloads, source: str) -> list[str]:
    if not isinstance(payloads, list):
        raise ValueError(f"{source}: payload field must be a list")

    lines: list[str] = []
    for idx, entry in enumerate(payloads, start=1):
        item_source = f"{source} payload {idx}"
        if not isinstance(entry, dict):
            raise ValueError(f"{item_source}: payload must be an object")
        params = entry.get("params")
        if not isinstance(params, list) or not params or not isinstance(params[0], dict):
            raise ValueError(f"{item_source}: params[0] execution payload is missing")

        np_version = _rpc_version(
            entry.get("newPayloadVersion"), "newPayloadVersion", item_source
        )
        fcu_value = entry.get(
            "forkchoiceUpdatedVersion", entry.get("forkchoiceVersion")
        )
        if fcu_value is None:
            fcu_value = 3 if np_version >= 3 else np_version
        fcu_version = _rpc_version(
            fcu_value, "forkchoiceUpdatedVersion", item_source
        )

        block_hash = params[0].get("blockHash")
        if not isinstance(block_hash, str) or not block_hash.startswith("0x"):
            raise ValueError(f"{item_source}: params[0].blockHash is missing")

        new_payload = {
            "jsonrpc": "2.0",
            "id": idx,
            "method": f"engine_newPayloadV{np_version}",
            "params": params,
        }
        forkchoice = {
            "jsonrpc": "2.0",
            "id": idx,
            "method": f"engine_forkchoiceUpdatedV{fcu_version}",
            "params": [{
                "headBlockHash": block_hash,
                "safeBlockHash": ZERO_HASH,
                "finalizedBlockHash": ZERO_HASH,
            }, None],
        }
        lines.extend((
            json.dumps(new_payload, separators=(",", ":")),
            json.dumps(forkchoice, separators=(",", ":")),
        ))
    return lines


def _stateful_test_requests(
    cfg: Config, name: str
) -> tuple[list[str], list[str], str]:
    index = _stateful_fixture_index(cfg)
    try:
        path, case_name = index[name]
    except KeyError as exc:
        raise KeyError(f"unknown stateful fixture test: {name}") from exc
    cases = dict(_read_fixture_cases(path))
    fixture = cases[case_name]
    source = f"{path.name}::{case_name}"
    setup = _fixture_payload_requests(
        fixture["setupEngineNewPayloads"], f"{source} setupEngineNewPayloads"
    )
    testing = _fixture_payload_requests(
        fixture["engineNewPayloads"], f"{source} engineNewPayloads"
    )
    if not testing:
        raise ValueError(f"{source}: engineNewPayloads is empty")
    return setup, testing, source


def discover_tests(cfg: Config, filter_override: str | None, limit: int | None,
                   explicit: list[str] | None = None,
                   defer_limit: bool = False) -> list[str]:
    """Return ordered list of basenames present in BOTH setup/ and testing/ that match the filter.

    When `explicit` is given (e.g. an arbitrary multi-selection from the web
    UI via --tests-from), it overrides the glob filter and `tests.order`: only
    the named tests are kept, in the order given, intersected with the valid
    setup/testing pairs. `limit` still applies last.
    """
    pattern = filter_override or cfg.tests.filter
    if _test_format(cfg) == "stateful_engine":
        available = _stateful_fixture_index(cfg)
        names = list(available)
        if explicit is not None:
            seen: set[str] = set()
            selected = []
            missing = []
            for name in explicit:
                if name in seen:
                    continue
                seen.add(name)
                if name in available:
                    selected.append(name)
                else:
                    missing.append(name)
            if missing:
                preview = ", ".join(missing[:5]) + (" ..." if len(missing) > 5 else "")
                print(
                    f"warn: {len(missing)} requested fixture test(s) were not found: "
                    f"{preview}", file=sys.stderr,
                )
        else:
            selected = [name for name in names if fnmatch.fnmatch(name, pattern)]
            if cfg.tests.order == "alphabetical":
                selected.sort()
            elif cfg.tests.order == "as_listed":
                pass
            elif cfg.tests.order == "shuffled":
                random.shuffle(selected)
            else:
                raise ValueError(f"unknown tests.order: {cfg.tests.order}")
        if not defer_limit and limit is not None and limit >= 0:
            selected = selected[:limit]
        return selected

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

    if explicit is not None:
        # Explicit selection: preserve the requested order, keep only valid
        # pairs, drop unknowns with a warning. Glob filter / tests.order are
        # ignored on purpose so the web UI gets exactly what it asked for.
        seen: set[str] = set()
        selected: list[str] = []
        missing: list[str] = []
        for n in explicit:
            if n in seen:
                continue
            seen.add(n)
            if n in paired:
                selected.append(n)
            else:
                missing.append(n)
        if missing:
            preview = ", ".join(missing[:5]) + (" ..." if len(missing) > 5 else "")
            print(f"warn: {len(missing)} requested test(s) are not valid setup/testing "
                  f"pairs and were skipped: {preview}", file=sys.stderr)
        if not defer_limit and limit is not None and limit >= 0:
            selected = selected[:limit]
        return selected

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
    if not defer_limit and limit is not None and limit >= 0:
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
    if _test_format(cfg) == "stateful_engine":
        setup_lines, testing_lines, source = _stateful_test_requests(cfg, name)
        setup_ok = replay_requests(
            cfg, secret, session, setup_lines, source, log,
            phase="setup", profiler=setup_profiler,
        )
    else:
        setup_ok = replay_file(
            cfg, secret, session, setup_dir / name, log,
            phase="setup", profiler=setup_profiler,
        )
    if not setup_ok:
        log.event(f"fail-fast tripped during setup of {name}")
        return False

    if _test_format(cfg) == "stateful_engine":
        testing_ok = replay_requests(
            cfg, secret, session, testing_lines, source, log,
            phase="testing", profiler=testing_profiler,
        )
    else:
        testing_ok = replay_file(
            cfg, secret, session, testing_dir / name, log,
            phase="testing", profiler=testing_profiler,
        )
    if not testing_ok:
        log.event(f"fail-fast tripped during testing of {name}")
        return False
    return True


def _make_test_profilers(
    cfg: Config, log: SweepLog, idx: int, name: str,
) -> tuple[ProfilerSession | None, ProfilerSession | None]:
    if not cfg.profile.enabled:
        return None, None
    run_id = log.root.name
    return (
        ProfilerSession(
            cfg.profile,
            cfg.besu.container_name,
            _profile_output_filename(
                run_id, idx, name, "setup", cfg.profile.output_format
            ),
            log.root,
            log,
        ),
        ProfilerSession(
            cfg.profile,
            cfg.besu.container_name,
            _profile_output_filename(
                run_id, idx, name, "testing", cfg.profile.output_format
            ),
            log.root,
            log,
        ),
    )


def _replay_prelude(
    cfg: Config,
    secret: bytes,
    session: requests.Session,
    log: SweepLog,
    phase: str = "prelude",
) -> bool:
    for fname in cfg.input.prelude:
        if not replay_file(
            cfg, secret, session,
            cfg.input.dir / fname, log, phase=phase,
        ):
            log.event(f"fail-fast tripped during prelude {fname}")
            return False
    return True


def _finish_test_result(
    cfg: Config,
    log: SweepLog,
    idx: int,
    n_tests: int,
    name: str,
    test_ok: bool,
    failures_before: int,
) -> bool:
    new_failures = log.failure_total - failures_before
    if new_failures:
        log.event(
            f"[{idx}/{n_tests}] {name}: {new_failures} "
            "failed request(s); this test produced NO valid "
            "measurement"
        )
        test_ok = False
    save_container_logs(
        cfg.besu.container_name,
        log.root / _besu_log_filename(idx, name, failed=not test_ok),
        log,
    )
    return test_ok


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


def bake_prelude_layer(
    cfg: Config,
    secret: bytes,
    session: requests.Session,
    log: SweepLog,
    label: str = "",
) -> bool:
    """Bake input.gas_bump_file ONCE into the persistent prelude overlay layer.

    OverlayFS only. Mounts ONLY the prelude layer (test layer NOT mounted, so
    Besu's writes land in prelude/upper), starts Besu on prelude/merged,
    replays the gas-bump, then stops Besu (flushing RocksDB) while LEAVING the
    prelude mounted so per-test `reset-test` can stack the test layer on top.

    Returns True on success. The 5000-block gas-bump then survives every
    per-test reset instead of being replayed before each test.
    """
    pfx = f"[{label}] " if label else ""
    gas_bump = cfg.input.gas_bump_file
    src = cfg.input.dir / gas_bump
    prelude_merged = cfg.besu.overlay_dir / "prelude" / "merged"

    if not src.is_file():
        log.event(f"{pfx}persist-prelude: gas-bump file not found: {src}")
        return False

    log.event(f"{pfx}persist-prelude: baking {gas_bump} into the prelude layer")
    overlay_bake_prelude(cfg.besu, log)

    ok = False
    try:
        start_besu(cfg.besu, log, data_mount=prelude_merged)
        wait_for_engine(cfg.besu, secret, log)
        log_chain_head(cfg.besu, log, f"{pfx}prelude head BEFORE gas-bump")
        ok = replay_file(cfg, secret, session, src, log, phase="prelude-bake")
        if ok:
            log_chain_head(cfg.besu, log, f"{pfx}prelude head AFTER gas-bump")
        bake_log = (
            log.root
            / f"besu-prelude-bake{('-' + _safe_label(label)) if label else ''}"
              f"{'' if ok else '-FAIL'}.log"
        )
        save_container_logs(cfg.besu.container_name, bake_log, log)
    finally:
        # Stop Besu so prelude/upper is flushed and consistent. The overlay
        # mount is host-side and survives the container teardown.
        stop_container(cfg.besu.container_name)
    return ok


def _apply_limit(tests: list[str], limit: int | None) -> list[str]:
    if limit is not None and limit >= 0:
        return tests[:limit]
    return tests


def run_sweep(cfg: Config, filter_override: str | None, limit: int | None,
              pick: bool, dry_run: bool, select: list[str] | None = None) -> int:
    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    log_root = cfg.run.log_dir / timestamp
    log = SweepLog(log_root)
    log.event(f"sweep start, log dir = {log_root}")

    defer_limit = _match_chain_head_enabled(cfg)
    tests = discover_tests(
        cfg, filter_override, limit, explicit=select, defer_limit=defer_limit,
    )
    log.event(
        f"matched {len(tests)} tests "
        f"(filter={filter_override or cfg.tests.filter}, order={cfg.tests.order}, "
        f"limit={limit})"
    )

    if dry_run:
        tests = apply_chain_head_filter(cfg, tests, log, dry_run=True)
        if pick:
            # Preview the filtered list; do not prompt.
            pass
        tests = _apply_limit(tests, limit)
        (log_root / "selected_tests.txt").write_text("\n".join(tests) + "\n")
        log.event(f"dry-run: {len(tests)} test(s) after chain-head filter")
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
    prepare_host_bind_mounts(cfg, log)

    # Preflight: passwordless sudo for both helpers we depend on.
    _reset_script = reset_script(cfg)
    for probe, hint in (
        (DOCKER + ["version", "--format", "{{.Server.Version}}"],
         "sudo -n docker version"),
        (["sudo", "-n", str(_reset_script), "--help"],
         f"sudo -n {_reset_script} --help"),
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
    tests = apply_chain_head_filter(
        cfg, tests, log, dry_run=False, secret=secret,
    )
    if pick:
        tests = _interactive_pick(tests, log)
    tests = _apply_limit(tests, limit)
    (log_root / "selected_tests.txt").write_text("\n".join(tests) + "\n")
    log.event(f"running {len(tests)} test(s)")

    setup_dir = cfg.input.dir / cfg.tests.setup_subdir
    testing_dir = cfg.input.dir / cfg.tests.testing_subdir

    started_container = False
    sweep_ok = True

    try:
        with requests.Session() as session:
            # persist-prelude: bake the gas-bump ONCE into the persistent
            # prelude overlay layer, then keep it across per-test resets.
            if cfg.run.persist_prelude:
                if not bake_prelude_layer(cfg, secret, session, log):
                    log.event("persist-prelude: gas-bump bake FAILED; aborting before tests")
                    sweep_ok = False
                    tests = []

            rewind = cfg.run.isolation == "rewind"
            rewind_target: tuple[int, str] | None = None

            if rewind:
                log.event(
                    "isolation=rewind: one Besu process for the sweep; "
                    "FCU back to the pre-run head after each test"
                )
                per_test_reset(cfg, log)
                start_besu(
                    cfg.besu, log,
                    profile=cfg.profile if cfg.profile.enabled else None,
                    profile_output_dir=log.root if cfg.profile.enabled else None,
                )
                started_container = True
                wait_for_engine(cfg.besu, secret, log)
                log_chain_head(cfg.besu, log, "head BEFORE prelude")
                if not _replay_prelude(cfg, secret, session, log):
                    sweep_ok = False
                    tests = []
                else:
                    log_chain_head(cfg.besu, log, "head AFTER prelude")
                    rewind_target = query_chain_head(cfg.besu)
                    if rewind_target is None:
                        log.event("rewind: could not capture pre-run head; aborting")
                        sweep_ok = False
                        tests = []
                    else:
                        n, h = rewind_target
                        log.event(f"rewind target: #{n:,} ({h})")

            for idx, name in enumerate(tests, start=1):
                log.event(f"[{idx}/{len(tests)}] {name}")

                if not rewind:
                    per_test_reset(cfg, log)
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

                setup_profiler, testing_profiler = _make_test_profilers(
                    cfg, log, idx, name
                )

                test_ok = True
                failures_before = log.failure_total

                if not rewind:
                    if not _replay_prelude(cfg, secret, session, log):
                        test_ok = False
                    else:
                        log_chain_head(
                            cfg.besu, log,
                            f"[{idx}/{len(tests)}] head AFTER prelude"
                        )

                if test_ok:
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

                test_ok = _finish_test_result(
                    cfg, log, idx, len(tests), name, test_ok, failures_before
                )

                if rewind:
                    if rewind_target is not None:
                        n, h = rewind_target
                        if not rewind_canonical_head(
                            cfg, secret, session, log, n, h
                        ):
                            log.event(
                                f"[{idx}/{len(tests)}] rewind FAILED; "
                                "later tests may SYNCING"
                            )
                            sweep_ok = False
                            if cfg.run.fail_fast:
                                break
                else:
                    stop_container(cfg.besu.container_name)
                    started_container = False

                if not test_ok:
                    sweep_ok = False
                    if cfg.run.fail_fast:
                        break

        log.event(f"sweep end: ok={sweep_ok}")
    finally:
        log.flush_summary({
            "config": {
                "image": cfg.besu.image,
                "snapshot": str(cfg.besu.data_snapshot_dir),
                "isolation": cfg.run.isolation,
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
# Prepare a pre-bumped snapshot (OverlayFS): bake the gas-bump blocks into a
# new snapshot directory so later sweeps can use --skip-gas-bump and avoid
# replaying the 5000-block gas-bump before every test.
# ---------------------------------------------------------------------------

def run_prepare_baseline(cfg: Config, baseline_out: Path | None) -> int:
    """Bake the gas-bump blocks into the pristine baseline, once.

    Dispatches on the reset backend. Either way it replays ONLY the gas-bump
    file (input.gas_bump_file), runs no tests, and leaves a baseline that
    --skip-gas-bump can use so the 5000-block gas-bump is no longer replayed
    before every test.
    """
    gas_bump = cfg.input.gas_bump_file
    src = cfg.input.dir / gas_bump
    if not src.is_file():
        print(f"error: gas-bump file not found: {src} "
              "(set input.gas_bump_file to the right name).", file=sys.stderr)
        nearby = sorted(src.parent.glob("*.request")) if src.parent.is_dir() else []
        if not nearby and src.parent.parent.is_dir():
            nearby = sorted(src.parent.parent.glob("*.request"))
        if nearby:
            print("  nearby *.request files:", file=sys.stderr)
            for path in nearby:
                try:
                    rel = path.relative_to(cfg.input.dir)
                except ValueError:
                    rel = path
                print(f"    {rel}", file=sys.stderr)
        return 2

    if cfg.run.reset_backend == "schelk":
        return _prepare_baseline_schelk(cfg, src, gas_bump, baseline_out)

    return _prepare_baseline_overlayfs(cfg, src, gas_bump, baseline_out)


def _prepare_baseline_overlayfs(
    cfg: Config, src: Path, gas_bump: str, baseline_out: Path | None
) -> int:
    """Build a new snapshot directory that already contains the gas-bump blocks.

    Flow: reset+mount the overlay, start Besu, replay ONLY the gas-bump file,
    stop Besu so RocksDB is flushed, then rsync the flattened
    <overlay_dir>/test/merged view into `baseline_out`. The original snapshot
    dir is never modified, so you switch baselines just by running with
    --skip-gas-bump (funding stays in the prelude and chains onto the bump tip).
    """
    out = (baseline_out or _bumped_snapshot_dir(cfg)).expanduser()
    # Never clobber the source baseline or the live overlay scratch root.
    for forbidden, why in (
        (cfg.besu.data_snapshot_dir, "the existing snapshot dir"),
        (cfg.besu.overlay_dir, "the overlay scratch root"),
    ):
        if str(out) == str(forbidden) or _same_path(out, forbidden):
            print(f"error: --baseline-out {out} would overwrite {why}; "
                  "choose a different path.", file=sys.stderr)
            return 2

    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    log = SweepLog(cfg.run.log_dir / f"{timestamp}-prepare")
    log.event(f"prepare-baseline start: replay {gas_bump} -> new snapshot {out}")

    # Focused preflight (mirrors run_sweep, minus the per-test machinery).
    prepare_host_bind_mounts(cfg, log)
    _reset_script = reset_script(cfg)
    for probe, hint in (
        (DOCKER + ["version", "--format", "{{.Server.Version}}"], "sudo -n docker version"),
        (["sudo", "-n", str(_reset_script), "--help"], f"sudo -n {_reset_script} --help"),
    ):
        try:
            _run(probe, capture=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"`{hint}` failed with exit {e.returncode}: {(e.stderr or '').strip()}"
            ) from None
    # The replay takes tens of minutes; check the destination first.
    ensure_baseline_out_dir(out, log)

    secret = load_jwt_secret(cfg.besu.jwt_secret_path)
    merged = cfg.besu.overlay_dir / "test" / "merged"
    started = False
    ok = False
    try:
        with requests.Session() as session:
            reset_to_baseline(cfg, log)
            start_besu(cfg.besu, log)
            started = True
            wait_for_engine(cfg.besu, secret, log)
            log_chain_head(cfg.besu, log, "head BEFORE gas-bump")
            ok = replay_file(
                cfg, secret, session, src, log, phase="prepare",
                require_all_valid=True,
            )
            after_head = None
            if ok:
                after_head = query_chain_head(cfg.besu)
                log_chain_head(cfg.besu, log, "head AFTER gas-bump")
            save_container_logs(
                cfg.besu.container_name,
                log.root / f"besu-prepare{'' if ok else '-FAIL'}.log", log,
            )
            # Stop Besu so RocksDB flushes and releases its files before we copy
            # the on-disk state (same consistency point the sweep relies on).
            stop_container(cfg.besu.container_name)
            started = False
            if not ok:
                log.event("prepare-baseline: gas-bump replay failed; snapshot NOT written")
            else:
                ensure_baseline_out_dir(out, log)
                # Trailing slash copies the CONTENTS of merged to the root of
                # out, so out/database, out/caches, ... mirror the snapshot
                # layout. sudo: the datadir is root-owned. --delete keeps a
                # re-run idempotent.
                #
                # --link-dest=<original snapshot>: the pre-bumped baseline is
                # read-only (only ever an overlay lowerdir), so files the
                # gas-bump did NOT change are hardlinked from the original
                # snapshot instead of copied. That turns a full ~snapshot-size
                # copy into hardlinks + only the changed RocksDB files (seconds
                # + a few GB instead of the whole DB). Cross-filesystem dests
                # just fall back to a normal copy, which is still correct.
                rsync = ["sudo", "rsync", "-aHAX", "--numeric-ids", "--delete",
                         "--info=progress2",
                         f"--link-dest={cfg.besu.data_snapshot_dir}",
                         f"{merged}/", f"{out}/"]
                log.event("prepare-baseline: "
                          + " ".join(shlex.quote(a) for a in rsync))
                _run(rsync)
                log.event(f"prepare-baseline: wrote pre-bumped snapshot to {out}")
                if after_head is not None:
                    write_head_sidecar(out, after_head[1], log)
        log.event(f"prepare-baseline end: ok={ok}")
    finally:
        if started and cfg.run.stop_container_on_exit:
            log.event(f"stopping container {cfg.besu.container_name}")
            stop_container(cfg.besu.container_name)
        log.flush_summary({
            "prepare_baseline": True,
            "ok": ok,
            "gas_bump_file": gas_bump,
            "snapshot_out": str(out),
        })
        log.close()

    if not ok:
        print("prepare-baseline failed: see the events log and besu-prepare-FAIL.log "
              f"in {log.root}", file=sys.stderr)
        return 1
    print()
    print(f"Pre-bumped snapshot ready: {out}")
    print("Use it for future sweeps by setting, in config.yaml:")
    print(f"  besu:\n    data_snapshot_dir: {out}")
    print("  run:\n    skip_gas_bump: true      # or pass --skip-gas-bump on the CLI")
    return 0


def _prepare_baseline_schelk(
    cfg: Config, src: Path, gas_bump: str, baseline_out: Path | None
) -> int:
    """Bake the gas-bump into the schelk baseline (the virgin device), in place.

    schelk has a single baseline (virgin), so there is no separate "bumped"
    directory like OverlayFS: instead we replay the gas-bump onto scratch and
    `schelk promote` it, copying the written blocks onto virgin. Every later
    `restore` then rolls back to the gas-bumped state, so --skip-gas-bump can
    drop the gas-bump from the per-test prelude. This OVERWRITES virgin: the
    pristine pre-bump baseline is gone until you reload the snapshot and re-init.
    """
    if baseline_out is not None:
        print("warn: --baseline-out is ignored for the schelk backend; the "
              "gas-bump is promoted into the virgin device in place "
              f"({cfg.schelk.virgin}).", file=sys.stderr)

    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    log = SweepLog(cfg.run.log_dir / f"{timestamp}-prepare")
    log.event(f"prepare-baseline (schelk) start: replay {gas_bump}, then promote "
              f"scratch -> virgin {cfg.schelk.virgin}")

    # Focused preflight (mirrors run_sweep, minus the per-test machinery).
    prepare_host_bind_mounts(cfg, log)
    _reset_script = reset_script(cfg)
    for probe, hint in (
        (DOCKER + ["version", "--format", "{{.Server.Version}}"], "sudo -n docker version"),
        (["sudo", "-n", str(_reset_script), "--help"], f"sudo -n {_reset_script} --help"),
    ):
        try:
            _run(probe, capture=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"`{hint}` failed with exit {e.returncode}: {(e.stderr or '').strip()}"
            ) from None

    secret = load_jwt_secret(cfg.besu.jwt_secret_path)
    started = False
    ok = False
    try:
        with requests.Session() as session:
            # Start from the pristine virgin baseline on scratch.
            reset_to_baseline(cfg, log)
            start_besu(cfg.besu, log)
            started = True
            wait_for_engine(cfg.besu, secret, log)
            log_chain_head(cfg.besu, log, "head BEFORE gas-bump")
            ok = replay_file(
                cfg, secret, session, src, log, phase="prepare",
                require_all_valid=True,
            )
            if ok:
                after_head = query_chain_head(cfg.besu)
                log_chain_head(cfg.besu, log, "head AFTER gas-bump")
            save_container_logs(
                cfg.besu.container_name,
                log.root / f"besu-prepare{'' if ok else '-FAIL'}.log", log,
            )
            # Stop Besu so RocksDB flushes onto scratch before we promote the
            # written blocks onto virgin.
            stop_container(cfg.besu.container_name)
            started = False
            if not ok:
                log.event("prepare-baseline: gas-bump replay failed; virgin NOT promoted")
            else:
                schelk_promote(cfg, log)
                log.event("prepare-baseline: promoted gas-bumped scratch onto the "
                          f"virgin baseline {cfg.schelk.virgin}")
                if after_head is not None:
                    write_head_sidecar(cfg.besu.data_snapshot_dir, after_head[1], log)
        log.event(f"prepare-baseline end: ok={ok}")
    finally:
        if started and cfg.run.stop_container_on_exit:
            log.event(f"stopping container {cfg.besu.container_name}")
            stop_container(cfg.besu.container_name)
        log.flush_summary({
            "prepare_baseline": True,
            "backend": "schelk",
            "ok": ok,
            "gas_bump_file": gas_bump,
            "virgin": cfg.schelk.virgin,
        })
        log.close()

    if not ok:
        print("prepare-baseline failed: see the events log and besu-prepare-FAIL.log "
              f"in {log.root}", file=sys.stderr)
        return 1
    print()
    print(f"Gas-bump baked into the schelk virgin device: {cfg.schelk.virgin}")
    print("Run future sweeps/compares with --skip-gas-bump (or run.skip_gas_bump:")
    print("true) so the gas-bump is no longer replayed before every test.")
    return 0


def _same_path(a: Path, b: Path) -> bool:
    """True if two paths resolve to the same location (existing or not)."""
    try:
        return a.resolve() == b.resolve()
    except OSError:
        return os.path.normpath(str(a)) == os.path.normpath(str(b))


def _bumped_snapshot_dir(cfg: Config) -> Path:
    """Where the pre-bumped snapshot lives: besu.bumped_snapshot_dir if set,
    else <data_snapshot_dir>-bumped. Both --prepare-baseline (output) and
    --skip-gas-bump (input) resolve it the same way, so one flag switches
    baselines with no config edit."""
    if cfg.besu.bumped_snapshot_dir is not None:
        return cfg.besu.bumped_snapshot_dir
    return Path(str(cfg.besu.data_snapshot_dir) + "-bumped")


# ===========================================================================
# Compare mode: run the whole suite twice on two Besu images, then diff the
# per-test testing-block times and emit an HTML report.
#
# This whole section is additive: it reuses the existing helpers (start_besu,
# reset_to_baseline, wait_for_engine, post_engine_line, _classify, request scan,
# discover_tests, SweepLog, ...) without modifying any of them, so the default
# single-image sweep keeps behaving exactly as before.
# ===========================================================================

def _image_label(image: str) -> str:
    """Human-friendly label for a docker image reference.

    `repo/besu:bal-devnet-2` -> `bal-devnet-2`; falls back to the whole
    reference when there is no tag.
    """
    ref = image.strip()
    # Drop a digest if present (`image@sha256:...`).
    ref = ref.split("@", 1)[0]
    if ":" in ref.rsplit("/", 1)[-1]:
        return ref.rsplit(":", 1)[1]
    return ref


def _safe_label(label: str) -> str:
    """Filename-safe form of a version label (for per-version artefacts)."""
    out = []
    for ch in label:
        out.append(ch if (ch.isalnum() or ch in "._-") else "-")
    return "".join(out).strip("-") or "ver"


# Besu logs one line per imported block, e.g.:
#   ... | AbstractEngineNewPayload | Imported #24,407,731  (fbdc2..26565)|
#       18 tx (100.0% parallel)| 0 ws| 0 blobs| 7 wei bfee|
#       299,371,362 (  0.0%) gas used| 1.090s exec| 274.70 Mgas/s| 0 peers
# We parse Besu's own gas-used / exec-time / Mgas/s straight from that line
# rather than recomputing them, so the numbers match what Besu reports.
_IMPORTED_BLOCK_RE = re.compile(r"Imported\s+#([\d,]+)")
_IMPORTED_GAS_RE = re.compile(r"([\d,]+)\s*\(\s*[\d.]+%\)\s*gas used")
# Besu reports the block exec time either in seconds ("1.090s exec") or, on
# newer builds, in milliseconds ("75.0ms exec"). Capture the unit so we can
# normalise both to seconds; matching only "s" silently dropped exec_s on the
# ms format, which broke the aggregate Mgas/s in compare reports.
_IMPORTED_EXEC_RE = re.compile(r"([\d.]+)\s*(ms|s)\s*exec")
_IMPORTED_MGAS_RE = re.compile(r"([\d.]+)\s*Mgas/s")


def _parse_imported_line(line: str) -> dict | None:
    """Extract {block, gas_used, exec_s, mgas_s} from one Besu 'Imported #'
    log line. Returns None if the line is not a block-import line."""
    m_blk = _IMPORTED_BLOCK_RE.search(line)
    if not m_blk:
        return None
    m_gas = _IMPORTED_GAS_RE.search(line)
    m_exec = _IMPORTED_EXEC_RE.search(line)
    m_mgas = _IMPORTED_MGAS_RE.search(line)

    def _int(s: str) -> int:
        return int(s.replace(",", ""))

    exec_s = None
    if m_exec:
        val = float(m_exec.group(1))
        # Normalise to seconds: "75.0ms" -> 0.075, "1.090s" -> 1.090.
        exec_s = val / 1000.0 if m_exec.group(2) == "ms" else val

    return {
        "block": _int(m_blk.group(1)),
        "gas_used": (_int(m_gas.group(1)) if m_gas else None),
        "exec_s": exec_s,
        "mgas_s": (float(m_mgas.group(1)) if m_mgas else None),
    }


def _parse_last_imported(log_path: Path) -> dict | None:
    """Metrics for the LAST 'Imported #' block in a Besu container log.

    Testing is the final phase of each test, so the last imported block is
    exactly the measured block we want to compare ("compare only the last
    block"). Returns None if no import line is found.
    """
    last: dict | None = None
    try:
        with log_path.open("r", errors="replace") as fh:
            for line in fh:
                if "Imported #" not in line:
                    continue
                parsed = _parse_imported_line(line)
                if parsed:
                    last = parsed
    except OSError:
        return None
    return last


def _replay_requests_measure(
    cfg: Config,
    secret: bytes,
    session: requests.Session,
    raw_lines: list[str],
    label: str,
    log: SweepLog,
    *,
    source_label: str,
    phase: str | None = None,
) -> tuple[bool, dict]:
    """Replay and time a sequence of JSON-RPC requests for compare mode.

    It times each Engine API call and returns per-call latency in milliseconds:

        {"newpayload_ms": [...], "fcu_ms": [...],
         "last_newpayload_ms": float | None, "total_newpayload_ms": float}

    The LAST newPayload latency is the headline number: in a
    testing/<name>.txt file it is the single measured heavy block, which is
    exactly what we want to compare between two Besu versions.

    `source_label` is the bucket name used for SweepLog counters.
    """
    prefix = f"replay [{phase}] " if phase else "replay "
    log.event(f"{prefix}{label}")

    items = _scan_requests(raw_lines)
    np_ms: list[float] = []
    fcu_ms: list[float] = []
    ok_all = True

    for line_no, method, raw in items:
        if not method:
            log.record_fail(source_label, line_no, "bad_json", {})
            ok_all = False
            if cfg.run.fail_fast:
                break
            continue

        t0 = time.perf_counter()
        status, body, err = post_engine_line(cfg, secret, session, raw)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        if err is not None and body is None:
            log.record_fail(source_label, line_no, "http_error",
                            {"method": method, "error": err})
            ok_all = False
            if cfg.run.fail_fast:
                break
            continue
        if status != 200:
            log.record_fail(source_label, line_no, "http_status",
                            {"method": method, "status": status,
                             "body": json.dumps(body) if body is not None else err})
            ok_all = False
            if cfg.run.fail_fast:
                break
            continue

        ok, kind, detail = _classify(method, body or {})
        if ok:
            log.record_ok(source_label)
        else:
            log.record_fail(source_label, line_no, kind, {"method": method, **detail})
            ok_all = False

        if method.startswith("engine_newPayload"):
            np_ms.append(elapsed_ms)
        elif method.startswith("engine_forkchoiceUpdated"):
            fcu_ms.append(elapsed_ms)

        if not ok and cfg.run.fail_fast:
            break

    return ok_all, {
        "newpayload_ms": np_ms,
        "fcu_ms": fcu_ms,
        "last_newpayload_ms": (np_ms[-1] if np_ms else None),
        "total_newpayload_ms": (sum(np_ms) if np_ms else 0.0),
    }


def _replay_file_measure(
    cfg: Config,
    secret: bytes,
    session: requests.Session,
    file_path: Path,
    log: SweepLog,
    *,
    source_label: str,
    phase: str | None = None,
) -> tuple[bool, dict]:
    """Replay and time one line-delimited JSON-RPC file."""
    return _replay_requests_measure(
        cfg, secret, session, file_path.read_text().splitlines(), file_path.name,
        log, source_label=source_label, phase=phase,
    )


def _run_version(
    cfg: Config,
    label: str,
    secret: bytes,
    tests: list[str],
    setup_dir: Path,
    testing_dir: Path,
    log: SweepLog,
) -> dict[str, dict]:
    """Run every selected test once against `cfg.besu.image`, timing the
    testing phase. Returns {test_name: per-test metrics}.

    Mirrors the per-test flow of `run_sweep` (restart: reset overlay -> start
    Besu -> prelude -> setup -> testing -> stop; rewind: one process + FCU
    back to the pre-run head) but without profiling, and with the testing
    phase timed via `_replay_file_measure`.
    """
    results: dict[str, dict] = {}
    started_container = False
    try:
        with requests.Session() as session:
            # persist-prelude: bake the gas-bump once (per version) into the
            # persistent prelude layer; per-test resets then keep it.
            if cfg.run.persist_prelude:
                if not bake_prelude_layer(cfg, secret, session, log, label=label):
                    log.event(f"[{label}] persist-prelude: gas-bump bake FAILED; "
                              "skipping this version's tests")
                    return results
            rewind = cfg.run.isolation == "rewind"
            rewind_target: tuple[int, str] | None = None
            if rewind:
                log.event(f"[{label}] isolation=rewind: one Besu process")
                per_test_reset(cfg, log)
                start_besu(cfg.besu, log)
                started_container = True
                wait_for_engine(cfg.besu, secret, log)
                prelude_ok = True
                for fname in cfg.input.prelude:
                    ok, _ = _replay_file_measure(
                        cfg, secret, session, cfg.input.dir / fname, log,
                        source_label=f"[{label}] {fname}", phase="prelude",
                    )
                    if not ok:
                        prelude_ok = False
                        break
                if not prelude_ok:
                    log.event(f"[{label}] prelude FAILED; skipping this version")
                    return results
                rewind_target = query_chain_head(cfg.besu)
                if rewind_target is None:
                    log.event(f"[{label}] rewind: no pre-run head; skipping")
                    return results
                n, h = rewind_target
                log.event(f"[{label}] rewind target: #{n:,} ({h})")

            for idx, name in enumerate(tests, start=1):
                log.event(f"[{label}] [{idx}/{len(tests)}] {name}")

                test_ok = True
                if not rewind:
                    per_test_reset(cfg, log)
                    drop_page_cache(log)
                    start_besu(cfg.besu, log)
                    started_container = True
                    wait_for_engine(cfg.besu, secret, log)

                    for fname in cfg.input.prelude:
                        ok, _ = _replay_file_measure(
                            cfg, secret, session, cfg.input.dir / fname, log,
                            source_label=f"[{label}] {fname}", phase="prelude",
                        )
                        if not ok:
                            test_ok = False
                            if cfg.run.fail_fast:
                                log.event(
                                    f"[{label}] fail-fast tripped during prelude {fname}"
                                )
                                break

                testing_metrics: dict | None = None
                if test_ok or not cfg.run.fail_fast:
                    # Setup phase (state prep) — replayed, not part of the number.
                    if _test_format(cfg) == "stateful_engine":
                        setup_lines, testing_lines, source = _stateful_test_requests(
                            cfg, name
                        )
                        s_ok, _ = _replay_requests_measure(
                            cfg, secret, session, setup_lines, source, log,
                            source_label=f"[{label}] setup/{name}", phase="setup",
                        )
                    else:
                        s_ok, _ = _replay_file_measure(
                            cfg, secret, session, setup_dir / name, log,
                            source_label=f"[{label}] setup/{name}", phase="setup",
                        )
                    if not s_ok:
                        test_ok = False
                    # Testing phase — this is the measured block.
                    if s_ok or not cfg.run.fail_fast:
                        if _test_format(cfg) == "stateful_engine":
                            t_ok, testing_metrics = _replay_requests_measure(
                                cfg, secret, session, testing_lines, source, log,
                                source_label=f"[{label}] testing/{name}",
                                phase="testing",
                            )
                        else:
                            t_ok, testing_metrics = _replay_file_measure(
                                cfg, secret, session, testing_dir / name, log,
                                source_label=f"[{label}] testing/{name}",
                                phase="testing",
                            )
                        if not t_ok:
                            test_ok = False

                besu_log_path = (
                    log.root / f"besu-{_safe_label(label)}-{idx:04d}-"
                               f"{_slugify(name)}{'' if test_ok else '-FAIL'}.log"
                )
                save_container_logs(cfg.besu.container_name, besu_log_path, log)
                if rewind:
                    if rewind_target is not None:
                        n, h = rewind_target
                        if not rewind_canonical_head(
                            cfg, secret, session, log, n, h
                        ):
                            log.event(f"[{label}] rewind FAILED after {name}")
                            test_ok = False
                            if cfg.run.fail_fast:
                                pass
                else:
                    stop_container(cfg.besu.container_name)
                    started_container = False

                # Pull gas-used / exec-time / Mgas/s straight from Besu's own
                # "Imported #" line for the last (measured) block.
                imported = _parse_last_imported(besu_log_path)
                if imported:
                    log.event(
                        f"[{label}] {name}: block #{imported['block']:,} "
                        f"gas={imported['gas_used']:,} "
                        f"exec={imported['exec_s']}s mgas/s={imported['mgas_s']}"
                    )
                else:
                    log.event(f"[{label}] {name}: no 'Imported #' line in besu log")

                results[name] = {
                    "ok": test_ok,
                    "block": imported["block"] if imported else None,
                    "gas_used": imported["gas_used"] if imported else None,
                    "exec_s": imported["exec_s"] if imported else None,
                    "mgas_s": imported["mgas_s"] if imported else None,
                    # Python-side timing kept as a fallback latency source.
                    "last_newpayload_ms": (
                        testing_metrics["last_newpayload_ms"] if testing_metrics else None
                    ),
                }

                if not test_ok and cfg.run.fail_fast:
                    log.event(f"[{label}] fail-fast: stopping after {name}")
                    break
    finally:
        if started_container and cfg.run.stop_container_on_exit:
            log.event(f"[{label}] stopping container {cfg.besu.container_name}")
            stop_container(cfg.besu.container_name)

    return results


def _latency_ms(r: dict) -> float | None:
    """Per-test latency in ms. Prefer Besu's own block exec time (exec_s);
    fall back to the Python-side newPayload timing if the log had no exec."""
    e = r.get("exec_s")
    if isinstance(e, (int, float)):
        return e * 1000.0
    ms = r.get("last_newpayload_ms")
    return ms if isinstance(ms, (int, float)) else None


def _build_comparison(
    label_x: str, image_x: str, results_x: dict[str, dict],
    label_y: str, image_y: str, results_y: dict[str, dict],
    tests: list[str],
) -> dict:
    """Join the two per-version result dicts into a comparison structure.

    For each test we compare the LAST imported (measured) block:
      - gas used (deterministic, so identical for x and y),
      - Mgas/s throughput (Besu-reported),
      - latency = Besu block exec time, in ms.

    Throughput is the headline: delta_mgas_pct = (y/x - 1) * 100, so a
    positive value means y is faster (higher Mgas/s).
    """
    rows: list[dict] = []
    total_gas = 0
    x_total_exec_s = 0.0
    y_total_exec_s = 0.0
    faster = slower = same = 0
    x_failures = y_failures = 0
    gas_mismatches = 0

    for name in tests:
        rx = results_x.get(name, {})
        ry = results_y.get(name, {})
        x_ok = bool(rx.get("ok"))
        y_ok = bool(ry.get("ok"))
        if not x_ok:
            x_failures += 1
        if not y_ok:
            y_failures += 1

        gas_x = rx.get("gas_used")
        gas_y = ry.get("gas_used")
        gas_used = gas_x if isinstance(gas_x, (int, float)) else gas_y
        gas_match = (
            isinstance(gas_x, (int, float)) and isinstance(gas_y, (int, float))
            and gas_x == gas_y
        )
        if (isinstance(gas_x, (int, float)) and isinstance(gas_y, (int, float))
                and gas_x != gas_y):
            gas_mismatches += 1

        x_mgas = rx.get("mgas_s")
        y_mgas = ry.get("mgas_s")
        x_lat = _latency_ms(rx)
        y_lat = _latency_ms(ry)

        delta_mgas = delta_mgas_pct = None
        if isinstance(x_mgas, (int, float)) and isinstance(y_mgas, (int, float)):
            delta_mgas = y_mgas - x_mgas
            delta_mgas_pct = (delta_mgas / x_mgas * 100.0) if x_mgas else None

        delta_lat_ms = delta_lat_pct = None
        if isinstance(x_lat, (int, float)) and isinstance(y_lat, (int, float)):
            delta_lat_ms = y_lat - x_lat
            delta_lat_pct = (delta_lat_ms / x_lat * 100.0) if x_lat else None

        # Aggregate only when both versions reported a usable block.
        x_exec = rx.get("exec_s")
        y_exec = ry.get("exec_s")
        if (isinstance(gas_used, (int, float))
                and isinstance(x_exec, (int, float))
                and isinstance(y_exec, (int, float))):
            total_gas += gas_used
            x_total_exec_s += x_exec
            y_total_exec_s += y_exec

        if isinstance(delta_mgas_pct, (int, float)):
            if delta_mgas_pct > 1.0:
                faster += 1
            elif delta_mgas_pct < -1.0:
                slower += 1
            else:
                same += 1

        rows.append({
            "test": name,
            "x_ok": x_ok, "y_ok": y_ok,
            "gas_used": gas_used, "gas_match": gas_match,
            "x_mgas": x_mgas, "y_mgas": y_mgas,
            "delta_mgas": delta_mgas, "delta_mgas_pct": delta_mgas_pct,
            "x_lat_ms": x_lat, "y_lat_ms": y_lat,
            "delta_lat_ms": delta_lat_ms, "delta_lat_pct": delta_lat_pct,
        })

    x_agg_mgas = (total_gas / 1e6 / x_total_exec_s) if x_total_exec_s else None
    y_agg_mgas = (total_gas / 1e6 / y_total_exec_s) if y_total_exec_s else None
    overall_mgas_pct = (
        (y_agg_mgas - x_agg_mgas) / x_agg_mgas * 100.0
        if (x_agg_mgas and y_agg_mgas) else None
    )
    return {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "metric": "mgas_s",
        "metric_desc": "Besu-reported gas used, Mgas/s throughput and block "
                       "exec latency of the last imported (measured) block, "
                       "parsed from the per-test container log",
        "version_x": {"label": label_x, "image": image_x},
        "version_y": {"label": label_y, "image": image_y},
        "rows": rows,
        "summary": {
            "tests": len(tests),
            "compared": faster + slower + same,
            "total_gas": total_gas,
            "x_total_exec_s": x_total_exec_s,
            "y_total_exec_s": y_total_exec_s,
            "x_agg_mgas_s": x_agg_mgas,
            "y_agg_mgas_s": y_agg_mgas,
            "overall_mgas_pct": overall_mgas_pct,
            "y_faster": faster,
            "y_slower": slower,
            "neutral": same,
            "x_failures": x_failures,
            "y_failures": y_failures,
            "gas_mismatches": gas_mismatches,
        },
    }


def _fmt_ms(v) -> str:
    if not isinstance(v, (int, float)):
        return "n/a"
    return f"{v:,.1f}"


def _fmt_mgas(v) -> str:
    if not isinstance(v, (int, float)):
        return "n/a"
    return f"{v:,.2f}"


def _fmt_gas_m(v) -> str:
    """Gas used rendered in millions of gas (Mgas)."""
    if not isinstance(v, (int, float)):
        return "n/a"
    return f"{v / 1e6:,.1f}M"


def _fmt_pct(v) -> str:
    if not isinstance(v, (int, float)):
        return "n/a"
    return f"{v:+.1f}%"


def _render_comparison_html(cmp: dict) -> str:
    """Self-contained HTML report (embedded CSS + a little vanilla JS for
    column sorting). No external assets, so it opens fine after `scp`.

    Headline is Mgas/s throughput on the last imported block; gas used is
    shown once (identical for both versions) and latency (block exec time)
    is shown per version with its delta.
    """
    import html as _html

    vx, vy = cmp["version_x"], cmp["version_y"]
    s = cmp["summary"]

    def _tput_cls(pct):
        """Colour by throughput delta: y higher Mgas/s => faster => green."""
        if not isinstance(pct, (int, float)):
            return "na"
        if pct > 1.0:
            return "faster"
        if pct < -1.0:
            return "slower"
        return "neutral"

    # Worst throughput regression first (most negative delta on top); tests
    # with no comparable number sink to the bottom.
    def _sort_key(r):
        d = r["delta_mgas_pct"]
        return (d if isinstance(d, (int, float)) else float("inf"),)
    rows = sorted(cmp["rows"], key=_sort_key)

    body_rows = []
    for r in rows:
        cls = _tput_cls(r["delta_mgas_pct"])
        d_mgas = r["delta_mgas"]
        d_lat = r["delta_lat_ms"]
        gas_txt = _fmt_gas_m(r["gas_used"])
        if not r["gas_match"] and isinstance(r["gas_used"], (int, float)):
            gas_txt += " &#9888;"  # gas differed between x and y
        body_rows.append(
            "<tr class='{cls}'>"
            "<td class='test' title='{full}'><code>{test}</code></td>"
            "<td class='num gas'>{gas}</td>"
            "<td class='num'>{xm}</td>"
            "<td class='num'>{ym}</td>"
            "<td class='num delta' data-sort='{dms_sort}'>{dmgas}</td>"
            "<td class='num pct' data-sort='{dpct_sort}'>{dmgaspct}</td>"
            "<td class='num lat'>{xl}</td>"
            "<td class='num lat'>{yl}</td>"
            "<td class='num latdelta' data-sort='{dlat_sort}'>{dlat}</td>"
            "<td class='status'>{stat}</td>"
            "</tr>".format(
                cls=cls,
                full=_html.escape(r["test"]),
                test=_html.escape(r["test"]),
                gas=gas_txt,
                xm=_fmt_mgas(r["x_mgas"]),
                ym=_fmt_mgas(r["y_mgas"]),
                dms_sort=(d_mgas if isinstance(d_mgas, (int, float)) else -1e18),
                dmgas=(f"{d_mgas:+,.2f}" if isinstance(d_mgas, (int, float)) else "n/a"),
                dpct_sort=(r["delta_mgas_pct"]
                           if isinstance(r["delta_mgas_pct"], (int, float)) else -1e18),
                dmgaspct=_fmt_pct(r["delta_mgas_pct"]),
                xl=_fmt_ms(r["x_lat_ms"]),
                yl=_fmt_ms(r["y_lat_ms"]),
                dlat_sort=(d_lat if isinstance(d_lat, (int, float)) else 1e18),
                dlat=(f"{d_lat:+,.1f}" if isinstance(d_lat, (int, float)) else "n/a"),
                stat=("ok" if (r["x_ok"] and r["y_ok"])
                      else "fail x" if not r["x_ok"] and r["y_ok"]
                      else "fail y" if r["x_ok"] and not r["y_ok"]
                      else "fail x+y"),
            )
        )

    overall = s["overall_mgas_pct"]
    overall_cls = _tput_cls(overall)
    overall_txt = _fmt_pct(overall) if isinstance(overall, (int, float)) else "n/a"

    return """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Besu replay comparison: {lx} vs {ly}</title>
<style>
  :root {{ color-scheme: light dark; }}
  body {{ font: 14px/1.5 -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
         margin: 0; padding: 24px; background: #0f1115; color: #e6e6e6; }}
  h1 {{ font-size: 20px; margin: 0 0 4px; }}
  .sub {{ color: #9aa4b2; margin-bottom: 20px; }}
  .sub code {{ color: #cdd6f4; }}
  .cards {{ display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 22px; }}
  .card {{ background: #1b1f2a; border: 1px solid #2a3040; border-radius: 10px;
          padding: 12px 16px; min-width: 150px; }}
  .card .k {{ color: #9aa4b2; font-size: 12px; text-transform: uppercase;
             letter-spacing: .04em; }}
  .card .v {{ font-size: 22px; font-weight: 600; margin-top: 4px; }}
  .v.faster {{ color: #51cf66; }} .v.slower {{ color: #ff6b6b; }}
  .v.neutral {{ color: #e6e6e6; }} .v.na {{ color: #9aa4b2; }}
  table {{ border-collapse: collapse; width: 100%; background: #161a23;
          border-radius: 10px; overflow: hidden; }}
  th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #232838; }}
  th {{ background: #1b2030; cursor: pointer; user-select: none; position: sticky; top: 0; }}
  th:hover {{ background: #222a3d; }}
  td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  td.gas {{ color: #cdd6f4; }}
  td.lat, td.latdelta {{ color: #9aa4b2; }}
  td.test code {{ color: #cdd6f4; word-break: break-all; }}
  tr.faster td.pct, tr.faster td.delta {{ color: #51cf66; }}
  tr.slower td.pct, tr.slower td.delta {{ color: #ff6b6b; }}
  tr.neutral td.pct {{ color: #9aa4b2; }}
  tr.na {{ opacity: .6; }}
  td.status {{ color: #9aa4b2; }}
  .legend {{ margin-top: 14px; color: #9aa4b2; font-size: 12px; }}
</style></head>
<body>
  <h1>Besu stateful-replay comparison</h1>
  <div class="sub">
    <b>x</b> = <code>{lx}</code> (<code>{ix}</code>) &nbsp;vs&nbsp;
    <b>y</b> = <code>{ly}</code> (<code>{iy}</code>)<br>
    Metric: {metric_desc}.<br>
    Generated {gen}.
  </div>

  <div class="cards">
    <div class="card"><div class="k">Tests compared</div><div class="v">{compared}/{tests}</div></div>
    <div class="card"><div class="k">Overall throughput (y vs x)</div><div class="v {ocls}">{overall}</div></div>
    <div class="card"><div class="k">x aggregate Mgas/s</div><div class="v na">{xagg}</div></div>
    <div class="card"><div class="k">y aggregate Mgas/s</div><div class="v na">{yagg}</div></div>
    <div class="card"><div class="k">y faster</div><div class="v faster">{faster}</div></div>
    <div class="card"><div class="k">y slower</div><div class="v slower">{slower}</div></div>
    <div class="card"><div class="k">neutral (&lt;1%)</div><div class="v neutral">{neutral}</div></div>
    <div class="card"><div class="k">failures x / y</div><div class="v na">{xf} / {yf}</div></div>
  </div>

  <table id="cmp">
    <thead><tr>
      <th data-col="0" data-type="str">Test</th>
      <th data-col="1" data-type="num">Gas used</th>
      <th data-col="2" data-type="num">x &middot; {lx} (Mgas/s)</th>
      <th data-col="3" data-type="num">y &middot; {ly} (Mgas/s)</th>
      <th data-col="4" data-type="num">&#916; Mgas/s</th>
      <th data-col="5" data-type="num">&#916; % tput</th>
      <th data-col="6" data-type="num">x lat (ms)</th>
      <th data-col="7" data-type="num">y lat (ms)</th>
      <th data-col="8" data-type="num">&#916; lat (ms)</th>
      <th data-col="9" data-type="str">status</th>
    </tr></thead>
    <tbody>
      {rows}
    </tbody>
  </table>

  <div class="legend">
    One row per test = the last imported (measured) block. Gas used is
    identical across versions (&#9888; flags a mismatch). Throughput
    (Mgas/s) and latency are Besu's own reported numbers. Rows are sorted
    worst throughput regression first. Green = y faster than x, red = y
    slower. &Delta;% tput is relative to x; click a header to re-sort.
  </div>

<script>
(function () {{
  var table = document.getElementById('cmp');
  var tbody = table.tBodies[0];
  var dir = {{}};
  function val(td, type) {{
    if (td && td.dataset && td.dataset.sort !== undefined) return parseFloat(td.dataset.sort);
    var t = td ? td.textContent.trim() : '';
    if (type === 'num') {{ var n = parseFloat(t.replace(/[,%+M]/g, '')); return isNaN(n) ? Infinity : n; }}
    return t.toLowerCase();
  }}
  Array.prototype.forEach.call(table.tHead.rows[0].cells, function (th) {{
    th.addEventListener('click', function () {{
      var col = +th.dataset.col, type = th.dataset.type;
      dir[col] = !dir[col];
      var rows = Array.prototype.slice.call(tbody.rows);
      rows.sort(function (a, b) {{
        var va = val(a.cells[col], type), vb = val(b.cells[col], type);
        if (va < vb) return dir[col] ? -1 : 1;
        if (va > vb) return dir[col] ? 1 : -1;
        return 0;
      }});
      rows.forEach(function (r) {{ tbody.appendChild(r); }});
    }});
  }});
}})();
</script>
</body></html>
""".format(
        lx=_html.escape(vx["label"]), ly=_html.escape(vy["label"]),
        ix=_html.escape(vx["image"]), iy=_html.escape(vy["image"]),
        metric_desc=_html.escape(cmp["metric_desc"]),
        gen=_html.escape(cmp["generated_at"]),
        compared=s["compared"], tests=s["tests"],
        overall=overall_txt, ocls=overall_cls,
        xagg=_fmt_mgas(s["x_agg_mgas_s"]), yagg=_fmt_mgas(s["y_agg_mgas_s"]),
        faster=s["y_faster"], slower=s["y_slower"], neutral=s["neutral"],
        xf=s["x_failures"], yf=s["y_failures"],
        rows="\n      ".join(body_rows) if body_rows else
             "<tr><td colspan='10'>no tests</td></tr>",
    )


def run_compare(
    cfg: Config,
    *,
    image_x: str, image_y: str,
    label_x: str, label_y: str,
    filter_override: str | None,
    limit: int | None,
    dry_run: bool,
    select: list[str] | None = None,
) -> int:
    """Compare-mode entry point.

    Runs every selected test on `image_x`, then on `image_y`, then writes
    comparison.json + comparison.html into the run dir.
    """
    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    log_root = cfg.run.log_dir / f"{timestamp}-compare"
    log = SweepLog(log_root)
    log.event(f"compare start, log dir = {log_root}")
    log.event(f"  x: label={label_x!r} image={image_x!r}")
    log.event(f"  y: label={label_y!r} image={image_y!r}")

    defer_limit = _match_chain_head_enabled(cfg)
    tests = discover_tests(
        cfg, filter_override, limit, explicit=select, defer_limit=defer_limit,
    )
    log.event(
        f"matched {len(tests)} tests "
        f"(filter={filter_override or cfg.tests.filter}, order={cfg.tests.order}, "
        f"limit={limit})"
    )

    if dry_run:
        tests = apply_chain_head_filter(cfg, tests, log, dry_run=True)
        tests = _apply_limit(tests, limit)
        (log_root / "selected_tests.txt").write_text("\n".join(tests) + "\n")
        log.event("dry-run: wrote selected_tests.txt and exiting")
        log.flush_summary({
            "mode": "compare", "dry_run": True, "selected": len(tests),
            "version_x": {"label": label_x, "image": image_x},
            "version_y": {"label": label_y, "image": image_y},
        })
        log.close()
        print(f"\ncompare dry-run: would run {len(tests)} tests on "
              f"{label_x!r} then {label_y!r}.")
        return 0

    # Same preflight as run_sweep (prelude exists, JWT, mounts, sudo).
    for name in cfg.input.prelude:
        p = cfg.input.dir / name
        if not p.is_file():
            raise FileNotFoundError(f"prelude file missing: {p}")
    prepare_host_bind_mounts(cfg, log)
    _reset_script = reset_script(cfg)
    for probe, hint in (
        (DOCKER + ["version", "--format", "{{.Server.Version}}"], "sudo -n docker version"),
        (["sudo", "-n", str(_reset_script), "--help"], f"sudo -n {_reset_script} --help"),
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
    tests = apply_chain_head_filter(
        cfg, tests, log, dry_run=False, secret=secret,
    )
    tests = _apply_limit(tests, limit)
    (log_root / "selected_tests.txt").write_text("\n".join(tests) + "\n")
    log.event(f"running {len(tests)} test(s)")
    setup_dir = cfg.input.dir / cfg.tests.setup_subdir
    testing_dir = cfg.input.dir / cfg.tests.testing_subdir

    all_results: dict[str, dict[str, dict]] = {}
    try:
        for label, image in ((label_x, image_x), (label_y, image_y)):
            log.event(f"=== running suite on version {label!r} (image={image}) ===")
            # New Config view with only the image swapped; nothing else mutates,
            # and the original cfg/besu objects are left untouched.
            vcfg = dataclasses.replace(
                cfg, besu=dataclasses.replace(cfg.besu, image=image)
            )
            all_results[label] = _run_version(
                vcfg, label, secret, tests, setup_dir, testing_dir, log
            )
    finally:
        if cfg.run.stop_container_on_exit:
            stop_container(cfg.besu.container_name)

    comparison = _build_comparison(
        label_x, image_x, all_results.get(label_x, {}),
        label_y, image_y, all_results.get(label_y, {}),
        tests,
    )
    json_path = log_root / "comparison.json"
    html_path = log_root / "comparison.html"
    json_path.write_text(json.dumps(comparison, indent=2))
    html_path.write_text(_render_comparison_html(comparison))
    log.event(f"wrote {json_path}")
    log.event(f"wrote {html_path}")

    log.flush_summary({
        "mode": "compare",
        "version_x": {"label": label_x, "image": image_x},
        "version_y": {"label": label_y, "image": image_y},
        "comparison_summary": comparison["summary"],
    })
    log.close()

    sm = comparison["summary"]
    print(f"\nComparison written to:\n  {html_path}\n  {json_path}")
    print(f"  y faster on {sm['y_faster']} / slower on {sm['y_slower']} / "
          f"neutral {sm['neutral']} (of {sm['compared']} compared).")
    return 0


def rebuild_report(run_dir: Path) -> int:
    """Regenerate comparison.html + comparison.json from an existing compare
    run's saved Besu logs, WITHOUT re-running anything.

    Useful after changing the report format, or to upgrade a report produced
    by an older version of this script: the per-test container logs already
    contain Besu's `Imported #` lines, so gas / Mgas/s / latency can be
    re-extracted on the spot.

    Reads `summary.json` (for the two version labels + images) and
    `selected_tests.txt` (for the ordered test list), then matches each test
    to its `besu-<label>-NNNN-*.log` file by index.
    """
    run_dir = _abs_path(run_dir)
    summary_path = run_dir / "summary.json"
    selected_path = run_dir / "selected_tests.txt"
    if not summary_path.is_file():
        raise FileNotFoundError(f"no summary.json in {run_dir}")
    if not selected_path.is_file():
        raise FileNotFoundError(f"no selected_tests.txt in {run_dir}")

    summ = json.loads(summary_path.read_text())
    vx = summ.get("version_x") or {}
    vy = summ.get("version_y") or {}
    label_x = vx.get("label", "x")
    image_x = vx.get("image", "?")
    label_y = vy.get("label", "y")
    image_y = vy.get("image", "?")
    tests = [ln for ln in selected_path.read_text().splitlines() if ln.strip()]

    def _collect(label: str) -> dict[str, dict]:
        res: dict[str, dict] = {}
        sl = _safe_label(label)
        for idx, name in enumerate(tests, start=1):
            matches = sorted(run_dir.glob(f"besu-{sl}-{idx:04d}-*.log"))
            parsed = None
            failed = False
            for mpath in matches:
                if mpath.name.endswith("-FAIL.log"):
                    failed = True
                p = _parse_last_imported(mpath)
                if p:
                    parsed = p
            res[name] = {
                "ok": (not failed) and parsed is not None,
                "block": parsed["block"] if parsed else None,
                "gas_used": parsed["gas_used"] if parsed else None,
                "exec_s": parsed["exec_s"] if parsed else None,
                "mgas_s": parsed["mgas_s"] if parsed else None,
                "last_newpayload_ms": None,
            }
        return res

    comparison = _build_comparison(
        label_x, image_x, _collect(label_x),
        label_y, image_y, _collect(label_y),
        tests,
    )
    json_path = run_dir / "comparison.json"
    html_path = run_dir / "comparison.html"
    json_path.write_text(json.dumps(comparison, indent=2))
    html_path.write_text(_render_comparison_html(comparison))

    sm = comparison["summary"]
    print(f"Rebuilt report from {run_dir}:\n  {html_path}\n  {json_path}")
    print(f"  y faster on {sm['y_faster']} / slower on {sm['y_slower']} / "
          f"neutral {sm['neutral']} (of {sm['compared']} compared).")
    if sm["gas_mismatches"]:
        print(f"  warning: {sm['gas_mismatches']} test(s) had differing gas "
              "used between x and y.")
    return 0


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
    p.add_argument("--tests-from", default=None, metavar="FILE",
                   help="run exactly the test basenames listed in FILE (one per "
                        "line, '#' comments allowed), in that order. Overrides "
                        "--filter and tests.order; used by the web UI to run an "
                        "arbitrary multi-selection. Still intersected with the "
                        "valid setup/testing pairs; --limit still applies.")
    p.add_argument("--dry-run", action="store_true",
                   help="resolve config + selected tests, then exit without touching the system")
    p.add_argument("--profile", action="store_true",
                   help="enable async-profiler around the last newPayload+FCU pair "
                        "of setup/ and testing/ (overrides profile.enabled in yaml)")
    p.add_argument("--jfr-all", action="store_true",
                   help="enable profiling and capture CPU/wall + allocation (memory) "
                        "+ lock contention into a single JFR per phase (sets "
                        "output_format=jfr and default --alloc/--lock if unset)")
    p.add_argument("--reset-backend", choices=RESET_BACKENDS, default=None,
                   help="how to reset Besu state between tests: 'overlayfs' "
                        "(OverlayFS over a snapshot dir, default) or 'schelk' "
                        "(dm-era block-level rollback). Overrides run.reset_backend.")
    p.add_argument("--isolation", choices=ISOLATION_MODES, default=None,
                   help="restart = new Besu container per test (stateful). "
                        "rewind = one process, FCU back to the pre-run head "
                        "after each test (compute / besu-bal-full). "
                        "Overrides run.isolation.")
    p.add_argument("--no-match-chain-head", dest="match_chain_head",
                   action="store_false", default=None,
                   help="keep stateful fixtures that do not chain onto the "
                        "head after the pre-run (genesis-style cases). "
                        "Default: drop them.")
    p.add_argument("--skip-gas-bump", "--no-gas-bump", dest="skip_gas_bump",
                   action="store_true", default=None,
                   help="skip the gas-bump prelude file (input.gas_bump_file); use "
                        "when the baseline ALREADY contains the gas-bumped blocks "
                        "(build it once with --prepare-baseline). Works on both "
                        "backends: overlayfs swaps to the pre-bumped snapshot dir, "
                        "schelk rolls back to the pre-bumped virgin device. Combine "
                        "with --compare for a gas-bump-free comparison. Overrides "
                        "run.skip_gas_bump.")
    p.add_argument("--persist-prelude", dest="persist_prelude",
                   action="store_true", default=None,
                   help="(overlayfs) bake input.gas_bump_file ONCE into a persistent "
                        "prelude overlay layer at sweep start, then keep it across "
                        "per-test resets (reset-test). Zero-copy alternative to "
                        "--prepare-baseline + --skip-gas-bump; the gas-bump is "
                        "dropped from the per-test prelude. Overrides "
                        "run.persist_prelude.")
    p.add_argument("--prepare-baseline", action="store_true",
                   help="bake the gas-bump blocks into the baseline once so later "
                        "runs can use --skip-gas-bump. Resets the baseline, starts "
                        "Besu, replays ONLY input.gas_bump_file, stops Besu, then: "
                        "(overlayfs) rsyncs the flattened result into a NEW snapshot "
                        "dir (--baseline-out), leaving the original untouched; "
                        "(schelk) `schelk promote`s the gas-bumped scratch onto the "
                        "virgin device IN PLACE. Runs no tests.")
    p.add_argument("--baseline-out", default=None, metavar="DIR",
                   help="output directory for --prepare-baseline on overlayfs "
                        "(default: <besu.data_snapshot_dir>-bumped). Ignored for "
                        "schelk, which promotes the virgin device in place.")

    # --- compare mode (run the suite twice on two Besu images, diff the times) ---
    p.add_argument("--compare", action="store_true",
                   help="run every selected test on two Besu images back-to-back "
                        "and emit an HTML comparison of testing-block times")
    p.add_argument("--image-x", default=None,
                   help="compare mode: first ('baseline') Besu image. "
                        "Defaults to besu.image from the config.")
    p.add_argument("--image-y", default=None,
                   help="compare mode: second ('candidate') Besu image. Required "
                        "with --compare.")
    p.add_argument("--label-x", default=None,
                   help="compare mode: display label for image-x (default: the image tag)")
    p.add_argument("--label-y", default=None,
                   help="compare mode: display label for image-y (default: the image tag)")
    p.add_argument("--rebuild-report", default=None, metavar="RUN_DIR",
                   help="regenerate comparison.html/json from an existing "
                        "runs/<ts>-compare dir's saved Besu logs, without "
                        "re-running anything (no config needed)")
    return p.parse_args(argv)


def _install_sigint_handler() -> None:
    def _handle(signum, frame):  # noqa: ARG001
        print("\nbench: caught SIGINT, propagating", file=sys.stderr)
        raise KeyboardInterrupt
    signal.signal(signal.SIGINT, _handle)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])

    # Report rebuild reads only the existing run dir; no config required.
    if args.rebuild_report:
        return rebuild_report(_abs_path(args.rebuild_report))

    cfg = load_config(_abs_path(args.config))
    if args.profile:
        cfg.profile.enabled = True
    if args.jfr_all:
        cfg.profile.enabled = True
        cfg.profile.output_format = "jfr"
        if not cfg.profile.alloc:
            cfg.profile.alloc = "512k"
        if not cfg.profile.lock:
            cfg.profile.lock = "10ms"
    if args.reset_backend:
        cfg.run.reset_backend = args.reset_backend
    if args.isolation:
        cfg.run.isolation = args.isolation
    print(f"reset backend: {cfg.run.reset_backend}")
    print(f"isolation: {cfg.run.isolation}")
    warn_if_rewind_fixture_mix(cfg)
    if cfg.run.reset_backend == "schelk":
        missing = [k for k in ("virgin", "scratch", "ramdisk")
                   if not getattr(cfg.schelk, k)]
        if missing:
            print(
                "error: reset_backend='schelk' requires schelk.{} in the config "
                "(block devices for the dm-era backend). See config.example.yaml.".format(
                    ", schelk.".join(missing)
                ),
                file=sys.stderr,
            )
            return 2

    # Gas-bump skip: when the snapshot already contains the gas-bumped blocks,
    # drop input.gas_bump_file from the prelude so it is not replayed. funding
    # (and the tests) then chain straight onto the snapshot tip, which must be
    # the gas-bump tip. Done once here so every downstream site (preflight,
    # sweep, compare) sees the already-filtered prelude.
    if args.skip_gas_bump:
        cfg.run.skip_gas_bump = True
    if args.persist_prelude:
        cfg.run.persist_prelude = True
    if args.match_chain_head is False:
        cfg.tests.match_chain_head = False

    if cfg.run.skip_gas_bump and cfg.run.persist_prelude:
        print(
            "error: --skip-gas-bump and --persist-prelude are mutually exclusive. "
            "Both remove the gas-bump from the per-test prelude, but --skip-gas-bump "
            "expects a pre-bumped snapshot while --persist-prelude bakes the gas-bump "
            "into a persistent overlay layer at sweep start. Pick one.",
            file=sys.stderr,
        )
        return 2
    if cfg.run.isolation == "rewind" and cfg.run.persist_prelude:
        print(
            "error: isolation=rewind keeps one Besu process and does not reset "
            "the overlay between tests. --persist-prelude is for the restart "
            "isolation loop. Use --skip-gas-bump with rewind.",
            file=sys.stderr,
        )
        return 2
    if cfg.run.persist_prelude and cfg.run.reset_backend != "overlayfs":
        print(
            "error: --persist-prelude is OverlayFS-only (it bakes the gas-bump into "
            "the prelude overlay layer). For the schelk backend, bake the gas-bump "
            "into the virgin device once with `--prepare-baseline`, then run with "
            "`--skip-gas-bump`.",
            file=sys.stderr,
        )
        return 2

    # Either gas-bump-skipping mode drops input.gas_bump_file from the PER-TEST
    # prelude. --skip-gas-bump expects it already in the snapshot; --persist-prelude
    # bakes it once into the prelude overlay layer (below / in the sweep). Done
    # once here so every downstream site (preflight, sweep, compare) sees the
    # already-filtered prelude.
    if cfg.run.skip_gas_bump or cfg.run.persist_prelude:
        mode = "skip-gas-bump" if cfg.run.skip_gas_bump else "persist-prelude"
        before = list(cfg.input.prelude)
        configured = Path(cfg.input.gas_bump_file)
        cfg.input.prelude = [
            f for f in before
            if Path(f) != configured and Path(f).name != configured.name
        ]
        removed = [f for f in before if f not in cfg.input.prelude]
        if removed:
            if cfg.run.persist_prelude:
                where = "the persistent prelude layer"
            elif cfg.run.reset_backend == "schelk":
                where = "the pre-bumped virgin device"
            else:
                where = "the pre-bumped snapshot"
            print(f"{mode}: omitting prelude file(s) {removed} from the per-test "
                  f"prelude (replayed once into {where}). per-test prelude is now "
                  f"{cfg.input.prelude or '[]'}")
        else:
            print(f"{mode}: no prelude entry named "
                  f"{cfg.input.gas_bump_file!r} to skip; per-test prelude unchanged "
                  f"({cfg.input.prelude or '[]'})")

    _install_sigint_handler()

    # Explicit test selection (web UI multi-select via --tests-from). Read the
    # newline-delimited basenames once here and pass them to both run modes.
    select: list[str] | None = None
    if args.tests_from:
        tf = _abs_path(args.tests_from)
        if not tf.is_file():
            print(f"error: --tests-from file not found: {tf}", file=sys.stderr)
            return 2
        select = [
            ln.strip() for ln in tf.read_text().splitlines()
            if ln.strip() and not ln.lstrip().startswith("#")
        ]
        if not select:
            print(f"error: --tests-from file is empty: {tf}", file=sys.stderr)
            return 2
        print(f"tests-from: {len(select)} explicitly selected test(s) from {tf}")

    if args.prepare_baseline:
        return run_prepare_baseline(
            cfg,
            Path(args.baseline_out).expanduser() if args.baseline_out else None,
        )

    # When gas-bump is skipped on OverlayFS, also switch the baseline to the
    # pre-bumped snapshot automatically, so one flag (--skip-gas-bump) flips
    # both the prelude AND the snapshot with no config edit. (schelk's baseline
    # is the virgin block device, so there is no dir to swap.)
    if cfg.run.skip_gas_bump and cfg.run.reset_backend == "overlayfs":
        bumped = _bumped_snapshot_dir(cfg)
        if not bumped.is_dir():
            print(
                f"error: --skip-gas-bump needs the pre-bumped snapshot at {bumped}, "
                "which does not exist. Build it once with `--prepare-baseline` "
                "(or set besu.bumped_snapshot_dir to its location).",
                file=sys.stderr,
            )
            return 2
        print(f"skip-gas-bump: using pre-bumped snapshot {bumped}")
        cfg.besu.data_snapshot_dir = bumped
    elif cfg.run.skip_gas_bump and cfg.run.reset_backend == "schelk":
        # schelk's baseline is the virgin device, which must already contain the
        # gas-bump (bake it once with `--prepare-baseline`). There is no dir to
        # swap, so the per-test reset (schelk restore) already rolls back to it.
        print(f"skip-gas-bump: using the schelk virgin device {cfg.schelk.virgin} as "
              "the pre-bumped baseline (run --prepare-baseline once to bake the "
              "gas-bump into it).")

    if args.compare:
        image_x = args.image_x or cfg.besu.image
        image_y = args.image_y
        if not image_y:
            print(
                "error: --compare needs two images. Pass --image-y <image> "
                "(and optionally --image-x <image>; it defaults to besu.image "
                f"= {cfg.besu.image!r}).",
                file=sys.stderr,
            )
            return 2
        label_x = args.label_x or _image_label(image_x)
        label_y = args.label_y or _image_label(image_y)
        if label_x == label_y:
            # Disambiguate identical labels so the report columns stay distinct.
            label_x, label_y = f"{label_x} (x)", f"{label_y} (y)"
        return run_compare(
            cfg,
            image_x=image_x, image_y=image_y,
            label_x=label_x, label_y=label_y,
            filter_override=args.filter, limit=args.limit,
            dry_run=args.dry_run, select=select,
        )

    return run_sweep(cfg, filter_override=args.filter, limit=args.limit,
                     pick=args.pick, dry_run=args.dry_run, select=select)


if __name__ == "__main__":
    sys.exit(main())
