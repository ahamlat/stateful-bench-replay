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
    filter: str
    order: str


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
            filter=str(t.get("filter", "*")),
            order=str(t.get("order", "alphabetical")),
        ),
        run=RunConfig(
            reset_overlay=bool(r.get("reset_overlay", True)),
            reset_backend=_validate_backend(r.get("reset_backend", "overlayfs")),
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


def _validate_backend(value: str) -> str:
    value = str(value).lower()
    if value not in RESET_BACKENDS:
        raise ValueError(
            f"run.reset_backend must be one of {RESET_BACKENDS}, got {value!r}"
        )
    return value


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
    _run(docker_cmd, capture=True)


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


# ---------------------------------------------------------------------------
# JWT + Engine API
# ---------------------------------------------------------------------------

_JWT_FALLBACK_SOURCES = (
    Path("/data/jwt.hex"),
    Path.home() / ".besu" / "jwt.hex",
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

def discover_tests(cfg: Config, filter_override: str | None, limit: int | None,
                   explicit: list[str] | None = None) -> list[str]:
    """Return ordered list of basenames present in BOTH setup/ and testing/ that match the filter.

    When `explicit` is given (e.g. an arbitrary multi-selection from the web
    UI via --tests-from), it overrides the glob filter and `tests.order`: only
    the named tests are kept, in the order given, intersected with the valid
    setup/testing pairs. `limit` still applies last.
    """
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
        if limit is not None and limit >= 0:
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


def run_sweep(cfg: Config, filter_override: str | None, limit: int | None,
              pick: bool, dry_run: bool, select: list[str] | None = None) -> int:
    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    log_root = cfg.run.log_dir / timestamp
    log = SweepLog(log_root)
    log.event(f"sweep start, log dir = {log_root}")

    tests = discover_tests(cfg, filter_override, limit, explicit=select)
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
    # JWT auto-provisioning: copy from a known fallback (/data/jwt.hex etc.)
    # or generate a fresh one if nothing is available. /tmp paths in
    # particular vanish across reboots, so this avoids "jwt secret missing"
    # being the first thing you see after a host restart.
    ensure_jwt_secret(cfg.besu.jwt_secret_path, log)

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

            # Each test runs end-to-end in ONE Besu container:
            #   reset overlay -> start Besu -> gas-bump -> funding ->
            #   setup -> testing -> stop Besu
            # No mid-flight restarts: every test sees the exact same warm-up
            # path before the measured block.
            for idx, name in enumerate(tests, start=1):
                log.event(f"[{idx}/{len(tests)}] {name}")

                # Roll on-disk state back to the pristine baseline and remount.
                # With the OverlayFS backend this wipes both overlay layers;
                # with the schelk backend it does an incremental dm-era restore.
                # With persist-prelude we instead wipe ONLY the test layer and
                # keep the baked gas-bump prelude.
                # Everything below writes into the freshly-reset state.
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

                # Build profiler sessions for this test (one per phase).
                # The runner brackets the LAST newPayload+FCU pair of each
                # phase, which corresponds to the heavy setup block and the
                # measured testing block respectively.
                setup_profiler: ProfilerSession | None = None
                testing_profiler: ProfilerSession | None = None
                if cfg.profile.enabled:
                    run_id = log.root.name  # the timestamp folder name
                    setup_profiler = ProfilerSession(
                        cfg.profile,
                        cfg.besu.container_name,
                        _profile_output_filename(run_id, idx, name, "setup", cfg.profile.output_format),
                        log.root,
                        log,
                    )
                    testing_profiler = ProfilerSession(
                        cfg.profile,
                        cfg.besu.container_name,
                        _profile_output_filename(run_id, idx, name, "testing", cfg.profile.output_format),
                        log.root,
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
                # runs get a -FAIL suffix to make them easy to spot. The
                # filename embeds a slug of the test name so runs/<ts>/ ls
                # is self-documenting once you have several tests.
                save_container_logs(
                    cfg.besu.container_name,
                    log.root / _besu_log_filename(idx, name, failed=not test_ok),
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
    ensure_jwt_secret(cfg.besu.jwt_secret_path, log)
    for spec in cfg.besu.extra_mounts:
        host = spec.split(":", 1)[0]
        if not host or not Path(host).exists():
            raise FileNotFoundError(
                f"besu.extra_mounts host path does not exist: {host!r} "
                f"(from spec {spec!r})."
            )
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
            ok = replay_file(cfg, secret, session, src, log, phase="prepare")
            if ok:
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
                out.mkdir(parents=True, exist_ok=True)
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
    ensure_jwt_secret(cfg.besu.jwt_secret_path, log)
    for spec in cfg.besu.extra_mounts:
        host = spec.split(":", 1)[0]
        if not host or not Path(host).exists():
            raise FileNotFoundError(
                f"besu.extra_mounts host path does not exist: {host!r} "
                f"(from spec {spec!r})."
            )
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
            ok = replay_file(cfg, secret, session, src, log, phase="prepare")
            if ok:
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
# reset_to_baseline, wait_for_engine, post_engine_line, _classify, _scan_lines,
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
    """Timed sibling of `replay_file`, used only by compare mode.

    Re-uses `_scan_lines`, `post_engine_line` and `_classify` unchanged. On
    top of the normal ok/fail bookkeeping it times every Engine-API call and
    returns the per-call latencies in milliseconds:

        {"newpayload_ms": [...], "fcu_ms": [...],
         "last_newpayload_ms": float | None, "total_newpayload_ms": float}

    The LAST newPayload latency is the headline number: in a
    testing/<name>.txt file it is the single measured heavy block, which is
    exactly what we want to compare between two Besu versions.

    `source_label` is the bucket name used for SweepLog counters; in compare
    mode we prefix it with the version label so each version keeps its own
    ok/fail tallies in summary.json.
    """
    label = file_path.name
    prefix = f"replay [{phase}] " if phase else "replay "
    log.event(f"{prefix}{label}")

    items = _scan_lines(file_path)
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

    Mirrors the per-test flow of `run_sweep` (reset overlay -> start Besu ->
    prelude -> setup -> testing -> stop) but without profiling, and with the
    testing phase timed via `_replay_file_measure`.
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
            for idx, name in enumerate(tests, start=1):
                log.event(f"[{label}] [{idx}/{len(tests)}] {name}")

                per_test_reset(cfg, log)
                start_besu(cfg.besu, log)
                started_container = True
                wait_for_engine(cfg.besu, secret, log)

                test_ok = True

                # Prelude (gas-bump then funding) — not timed, identical per test.
                for fname in cfg.input.prelude:
                    ok, _ = _replay_file_measure(
                        cfg, secret, session, cfg.input.dir / fname, log,
                        source_label=f"[{label}] {fname}", phase="prelude",
                    )
                    if not ok:
                        test_ok = False
                        if cfg.run.fail_fast:
                            log.event(f"[{label}] fail-fast tripped during prelude {fname}")
                            break

                testing_metrics: dict | None = None
                if test_ok or not cfg.run.fail_fast:
                    # Setup phase (state prep) — replayed, not part of the number.
                    s_ok, _ = _replay_file_measure(
                        cfg, secret, session, setup_dir / name, log,
                        source_label=f"[{label}] setup/{name}", phase="setup",
                    )
                    if not s_ok:
                        test_ok = False
                    # Testing phase — this is the measured block.
                    if s_ok or not cfg.run.fail_fast:
                        t_ok, testing_metrics = _replay_file_measure(
                            cfg, secret, session, testing_dir / name, log,
                            source_label=f"[{label}] testing/{name}", phase="testing",
                        )
                        if not t_ok:
                            test_ok = False

                besu_log_path = (
                    log.root / f"besu-{_safe_label(label)}-{idx:04d}-"
                               f"{_slugify(name)}{'' if test_ok else '-FAIL'}.log"
                )
                save_container_logs(cfg.besu.container_name, besu_log_path, log)
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

    tests = discover_tests(cfg, filter_override, limit, explicit=select)
    log.event(
        f"matched {len(tests)} tests "
        f"(filter={filter_override or cfg.tests.filter}, order={cfg.tests.order}, limit={limit})"
    )
    (log_root / "selected_tests.txt").write_text("\n".join(tests) + "\n")

    if dry_run:
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
    ensure_jwt_secret(cfg.besu.jwt_secret_path, log)
    for spec in cfg.besu.extra_mounts:
        host = spec.split(":", 1)[0]
        if not host or not Path(host).exists():
            raise FileNotFoundError(
                f"besu.extra_mounts host path does not exist: {host!r} "
                f"(from spec {spec!r})."
            )
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
    print(f"reset backend: {cfg.run.reset_backend}")

    # Gas-bump skip: when the snapshot already contains the gas-bumped blocks,
    # drop input.gas_bump_file from the prelude so it is not replayed. funding
    # (and the tests) then chain straight onto the snapshot tip, which must be
    # the gas-bump tip. Done once here so every downstream site (preflight,
    # sweep, compare) sees the already-filtered prelude.
    if args.skip_gas_bump:
        cfg.run.skip_gas_bump = True
    if args.persist_prelude:
        cfg.run.persist_prelude = True

    if cfg.run.skip_gas_bump and cfg.run.persist_prelude:
        print(
            "error: --skip-gas-bump and --persist-prelude are mutually exclusive. "
            "Both remove the gas-bump from the per-test prelude, but --skip-gas-bump "
            "expects a pre-bumped snapshot while --persist-prelude bakes the gas-bump "
            "into a persistent overlay layer at sweep start. Pick one.",
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
        cfg.input.prelude = [f for f in before
                             if Path(f).name != cfg.input.gas_bump_file]
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
