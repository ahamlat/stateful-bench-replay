# Stateful replay benchmark

Replay engine-API JSON-RPC streams (newPayload/forkchoiceUpdated) against a
Besu snapshot mounted via OverlayFS so every sweep starts from the same chain
state.

Per sweep:

1. Reset the two-layer overlay (umount both → wipe upper/work → remount).
2. Start a Besu container with the test-layer merged overlay as its data dir.
3. Wait for the Engine API to come up.
4. Replay each prelude file in order (`gas-bump.txt`, `funding.txt`).
5. Stop Besu.
6. For each selected test (basename matched by `tests.filter`):
   - wipe only the test overlay layer (prelude writes are preserved),
   - start Besu, wait for the Engine API,
   - replay `setup/<name>.txt` then `testing/<name>.txt`,
   - stop Besu.
7. Write a summary.

## Per-test isolation (the only mode)

Every test starts from the snapshot + prelude state and from a freshly
restarted Besu. There is no "share one Besu across all tests" mode any
more — running tests sequentially in the same process produced state-root
mismatches because each test rewrites genesis-like contracts.

The two-layer overlay makes the per-test reset cheap: the prelude writes
live in `<overlay_dir>/prelude/upper`, the test writes live in
`<overlay_dir>/test/upper`, and "reset the test layer" is just umount →
`rm -rf test/upper test/work` → remount. No copy of multi-GB chain state.

Run a single test with:

```bash
./runBenchmark.sh --filter '*test_single_opcode.py__test_sload_bloated*' --limit 1
```

## Besu command line

The runner builds the `docker run` invocation as:

```bash
docker run -d --rm --name <besu.container_name> --network host \
  -v <overlay_dir>/test/merged:<besu.container_data_path> \
  -v <each entry from besu.extra_mounts> \
  <besu.image> \
  --data-path=<besu.container_data_path> \
  <each entry from besu.extra_args>
```

The runner injects only `--data-path=...` because that's the whole point of
overlay management. Everything else (engine RPC, JWT path, genesis, BAL
flags, metrics, etc.) is up to you in `besu.extra_args`. The JWT path you
put after `--engine-jwt-secret=` must match the in-container path you set
up with `besu.extra_mounts`.

## Layout

| File                                  | Purpose                                                       |
| ------------------------------------- | ------------------------------------------------------------- |
| [`run.py`](run.py)                    | Orchestrator (YAML, overlay, docker, Engine API, replay).     |
| [`config.example.yaml`](config.example.yaml) | Documented config template.                            |
| [`scripts/overlay.sh`](scripts/overlay.sh) | sudo helper for OverlayFS lifecycle.                     |
| [`requirements.txt`](requirements.txt) | Python dependencies (`PyYAML`, `requests`, `PyJWT`).         |

## AWS VM bootstrap (one-time)

Tested layout: Ubuntu 22.04+ or Amazon Linux 2023, x86_64, snapshot disk
mounted at `/data`, snapshot directory at `/data/besu`.

The runner is intentionally **sudo-driven**: every privileged call
(`docker`, `overlay.sh`) goes through `sudo -n`. You do **not** need to
join the `docker` group, and we don't install anything under `/opt`.

```bash
# 1. Packages
sudo apt-get update
sudo apt-get install -y docker.io python3-venv python3-pip rsync openssl
sudo systemctl enable --now docker

# 2. JWT secret + genesis file (must already exist where config.yaml points)
ls -lh /data/jwt.hex                                                          # already provisioned in your setup
ls -lh ~/stateful-bench-replay/genesis-perf-devnet-3-24358000-amsterdam-besu.json

# 3. Overlay scratch dirs (the helper recreates them if missing).
sudo mkdir -p /data/besu-overlay/{prelude,test}/{upper,work,merged}

# 4. Allowlist BOTH commands the runner shells out to, passwordless.
#    (The runner uses `sudo -n` so a missing entry fails fast instead of
#    prompting in the middle of a sweep.)
sudo tee /etc/sudoers.d/besu-bench >/dev/null <<EOF
$USER ALL=(root) NOPASSWD: /home/$USER/stateful-bench-replay/scripts/overlay.sh
$USER ALL=(root) NOPASSWD: /usr/bin/docker
EOF
sudo chmod 440 /etc/sudoers.d/besu-bench

# 5. Sanity check
sudo -n docker version --format '{{.Server.Version}}'
sudo -n /home/$USER/stateful-bench-replay/scripts/overlay.sh --help | head -3
```

If `docker` lives somewhere other than `/usr/bin/docker` on your distro,
adjust the second sudoers line accordingly (`command -v docker` will tell
you). The runner does not install anything under `/opt`, and pulling
updates is just `git pull` in your checkout.

### Sync test inputs

From your laptop:

```bash
rsync -a --info=progress2 \
    ~/IdeaProjects/reproduce-8038/generated-stateful-tests-stateful-perf-devnet-3-24656413846 \
    ubuntu@<vm>:/home/ubuntu/reproduce-8038/
```

### Pull a Besu image

The input data uses `engine_newPayloadV5` and Amsterdam-fork payload fields
(`blockAccessList`, `slotNumber`). Pin a Besu image that supports them:

```bash
docker pull hyperledger/besu:<tag>
```

## Configure

Copy and edit the template:

```bash
cp config.example.yaml config.yaml
$EDITOR config.yaml
```

Key fields:

- `besu.image`: the Amsterdam-capable Besu tag you pulled.
- `besu.data_snapshot_dir`: `/data/besu` (the read-only lower).
- `besu.overlay_dir`: `/data/besu-overlay` (writes land in `prelude/upper` and `test/upper`).
- `besu.container_data_path`: where the test/merged overlay is mounted in the container; matches the `--data-path` Besu uses.
- `besu.jwt_secret_path`: host path to the JWT (e.g. `/tmp/jwtsecret`); the runner reads it to sign Engine API requests.
- `besu.extra_mounts`: bind-mount JWT, genesis, etc. into the container (e.g. `/tmp/jwtsecret:/tmp/jwtsecret:ro`).
- `besu.extra_args`: full Besu command line (`--engine-jwt-secret=`, `--genesis-file=`, BAL flags, metrics, etc.).
- `input.dir`: where you rsync'd the generated tests.
- `input.prelude`: ordered list, must be `[gas-bump.txt, funding.txt]` for this dataset (gas-bump is built on the snapshot tip, funding is built on the gas-bump tip).
- `tests.filter`: glob over basenames; `"*"` runs all tests.

## Run

The easiest entry point is the wrapper script: it auto-creates a venv on
first call, installs `requirements.txt`, and forwards every argument to
`run.py`.

```bash
# First call also bootstraps ./venv/ from requirements.txt.
./runBenchmark.sh --dry-run                              # validate config + show selection
./runBenchmark.sh                                        # full sweep with config.yaml
./runBenchmark.sh --filter '*BALANCE*30M*'               # filter override
./runBenchmark.sh --filter '*sload_bloated*' --limit 1   # one test only (handy for debugging)
./runBenchmark.sh -c staging.yaml                        # different config
CONFIG=staging.yaml ./runBenchmark.sh                    # same, via env var
```

You can still call `run.py` directly if you prefer:

```bash
./venv/bin/python3 run.py --config config.yaml --dry-run
./venv/bin/python3 run.py --config config.yaml
./venv/bin/python3 run.py --config config.yaml --filter '*BALANCE*30M*'
```

Each invocation creates a timestamped subdir under `run.log_dir`:

```
runs/20260427-104500/
    events.log         # human-readable timeline
    failures.jsonl     # one JSON object per non-VALID / errored request
    summary.json       # per-file counters + run config snapshot
```

If `runs/<ts>/failures.jsonl` is empty, every replayed line returned
`VALID` (or `SYNCING`/`ACCEPTED` where allowed by the Engine API spec).

## Operational notes

- `run.py` shells out to `sudo -n docker ...` for every container call and
  to `sudo -n scripts/overlay.sh reset-all/reset-test ...` for every
  filesystem call. The `-n` flag means none of them is allowed to prompt;
  that's why both must be in the sudoers allowlist (see bootstrap step 4).
- If a sweep aborts immediately with `sudo -n docker version failed`, your
  user can't run `sudo docker` non-interactively yet. Re-do step 4 of the
  bootstrap and re-check with `sudo -n docker version`.
- If overlay.sh rejects an action with `unknown action: reset-all`, your
  checkout is on an older single-layer version of the script — `git pull`
  then re-run.
- The Besu container is started with `--network host` so the runner can hit
  `127.0.0.1:8551` directly. Change `besu.engine_url` and drop `--network
  host` from `start_besu` in `run.py` if you need port mapping instead.
- `run.stop_container_on_exit: false` leaves Besu running so you can poke
  at it after a sweep; the next sweep will tear it down anyway.
- Setting `run.reset_overlay: false` skips the wipe but still ensures the
  overlay is mounted, useful for resuming an interrupted sweep.

## Caveats / open assumptions

- Expect ~5-15 s overhead per test for docker stop/start + Engine API
  readiness on top of the actual block replay time. The full 1 134-test
  suite is therefore long-running (≈ 1-2 minutes/test × 1134 → hours).
  Use `--filter` aggressively, or use `--limit N` to cap the run while
  iterating.
- The runner only logs failures (per the chosen "minimal" metrics mode).
  Adding per-call latency would be a few extra fields in `replay_file` and
  a CSV writer; the JSONL format makes this easy to bolt on later.
