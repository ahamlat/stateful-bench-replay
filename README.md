# Stateful replay benchmark

Replay engine-API JSON-RPC streams (newPayload/forkchoiceUpdated) against a
Besu snapshot mounted via OverlayFS so every sweep starts from the same chain
state.

Per sweep:

1. Reset the two-layer overlay (umount both → wipe upper/work → remount).
2. Start a Besu container with the test-layer merged overlay as its data dir.
3. Wait for the Engine API to come up.
4. Replay each prelude file in order (`gas-bump.txt`, `funding.txt`).
5. For each selected test (basename matched by `tests.filter`): replay
   `setup/<name>.txt` then `testing/<name>.txt`. The exact mechanics depend
   on `run.isolation` (see below).
6. Stop the container and write a summary.

## Isolation modes

`run.isolation` controls what happens between tests:

| Mode    | Per-test reset | Besu restarts? | Speed | Notes |
| ------- | -------------- | -------------- | ----- | ----- |
| `sweep` (default) | none | no  | fastest | All tests run in one Besu process. Tests share the canonical head; later tests see writes from earlier ones. |
| `restart` | wipe only the test overlay layer | yes | slow (~few s/test) | Strongest isolation. Prelude writes are preserved by the two-layer overlay so gas-bump+funding don't have to be replayed each time. |

The two-layer overlay makes `restart` cheap: the prelude writes live in
`<overlay_dir>/prelude/upper`, the test writes live in
`<overlay_dir>/test/upper`, and "reset the test layer" is just umount → `rm
-rf test/upper test/work` → remount. No copy of multi-GB chain state.

Override the YAML setting on the CLI:

```bash
./runBenchmark.sh --isolation restart --filter '*BALANCE*30M*'
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

```bash
# 1. Docker
sudo apt-get update && sudo apt-get install -y docker.io python3-pip rsync openssl
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
newgrp docker     # or log out / log back in

# 2. Python deps (use a venv if you prefer)
python3 -m pip install --user -r requirements.txt

# 3. JWT secret shared by Besu and the runner
#    The runner reads it from besu.jwt_secret_path (default /tmp/jwtsecret in
#    config.example.yaml) and bind-mounts it into the container via
#    besu.extra_mounts. The path here just has to match what you put in YAML.
openssl rand -hex 32 | sudo tee /tmp/jwtsecret >/dev/null
sudo chmod 644 /tmp/jwtsecret

# Place your genesis file too if besu.extra_args references one
# (the example uses --genesis-file=/tmp/genesis.json):
# sudo cp /path/to/genesis.json /tmp/genesis.json && sudo chmod 644 /tmp/genesis.json

# 4. Overlay scratch dirs (one-time; reset/mount happen per sweep).
#    The runner / overlay.sh will recreate these if missing, but pre-creating
#    saves a sudo prompt on first run.
sudo mkdir -p /data/besu-overlay/{prelude,test}/{upper,work,merged}

# 5. Install the overlay helper somewhere stable and allowlist it via sudoers
sudo install -m 0755 scripts/overlay.sh /opt/besu-bench/overlay.sh
echo "$USER ALL=(root) NOPASSWD: /opt/besu-bench/overlay.sh" \
    | sudo tee /etc/sudoers.d/besu-bench
sudo chmod 440 /etc/sudoers.d/besu-bench
```

If you prefer to keep the helper inside the repo checkout, point the sudoers
line at its absolute path instead of `/opt/besu-bench/overlay.sh`. The
`run.py` script invokes the copy that sits next to it
(`bench/stateful-replay/scripts/overlay.sh`) by default; symlink or replace
that path to match whatever you allowlisted in sudoers.

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
- `run.isolation`: `sweep` (default) or `restart`.

## Run

The easiest entry point is the wrapper script: it auto-creates a venv on
first call, installs `requirements.txt`, and forwards every argument to
`run.py`.

```bash
# First call also bootstraps ./venv/ from requirements.txt.
./runBenchmark.sh --dry-run                  # validate config + show selection
./runBenchmark.sh                            # full sweep with config.yaml
./runBenchmark.sh --filter '*BALANCE*30M*'   # filter override
./runBenchmark.sh -c staging.yaml            # different config
CONFIG=staging.yaml ./runBenchmark.sh        # same, via env var
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

- `run.py` runs `sudo -n scripts/overlay.sh reset-all ...` at the start of
  each sweep, and `sudo -n scripts/overlay.sh reset-test ...` between tests
  in `restart` mode. The `-n` flag means the call must not prompt; that's
  why the sudoers allowlist above is required.
- The Besu container is started with `--network host` so the runner can hit
  `127.0.0.1:8551` directly. Change `besu.engine_url` and drop `--network
  host` from `start_besu` in `run.py` if you need port mapping instead.
- `run.stop_container_on_exit: false` leaves Besu running so you can poke
  at it after a sweep; the next sweep will tear it down anyway.
- Setting `run.reset_overlay: false` skips the wipe but still ensures the
  overlay is mounted, useful for resuming an interrupted sweep.

## Caveats / open assumptions

- In `sweep` mode, all selected tests run as forks off the funding tip in
  a single Besu process. If your snapshot's storage format hits reorg-depth
  limits part way through a sweep, either bump the relevant Besu flag in
  `besu.extra_args` (e.g. `--bonsai-historical-block-limit=...`), switch
  to `--isolation restart`, or split the suite via `--filter`.
- In `restart` mode, expect ~5-15 s overhead per test for docker
  stop/start + Engine API readiness. The full 1 134-test sweep will take
  noticeably longer than `sweep` mode (1-2 minutes/test x 1134 ≈ hours).
  Use `--filter` aggressively or run `restart` only on the subset you care
  about.
- The runner only logs failures (per the chosen "minimal" metrics mode).
  Adding per-call latency would be a few extra fields in `replay_file` and
  a CSV writer; the JSONL format makes this easy to bolt on later.
