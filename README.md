# Stateful replay benchmark

Replay engine-API JSON-RPC streams (newPayload/forkchoiceUpdated) against a
Besu snapshot mounted via OverlayFS so every sweep starts from the same chain
state.

Per sweep:

1. Reset the overlay (umount → wipe upper/work → remount).
2. Start a Besu container with the merged overlay as its data dir.
3. Wait for the Engine API to come up.
4. Replay each prelude file in order (`funding.txt`, `gas-bump.txt`).
5. For each selected test (basename matched by `tests.filter`): replay
   `setup/<name>.txt` then `testing/<name>.txt`.
6. Stop the container and write a summary.

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
openssl rand -hex 32 | sudo tee /data/jwt.hex >/dev/null
sudo chmod 644 /data/jwt.hex

# 4. Overlay scratch dir (one-time; reset/mount happen per sweep)
sudo mkdir -p /data/besu-overlay/{upper,work,merged}

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

- `besu.data_snapshot_dir`: `/data/besu` (the read-only lower).
- `besu.overlay_dir`: `/data/besu-overlay` (writes land in `upper/`).
- `besu.image`: the Amsterdam-capable Besu tag you pulled.
- `besu.jwt_secret_path`: `/data/jwt.hex`.
- `input.dir`: where you rsync'd the generated tests.
- `tests.filter`: glob over basenames; `"*"` runs all 1 136 tests.

## Run

```bash
# Validate config + see what would run, without touching the system.
python3 run.py --config config.yaml --dry-run

# Full sweep with all matched tests.
python3 run.py --config config.yaml

# Override the filter on the CLI (wins over tests.filter in YAML).
python3 run.py --config config.yaml --filter '*BALANCE*30M*'
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

- `run.py` runs `sudo -n scripts/overlay.sh reset ...` at the start of each
  sweep. The `-n` flag means the call must not prompt; that's why the
  sudoers allowlist above is required.
- The Besu container is started with `--network host` so the runner can hit
  `127.0.0.1:8551` directly. Change `besu.engine_url` and drop `--network
  host` from `start_besu` in `run.py` if you need port mapping instead.
- `run.stop_container_on_exit: false` leaves Besu running so you can poke
  at it after a sweep; the next sweep will tear it down anyway.
- Setting `run.reset_overlay: false` skips the wipe but still ensures the
  overlay is mounted, useful for resuming an interrupted sweep.

## Caveats / open assumptions

- All 1 136 tests run as forks off the gas-bump tip in a single Besu
  process. If your snapshot's storage format hits reorg-depth limits part
  way through a sweep, either bump the relevant Besu flag in
  `besu.extra_args` (e.g. `--bonsai-historical-block-limit=...`) or split
  the suite via `--filter` and run multiple sweeps. Per-test overlay reset
  is not implemented today; it would be a small change to `run_sweep`.
- The runner only logs failures (per the chosen "minimal" metrics mode).
  Adding per-call latency would be a few extra fields in `replay_file` and
  a CSV writer; the JSONL format makes this easy to bolt on later.
