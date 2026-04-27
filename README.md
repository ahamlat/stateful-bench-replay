# Stateful replay benchmark

Replay engine-API JSON-RPC streams (newPayload/forkchoiceUpdated) against a
Besu snapshot mounted via OverlayFS so every sweep starts from the same chain
state.

Per selected test:

1. Reset the overlay (umount → wipe upper/work → remount). The chain on
   disk is exactly the snapshot again.
2. Start a Besu container with the overlay as its data dir.
3. Wait for the Engine API to come up.
4. Replay `gas-bump.txt`, `funding.txt`, `setup/<name>.txt`, then
   `testing/<name>.txt`, all in this one container.
5. Save the container's full log to `runs/<ts>/besu-<idx>.log` and stop
   the container.

So a run with N selected tests = N independent Besu containers, each
doing the full prelude + test flow from a clean snapshot.

## Per-test isolation

Every test sees the exact same warm-up: same snapshot, same gas-bump,
same funding, same Besu version, same JVM, same caches. There is no
shared state between tests, so the measured testing block always runs
against an identical chain head.

The cost is replaying the prelude (~5,000 gas-bump blocks + funding)
once per test. On this VM that is ~30s, dominated by Besu importing
gas-bump's empty blocks. If you select many tests at once, this will
add up — the trade-off is total isolation.

Run a single test with:

```bash
./runBenchmark.sh --filter '*test_single_opcode.py__test_sload_bloated*' --limit 1
```

## Besu command line

The runner builds the `docker run` invocation as:

```bash
sudo -n docker run -d --name <besu.container_name> --network host \
  -v <overlay_dir>/test/merged:<besu.container_data_path> \
  [--entrypoint <besu.entrypoint>] \
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

### `besu.entrypoint`

Some Besu images ship a wrapper script as their entrypoint that does
`chown -R besu:besu /opt/besu/data` before exec'ing Besu. On a multi-GB
Bonsai snapshot bind-mounted over OverlayFS, that recursive chown forces
millions of copy-ups and can sit silently for many minutes. The classic
symptom is "the container is running, `docker logs` is empty, eventually
the Engine API timeout fires" — and `top` on the host shows one or more
`chown` processes in `D` state.

Set `besu.entrypoint` to the real Besu launcher to skip the wrapper:

```yaml
besu:
  image: ethpandaops/besu:bal-devnet-2-with-prefetch
  entrypoint: /opt/besu/bin/besu
```

Leave it commented out for stock `hyperledger/besu:*` images, whose
default entrypoint is already the launcher.

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
./runBenchmark.sh --filter '*sload_bloated*' --limit 1   # first match alphabetically
./runBenchmark.sh --filter '*sload_bloated*' --pick      # interactive picker, run only the chosen one
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
`VALID`. The runner is strict on purpose: `SYNCING`/`ACCEPTED` are
recorded as failures because for deterministic replay against a prepped
snapshot they mean "Besu is missing the parent block" — which always
indicates a bug in the harness or the input data.

`runs/<ts>/besu-<idx>.log` is the full `docker logs` of each test's
Besu container, captured before the container was removed. Useful for
hunting state-root mismatches or timing the actual test block:

```bash
LATEST=$(ls -1d runs/* | tail -n1)
grep 'head =' "$LATEST/events.log"          # head before/after each phase
grep 'Imported #' "$LATEST/besu-0001.log"   # block-by-block import log
```

## Operational notes

- `run.py` shells out to `sudo -n docker ...` for every container call and
  to `sudo -n scripts/overlay.sh reset-all ...` for every
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
