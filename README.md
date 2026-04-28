# Stateful replay benchmark

Replay engine-API streams (`newPayload` + `forkchoiceUpdated`) against a
Besu snapshot mounted via OverlayFS, so every test starts from the same
chain state.

For each selected test, one Besu container does the full flow end-to-end:

```
reset overlay → start Besu → gas-bump.txt → funding.txt
              → setup/<name>.txt → testing/<name>.txt → stop Besu
```

## 1. One-time VM setup

Ubuntu 22.04+, x86_64, snapshot at `/data/besu`, JWT at `/data/jwt.hex`.

```bash
sudo apt-get install -y docker.io python3-venv python3-pip rsync
sudo systemctl enable --now docker
sudo mkdir -p /data/besu-overlay/{prelude,test}/{upper,work,merged}

# Allow passwordless sudo for the two commands the runner uses.
sudo tee /etc/sudoers.d/besu-bench >/dev/null <<EOF
$USER ALL=(root) NOPASSWD: /home/$USER/stateful-bench-replay/scripts/overlay.sh
$USER ALL=(root) NOPASSWD: /usr/bin/docker
EOF
sudo chmod 440 /etc/sudoers.d/besu-bench

# Sanity check
sudo -n docker version --format '{{.Server.Version}}'
```

Pull a Besu image that supports `engine_newPayloadV5` + Amsterdam-fork
fields (`blockAccessList`, `slotNumber`):

```bash
sudo docker pull ethpandaops/besu:bal-devnet-2-with-prefetch
```

Sync test inputs from your laptop:

```bash
rsync -a ~/IdeaProjects/reproduce-8038/generated-stateful-tests-stateful-perf-devnet-3-24656413846/ \
        <vm>:/home/<user>/reproduce-8038/
```

Place the genesis file in the repo root:
`genesis-perf-devnet-3-24358000-amsterdam-besu.json`.

## 2. Configure

```bash
cd ~/stateful-bench-replay
cp config.example.yaml config.yaml
$EDITOR config.yaml          # set besu.image, input.dir, paths to JWT + genesis
```

## 3. Run

```bash
# Validate everything without touching the system.
./runBenchmark.sh --dry-run

# Run a single test (heaviest signal, ~1 min on the dev VM).
./runBenchmark.sh --filter '*sload_bloated*' --limit 1

# Pick interactively from a filter that matches several tests.
./runBenchmark.sh --filter '*BALANCE*' --pick

# Full sweep (long-running: ~1-2 min/test × 1134 tests).
./runBenchmark.sh
```

First call bootstraps `./venv/` from `requirements.txt` automatically.

## 4. Inspect results

Every run creates `runs/<timestamp>/`:

```
events.log              # human-readable timeline (per-phase chain head, etc.)
failures.jsonl          # one JSON object per non-VALID response (empty == all good)
summary.json            # per-file ok/fail counters
besu-0001-<slug>.log    # full `docker logs` of test #1's Besu container
selected_tests.txt      # the resolved test list
```

The `<slug>` in artefact filenames is a sanitised version of the test
name, so `ls runs/<ts>/` is self-documenting once you've run several
tests.

Quick checks:

```bash
LATEST=$(ls -1d runs/* | tail -n1)
grep 'head =' "$LATEST/events.log"             # chain head before/after each phase
grep 'Imported #' "$LATEST"/besu-0001-*.log    # block-by-block import timing
```

`SYNCING`/`ACCEPTED` responses are recorded as failures on purpose: for
deterministic replay against a prepped snapshot they mean Besu is missing
the parent block, which is always a harness bug.

## 5. Profile a test (optional)

```bash
# One-time: install async-profiler 4.4 into ~/async-profiler
scripts/install-async-profiler.sh

# Run with --profile to flame-graph the heavy setup block + the testing block.
./runBenchmark.sh --filter '*sload_bloated*' --limit 1 --profile
```

Output flame graphs land in the run dir as
`profile-0001-<slug>-setup.html` and
`profile-0001-<slug>-testing.html` — open them in a browser. Default
event is `wall` (per-thread, no kernel tuning needed). Edit
`profile.event: cpu` in `config.yaml` for on-CPU flame graphs after
`sudo sysctl -w kernel.perf_event_paranoid=1`.

## Troubleshooting

- **`docker logs besu-bench` is empty for minutes after start**: the
  image's entrypoint is doing `chown -R` over the OverlayFS data dir.
  Set `besu.entrypoint: /opt/besu/bin/besu` to skip the wrapper.
- **`sudo -n docker version` fails**: re-do the sudoers step above.
- **`overlay.sh: unknown action: reset-all`**: stale checkout — `git pull`.
- **Engine API timeout**: every host path in `besu.extra_mounts` must
  exist on the host; otherwise `docker run` silently creates an empty
  directory there and Besu fails before it logs anything.
- **JWT secret missing** is auto-handled: if `besu.jwt_secret_path`
  doesn't exist the runner copies from `/data/jwt.hex` or
  `~/.besu/jwt.hex`, and otherwise generates a fresh 32-byte hex secret
  in place. The same file is bind-mounted into the container so runner
  and Besu always agree.
