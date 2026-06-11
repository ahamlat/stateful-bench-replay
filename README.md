# Stateful replay benchmark

Replay engine-API streams (`newPayload` + `forkchoiceUpdated`) against a
Besu snapshot, so every test starts from the same chain state. The per-test
state reset is pluggable (see [Reset backend](#reset-backend-overlayfs-vs-schelk)):
**OverlayFS** (default) or **schelk** (dm-era block-level rollback).

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
`<run-id>-0001-<slug>-setup.html` and
`<run-id>-0001-<slug>-testing.html` — `<run-id>` is the timestamp
folder, so the file is self-identifying once you copy it out of the
run dir. `<slug>` is a compact form of the test name with the boiler
plate parameters stripped (`fork_Amsterdam`, `benchmark_test`,
`initial_*_True`, etc.), so a filename typically looks like:

```
20260428-135916-0001-ext_account_query_warm-opcode_DELEGATECALL-benchmark_120M-testing.html
``` Open them in a browser. Default event is `wall`
(per-thread, no kernel tuning needed). Edit `profile.event: cpu` in
`config.yaml` for on-CPU flame graphs after
`sudo sysctl -w kernel.perf_event_paranoid=1`.

## 6. Compare two Besu versions (optional)

`--compare` runs the **whole selected suite twice** — once on image *x*,
then once on image *y* — and writes a self-contained HTML report diffing
the per-test testing-block time.

```bash
# Compare two images on a single test (quick smoke test of the mode).
./runBenchmark.sh --compare \
    --image-x ethpandaops/besu:bal-devnet-2 \
    --image-y ethpandaops/besu:bal-devnet-3 \
    --filter '*sload_bloated*' --limit 1

# x defaults to besu.image from config.yaml, so you usually only pass y:
./runBenchmark.sh --compare --image-y ethpandaops/besu:my-branch

# Give the columns friendly names instead of the image tag.
./runBenchmark.sh --compare \
    --image-y ethpandaops/besu:my-branch \
    --label-x main --label-y my-branch
```

For each version the runner does the normal per-test flow (reset overlay →
start Besu → prelude → `setup/<name>.txt` → `testing/<name>.txt` → stop). It
then compares **only the last imported (measured) block** of each test,
using Besu's *own* numbers parsed from the `Imported #…` log line:

- **gas used** — deterministic, so identical for *x* and *y* (a `⚠` flags
  any mismatch);
- **Mgas/s** — throughput, the headline metric;
- **latency** — Besu's block `exec` time, in ms.

Results land in `runs/<timestamp>-compare/`:

```
comparison.html         # sortable table: gas, Mgas/s (x/y/Δ/Δ%), latency (x/y/Δ)
comparison.json         # same data, machine-readable
selected_tests.txt      # the resolved test list (shared by both versions)
besu-<label>-NNNN-*.log # per-version, per-test Besu logs (source of the numbers)
events.log              # full timeline of both runs
summary.json            # per-version ok/fail tallies + comparison summary
```

Open `comparison.html` in any browser (no external assets). Rows are sorted
worst-throughput-regression first; green = *y* faster (higher Mgas/s) than
*x*, red = *y* slower. The summary cards show each version's aggregate
Mgas/s and the overall throughput delta.

`--compare --dry-run` resolves the test list and config without starting
any container, exactly like the normal `--dry-run`.

### Rebuild a report without re-running

The per-test Besu logs already contain every `Imported #` line, so you can
regenerate `comparison.html` / `comparison.json` from a finished run's logs
without replaying anything (handy after a report-format change, or to
upgrade a report produced by an older version of the script):

```bash
./runBenchmark.sh --rebuild-report runs/20260602-124513-compare
```

It reads `summary.json` (for the two image labels) and `selected_tests.txt`
(for the ordered test list), re-parses the `besu-<label>-NNNN-*.log` files,
and overwrites the two `comparison.*` artefacts in place. No config needed.

## Reset backend: OverlayFS vs schelk

Between every test the harness rolls Besu's on-disk state back to the pristine
snapshot. Two backends implement that reset; pick one with
`run.reset_backend` in `config.yaml` or `--reset-backend {overlayfs,schelk}`
on the CLI (the CLI wins). Besu's bind mount is identical either way
(`<overlay_dir>/test/merged`), so the same suite runs unchanged on both — handy
for A/B'ing the reset mechanism itself.

| | `overlayfs` (default) | `schelk` |
|---|---|---|
| Layer | OverlayFS over a snapshot **directory** (`scripts/overlay.sh`) | dm-era over a **block device** (`scripts/schelk.sh`) |
| Hot-path overhead | copy-up on first write + stacked-fs cost | ≈ none (no IO redirection, plain ext4 on NVMe) |
| Reset speed | instant (wipe `upper`/`work`) | seconds (copy back only written blocks) |
| Hardware | one dir on one disk | **two equal-size block devices + a DRAM ramdisk** |

Use `schelk` when you need production-faithful block-execution numbers
(`Mgas/s`, `exec` latency) without OverlayFS copy-up distorting the measured
block; use `overlayfs` when you can't dedicate two volumes + DRAM or you can
tolerate the overhead. See [tempoxyz/schelk](https://github.com/tempoxyz/schelk).

### Using the schelk backend

`scripts/schelk.sh` is a drop-in wrapper exposing the same verbs as
`overlay.sh` (`init` / `mount-all` / `reset-all` / `reset-test` / `umount-all`
/ `status`), mapped onto schelk's `init-from` / `mount` / `restore` / `recover`
/ `status`. Per test the sweep calls `reset-all` → `schelk restore`.

One-time VM setup (in addition to the steps in [section 1](#1-one-time-vm-setup)):

```bash
# 1. Two equal-size block devices; put the Besu snapshot on the VIRGIN one.
#    SCRATCH is what Besu mounts and writes to.

# 2. Build + install schelk (needs a Rust toolchain).
git clone https://github.com/tempoxyz/schelk /tmp/schelk
cargo install --path /tmp/schelk --root /usr/local   # -> /usr/local/bin/schelk

# 3. Adopt the virgin volume and clone it to scratch (heavy, one-time).
sudo scripts/schelk.sh init \
    --bin /usr/local/bin/schelk \
    --virgin /dev/nvme1n1 --scratch /dev/nvme2n1 --ramdisk /dev/ram0 \
    --mount-point /data/besu-overlay/test/merged --fstype ext4 \
    --ramdisk-size-kb 4194304

# 4. Allow passwordless sudo for the schelk helper (alongside overlay.sh + docker).
sudo tee -a /etc/sudoers.d/besu-bench >/dev/null <<EOF
$USER ALL=(root) NOPASSWD: /home/$USER/stateful-bench-replay/scripts/schelk.sh
EOF
```

Then configure the `schelk:` block in `config.yaml` (see
`config.example.yaml`) and run:

```bash
./runBenchmark.sh --reset-backend schelk --filter '*sload_bloated*' --limit 1
```

`--reset-backend schelk` also works with `--compare`. The runner aborts early
with a clear message if `schelk.virgin` / `schelk.scratch` / `schelk.ramdisk`
are missing from the config.

## Troubleshooting

- **`docker logs besu-bench` is empty for minutes after start**: the
  image's entrypoint is doing `chown -R` over the OverlayFS data dir.
  Set `besu.entrypoint: /opt/besu/bin/besu` to skip the wrapper.
- **`sudo -n docker version` fails**: re-do the sudoers step above.
- **`overlay.sh: unknown action: reset-all`**: stale checkout — `git pull`.
- **`schelk.sh: schelk binary not found`**: `sudo` sanitises `PATH`, so a
  `cargo install` binary in `~/.cargo/bin` is invisible. Set `schelk.bin` to an
  absolute path (e.g. install with `--root /usr/local`).
- **schelk `ramdisk ... not present`**: the dm-era metadata ramdisk is gone
  (e.g. after a reboot). It is recreated from `schelk.ramdisk_size_kb`; a reboot
  also invalidates incremental recovery, so run `sudo /usr/local/bin/schelk
  full-recover` once before the next sweep.
- **Engine API timeout**: every host path in `besu.extra_mounts` must
  exist on the host; otherwise `docker run` silently creates an empty
  directory there and Besu fails before it logs anything.
- **JWT secret missing** is auto-handled: if `besu.jwt_secret_path`
  doesn't exist the runner copies from `/data/jwt.hex` or
  `~/.besu/jwt.hex`, and otherwise generates a fresh 32-byte hex secret
  in place. The same file is bind-mounted into the container so runner
  and Besu always agree.
