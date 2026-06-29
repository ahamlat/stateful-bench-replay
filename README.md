# Stateful replay benchmark

Replay engine-API streams (`newPayload` + `forkchoiceUpdated`) against a Besu
snapshot, so every test starts from the exact same chain state. For each
selected test one Besu container does the whole flow end-to-end:

```
reset baseline → start Besu → gas-bump → funding → setup/<name> → testing/<name> → stop Besu
```

The only thing that changes between the two supported setups is **how the
per-test state reset works**:

| | **OverlayFS** (default) | **schelk** |
|---|---|---|
| Layer | OverlayFS over a snapshot **directory** | dm-era over a **block device** |
| Hot-path overhead | copy-up on first write + stacked-fs cost | ≈ none (plain ext4 on NVMe) |
| Reset | instant (wipe the overlay) | seconds (copy back only written blocks) |
| Hardware | one directory on one disk | **two equal-size block devices + a DRAM ramdisk** |
| Use it when | you can't dedicate two volumes + DRAM | you need production-faithful `Mgas/s` / `exec` numbers |

Besu's bind mount is `<overlay_dir>/test/merged` either way, so the **same
suite runs unchanged on both backends** — handy for A/B'ing the reset mechanism
itself. See [tempoxyz/schelk](https://github.com/tempoxyz/schelk).

---

## All flags

Everything is driven by `./runBenchmark.sh`, a thin wrapper that bootstraps
`./venv/` on first use and forwards every argument to `run.py`.

### Selecting what to run

| Flag | What it does |
|---|---|
| `--filter, -f '<glob>'` | Run only tests whose name matches the glob (e.g. `'*sload_bloated*'`). |
| `--limit, -n N` | Run at most `N` tests after filtering (`--limit 1` for a single test). |
| `--pick, -p` | List the matched tests and prompt to pick exactly one (interactive). |
| `--tests-from FILE` | Run exactly the test basenames listed in `FILE` (one per line), in that order. Overrides `--filter`/`tests.order`. Used by the [web console](#web-console-ui) to run an arbitrary multi-selection. |
| `--dry-run` | Resolve config + the test list and exit without touching the system. |
| `--config, -c FILE` | Use a different config file (default `config.yaml`). |

### Choosing the reset backend

| Flag | What it does |
|---|---|
| `--reset-backend {overlayfs,schelk}` | Override `run.reset_backend`. The CLI wins. |

### Skipping the gas-bump (works on **both** backends)

The prelude normally replays `gas-bump` (5000 blocks) before **every** test,
which is expensive. These flags remove it. See
[Skipping the gas-bump](#skipping-the-gas-bump) for details.

| Flag | What it does |
|---|---|
| `--prepare-baseline` | One-time: bake the gas-bump into the baseline. OverlayFS → new pre-bumped snapshot dir; schelk → `promote`s it onto the virgin device. Runs no tests. |
| `--baseline-out DIR` | Where `--prepare-baseline` writes (OverlayFS only; default `<data_snapshot_dir>-bumped`). |
| `--skip-gas-bump` (`--no-gas-bump`) | Drop the gas-bump and run against the pre-bumped baseline. Pairs with `--prepare-baseline`. |
| `--persist-prelude` | **OverlayFS only.** Zero-setup alternative: bake the gas-bump once into a persistent overlay layer at sweep start, keep it across per-test resets. |

### Comparing two Besu versions

| Flag | What it does |
|---|---|
| `--compare` | Run the whole suite twice (image *x*, then *y*) and emit an HTML diff of testing-block times. |
| `--image-x IMG` | First/baseline image (defaults to `besu.image`). |
| `--image-y IMG` | Second/candidate image (required with `--compare`). |
| `--label-x NAME` / `--label-y NAME` | Friendly column names (default: the image tag). |
| `--rebuild-report RUN_DIR` | Regenerate `comparison.html/json` from a finished run's logs, no replay. |

### Profiling

| Flag | What it does |
|---|---|
| `--profile` | Flame-graph the heavy setup block + the measured testing block (async-profiler). |
| `--jfr-all` | Enable profiling and capture CPU/wall + allocation + lock contention into one JFR per phase. |

---

## Testing with OverlayFS (default)

### One-time VM setup

Ubuntu 22.04+, x86_64, snapshot at `/data/besu`, JWT at `/data/jwt.hex`.

```bash
sudo apt-get install -y docker.io python3-venv python3-pip rsync
sudo systemctl enable --now docker
sudo mkdir -p /data/besu-overlay/{prelude,test}/{upper,work,merged}

# Passwordless sudo for the two commands the runner uses.
sudo tee /etc/sudoers.d/besu-bench >/dev/null <<EOF
$USER ALL=(root) NOPASSWD: /home/$USER/stateful-bench-replay/scripts/overlay.sh
$USER ALL=(root) NOPASSWD: /usr/bin/docker
EOF
sudo chmod 440 /etc/sudoers.d/besu-bench

# Pull a Besu image that supports engine_newPayloadV5 + Amsterdam fields.
sudo docker pull ethpandaops/besu:bal-devnet-2-with-prefetch

# Sync the test inputs and place the genesis file in the repo root.
rsync -a ~/IdeaProjects/.../generated-stateful-tests-.../ <vm>:/home/<user>/reproduce-8038/
```

### Configure + run

```bash
cd ~/stateful-bench-replay
cp config.example.yaml config.yaml
$EDITOR config.yaml          # set besu.image, input.dir, JWT + genesis paths

./runBenchmark.sh --dry-run                          # validate, touch nothing
./runBenchmark.sh --filter '*sload_bloated*' --limit 1   # single test (~1 min)
./runBenchmark.sh --filter '*BALANCE*' --pick        # pick interactively
./runBenchmark.sh                                    # full sweep
```

---

## Testing with schelk

schelk swaps the OverlayFS directory for a **block-level** rollback, so the
measured block runs against a plain ext4 on real NVMe (no copy-up distortion).
Set it up in four ordered phases.

### A. Prepare the disks

schelk needs **two equal-size raw block devices** plus a small DRAM ramdisk:

- **virgin** — holds the pristine Besu snapshot; Besu never writes it.
- **scratch** — equal size; Besu mounts and writes this.
- **ramdisk** (`/dev/ram0`) — dm-era metadata (~4 GiB per ~1.7 TiB at 4 KiB).

The snapshot's **contents must sit at the root** of the virgin filesystem
(`database/`, `caches/`, … directly under the mount — that filesystem *is*
Besu's data dir).

```bash
# Two equal partitions (THIS ERASES THE DISK). Or use two separate disks.
sudo wipefs -a /dev/nvmeXn1
sudo parted -s /dev/nvmeXn1 mklabel gpt
sudo parted -s /dev/nvmeXn1 mkpart virgin  ext4 0%   50%
sudo parted -s /dev/nvmeXn1 mkpart scratch ext4 50%  100%
sudo partprobe /dev/nvmeXn1            # -> nvmeXn1p1 (virgin), nvmeXn1p2 (scratch)

# Filesystem on VIRGIN, then load the snapshot at the FS root.
sudo mkfs.ext4 -F -L besu-virgin /dev/nvmeXn1p1
sudo mkdir -p /mnt/virgin && sudo mount /dev/nvmeXn1p1 /mnt/virgin
#   ... rsync / restore the Besu data dir INTO /mnt/virgin so /mnt/virgin/database, ... exist.
sync && sudo umount /mnt/virgin       # MUST be cleanly unmounted before init

# Leave SCRATCH (nvmeXn1p2) raw and unmounted; init clones virgin onto it.
```

> On AWS instance-store NVMe (e.g. i3en) this lives on **ephemeral** storage: a
> stop/terminate wipes it; a reboot keeps the data but invalidates incremental
> recovery (run `sudo schelk full-recover` once — see Troubleshooting).

### B. Build the tooling

```bash
# schelk (needs Rust >= 1.85). Build as your user, then copy into place
# (sudo sanitises PATH so ~/.cargo/bin is invisible to the helper).
git clone https://github.com/tempoxyz/schelk /tmp/schelk
cargo install --path /tmp/schelk --root "$HOME/.local"
sudo install -m 0755 "$HOME/.local/bin/schelk" /usr/local/bin/schelk

# thin-provisioning-tools >= 1.0 — the distro's 0.9.0 makes every reset take
# minutes; 1.0+ (Rust) makes it seconds.
sudo apt-get install -y clang libudev-dev libdevmapper-dev pkg-config build-essential
git clone https://github.com/device-mapper-utils/thin-provisioning-tools /tmp/tpt
cd /tmp/tpt && cargo build --release
sudo install -m 0755 target/release/pdata_tools /usr/local/sbin/pdata_tools
for t in era_invalidate era_check era_dump era_restore; do
    sudo ln -sf /usr/local/sbin/pdata_tools /usr/local/sbin/$t
done
```

### C. `schelk init` (heavy, one-time virgin → scratch clone)

```bash
cd ~/stateful-bench-replay
sudo scripts/schelk.sh init \
    --bin /usr/local/bin/schelk \
    --virgin /dev/nvmeXn1p1 --scratch /dev/nvmeXn1p2 --ramdisk /dev/ram0 \
    --mount-point /data/besu-overlay/test/merged --fstype ext4 \
    --ramdisk-size-kb 8388608

# Passwordless sudo for the schelk helper (alongside overlay.sh + docker).
sudo tee -a /etc/sudoers.d/besu-bench >/dev/null <<EOF
$USER ALL=(root) NOPASSWD: /home/$USER/stateful-bench-replay/scripts/schelk.sh
EOF
```

### D. Configure + run

Set the `schelk:` block in `config.yaml` to the same devices, and make schelk
the default so you don't pass the flag every time:

```yaml
run:
  reset_backend: schelk
schelk:
  bin: /usr/local/bin/schelk
  virgin:  /dev/nvmeXn1p1
  scratch: /dev/nvmeXn1p2
  ramdisk: /dev/ram0
  fstype: ext4
  ramdisk_size_kb: 8388608
```

```bash
./runBenchmark.sh --reset-backend schelk --dry-run                    # expect "reset backend: schelk"
./runBenchmark.sh --reset-backend schelk --filter '*sload_bloated*' --limit 1
```

The runner aborts early with a clear message if `schelk.virgin` / `scratch` /
`ramdisk` are missing from the config.

---

## Skipping the gas-bump

The gas-bump (5000 blocks) is replayed before **every** test — expensive. There
are two ways to avoid that, and `--skip-gas-bump` works on **both backends**.

### `--prepare-baseline` + `--skip-gas-bump` (both backends)

Bake the gas-bump into the baseline **once**, then drop it from every run with a
single flag. The mechanism differs per backend but the workflow is identical:

```bash
# 1. Build the pre-bumped baseline once (runs no tests).
./runBenchmark.sh --prepare-baseline                       # OverlayFS or schelk

# 2. Add --skip-gas-bump to any run — only `funding` replays per test.
./runBenchmark.sh --skip-gas-bump --filter '*sload_bloated*' --limit 1
```

- **OverlayFS** — `--prepare-baseline` rsyncs a **new** pre-bumped snapshot dir
  (`besu.bumped_snapshot_dir`, default `<data_snapshot_dir>-bumped`), leaving the
  original untouched. `--skip-gas-bump` then uses that dir as the baseline, so
  adding/removing the flag toggles between the pristine and pre-bumped baselines.
- **schelk** — `--prepare-baseline` replays the gas-bump onto scratch and
  `schelk promote`s it **onto the virgin device in place**. `--skip-gas-bump`
  then rolls back to that gas-bumped virgin each test. There is only one
  baseline, so this **overwrites** the pristine virgin: to go back, reload the
  snapshot and re-run `schelk init`.

> If the baseline does **not** actually contain the gas-bumped blocks,
> `funding`/the tests fail with `SYNCING` (missing parent). If your gas-bump
> file has a different name, set `input.gas_bump_file` to match.

### `--persist-prelude` (OverlayFS only)

A zero-setup alternative — no `--prepare-baseline`, no second snapshot. It bakes
the gas-bump once into a persistent prelude overlay layer at sweep start, then
wipes only the test layer per test (`reset-test`).

```bash
./runBenchmark.sh --persist-prelude --filter '*sload_bloated*' --limit 1
```

`--skip-gas-bump` and `--persist-prelude` are mutually exclusive. For schelk,
use `--prepare-baseline` + `--skip-gas-bump` instead.

---

## Comparing two Besu versions (`--compare`)

`--compare` runs the **whole selected suite twice** — once on image *x*, then on
image *y* — and writes a self-contained HTML report diffing the per-test
testing-block time. It works with **both backends** and with the gas-bump-skip
flags.

```bash
# Compare two images on a single test.
./runBenchmark.sh --compare \
    --image-x ethpandaops/besu:bal-devnet-2 \
    --image-y ethpandaops/besu:bal-devnet-3 \
    --filter '*sload_bloated*' --limit 1

# x defaults to besu.image, so you usually only pass y, plus friendly labels.
./runBenchmark.sh --compare --image-y ethpandaops/besu:my-branch \
    --label-x main --label-y my-branch

# Compare WITHOUT the gas-bump (after one --prepare-baseline). Same on schelk.
./runBenchmark.sh --compare --skip-gas-bump --image-y ethpandaops/besu:my-branch
./runBenchmark.sh --compare --skip-gas-bump --reset-backend schelk \
    --image-y ethpandaops/besu:my-branch
```

For each version the runner does the normal per-test flow, then compares **only
the last imported (measured) block**, using Besu's own `Imported #…` numbers:

- **gas used** — deterministic, identical for *x* and *y* (a `⚠` flags any mismatch);
- **Mgas/s** — throughput, the headline metric;
- **latency** — Besu's block `exec` time, in ms.

Results land in `runs/<timestamp>-compare/`:

```
comparison.html   # sortable table: gas, Mgas/s (x/y/Δ/Δ%), latency (x/y/Δ)
comparison.json   # same data, machine-readable
selected_tests.txt
besu-<label>-NNNN-*.log   # per-version, per-test Besu logs (source of the numbers)
events.log
summary.json      # per-version ok/fail tallies + comparison summary
```

Open `comparison.html` in any browser (no external assets). Rows sort
worst-throughput-regression first; green = *y* faster, red = *y* slower.
`--compare --dry-run` resolves the test list without starting a container.

---

## Web console (UI)

A spamoor-style web console lets you browse the test corpus, run an arbitrary
selection, compare two Besu versions on that selection, and read the results —
all from a browser. It runs **on the VM** (where `run.py`, the snapshot and
docker live) and you reach it over an SSH port-forward.

```bash
# On the VM (reuses the same venv as runBenchmark.sh):
./runWebUI.sh                       # serves http://127.0.0.1:8765
./runWebUI.sh --port 9000           # different port
CONFIG=staging.yaml ./runWebUI.sh   # different config

# On your laptop:
ssh -N -L 8765:127.0.0.1:8765 <vm>
# then open http://127.0.0.1:8765
```

It binds to `127.0.0.1` by default (only reachable through the tunnel). Use
`--host 0.0.0.0` only if you really want it on the network.

**What it does**

- **Tests** — a faceted explorer over all ~550 tests. Facets (file, opcode,
  gas, value_sent, account_mode, cache_strategy, fork, …) are parsed from the
  test names; filter by clicking chips or typing in the search box
  (`opcode=CALL gas=160M`). A performance heatmap colours each test by its
  latest known Mgas/s. Tick the tests you want (or "select filtered").
- **Run** — launch the selected tests as a normal sweep on the configured
  image, with the usual toggles (reset backend, skip-gas-bump, persist-prelude,
  profile, dry-run).
- **Compare** — run the selection twice (image *x* vs *y*) and get the same
  diff `--compare` produces.
- **Runs & Results** — every `runs/<ts>/` is listed; open one to see the
  comparison table (colour-coded Δ Mgas/s + latency) or the per-test sweep
  status, the events tail, failures, and links to download the raw
  `comparison.html` / Besu logs.
- **Jobs** — live log tail of running/finished launches, with a cancel button.

Under the hood the UI shells out to `run.py` and passes the multi-selection via
the new `--tests-from FILE` flag (one test basename per line). Nothing is
re-implemented: discovery, replay, comparison and log parsing all come from
`run.py`, so the console and the CLI always agree.

> The console reads results straight from `runs/` and the saved Besu logs, so
> it works on completed runs too — including ones you launched from the CLI.

---

## Inspecting results

Every run creates `runs/<timestamp>/`:

```
events.log              # human-readable timeline (per-phase chain head, etc.)
failures.jsonl          # one JSON object per non-VALID response (empty == all good)
summary.json            # per-file ok/fail counters
besu-0001-<slug>.log    # full `docker logs` of test #1's Besu container
selected_tests.txt      # the resolved test list
```

The `<slug>` is a sanitised test name, so `ls runs/<ts>/` is self-documenting.

```bash
LATEST=$(ls -1d runs/* | tail -n1)
grep 'head =' "$LATEST/events.log"             # chain head before/after each phase
grep 'Imported #' "$LATEST"/besu-0001-*.log    # block-by-block import timing
```

`SYNCING`/`ACCEPTED` responses are recorded as failures on purpose: for
deterministic replay they mean Besu is missing the parent block — a harness bug.

---

## Profiling (optional)

```bash
scripts/install-async-profiler.sh                       # one-time
./runBenchmark.sh --filter '*sload_bloated*' --limit 1 --profile
```

Flame graphs land in the run dir as `<run-id>-NNNN-<slug>-setup.html` and
`…-testing.html`. Default event is `wall` (no kernel tuning). Set
`profile.event: cpu` (after `sudo sysctl -w kernel.perf_event_paranoid=1`) for
on-CPU graphs, or pass `--jfr-all` for CPU + allocation + lock in one JFR.

---

## Troubleshooting

- **`docker logs besu-bench` is empty for minutes after start**: the image's
  entrypoint is doing `chown -R` over the data dir. Set
  `besu.entrypoint: /opt/besu/bin/besu` to skip the wrapper.
- **`sudo -n docker version` fails**: re-do the sudoers step.
- **`overlay.sh: unknown action: …`** / *"overlay.sh rejected this action"*:
  stale checkout — refresh the system copy:
  `sudo install -m 0755 scripts/overlay.sh /usr/local/sbin/besu-overlay.sh`.
- **`schelk.sh: schelk binary not found`**: `sudo` sanitises `PATH`. Set
  `schelk.bin` to an absolute path (install with `--root /usr/local`).
- **schelk `ramdisk … not present`**: gone after a reboot. It is recreated from
  `schelk.ramdisk_size_kb`, but a reboot also invalidates incremental recovery —
  run `sudo /usr/local/bin/schelk full-recover` once before the next sweep.
- **schelk reset is slow (`era_invalidate version 0.9.0 is slow`)**: build
  `thin-provisioning-tools >= 1.0` (see [phase B](#b-build-the-tooling)).
- **`overlay.sh: snapshot dir not found: /data/besu` on a schelk box**: the run
  fell back to OverlayFS. Pass `--reset-backend schelk` or set
  `run.reset_backend: schelk`.
- **Engine API timeout**: every host path in `besu.extra_mounts` must exist;
  otherwise `docker run` silently creates an empty dir and Besu fails with no logs.
- **JWT secret missing** is auto-handled: the runner copies from `/data/jwt.hex`
  or `~/.besu/jwt.hex`, else generates a fresh 32-byte hex secret in place.
