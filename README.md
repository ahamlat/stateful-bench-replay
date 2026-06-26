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

### Skip the gas-bump prelude (pre-bumped snapshots)

The prelude normally builds the chain bottom-up: `snapshot tip → gas-bump
(5000 blocks) → funding (1 block) → tests`, replayed before **every** test. If
your snapshot was captured **after** the gas-bump blocks were already applied
(its tip *is* the gas-bump tip), replaying `gas-bump.txt` is redundant. Skip it:

```bash
# CLI (overrides the config for this run)
./runBenchmark.sh --skip-gas-bump --filter '*sload_bloated*' --limit 1
```

```yaml
# or make it the default in config.yaml
run:
  skip_gas_bump: true
```

When set, the runner drops `input.gas_bump_file` (default `gas-bump.txt`) from
the prelude, so only the remaining entries (e.g. `funding.txt`) are replayed —
`funding` then chains straight onto the snapshot tip. It works with every mode
(`--compare`, `--profile`, both reset backends) and prints what it removed at
startup (`skip-gas-bump: omitting prelude file(s) ['gas-bump.txt'] …`).

> Use this **only** when the snapshot really contains the gas-bumped blocks. If
> it doesn't, `funding`/the tests fail with `SYNCING` (missing parent), because
> their `parentHash` points at the gas-bump tip. If your gas-bump file has a
> different name, set `input.gas_bump_file` to match.

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

The one-time setup has four phases: **prepare the disks**, **build the
tooling**, **`schelk init`**, then **configure + run**. Do them in order.

#### A. Prepare the disks (the part most people get wrong)

schelk needs **two equal-size raw block devices** plus a small DRAM ramdisk:

- **virgin** — holds the pristine Besu snapshot; Besu *never* writes it.
- **scratch** — equal size; Besu mounts and writes this. `init` clones virgin
  onto it, and each reset copies back only the blocks the test changed.
- **ramdisk** (`/dev/ram0`) — dm-era metadata. `schelk.sh` creates it with
  `modprobe brd` from `schelk.ramdisk_size_kb` if it is absent. Rule of thumb:
  ~4 GiB per ~1.7 TiB of volume at 4 KiB granularity.

The two devices can be **two whole disks** (best: virgin and scratch on
separate physical disks → no IO contention during the measured block) or **two
partitions on one disk** (fine for getting started; they share a spindle). Each
must be **larger than the snapshot** (`du -sh <snapshot>` to check).

Example: split one empty disk into two equal partitions, then load the snapshot
onto virgin. The snapshot's **contents must sit at the root** of the virgin
filesystem (i.e. `database/`, `caches/`, … directly under the mount), because
that filesystem *is* what Besu sees as its data dir.

```bash
# 1. Two equal partitions (THIS ERASES THE DISK). Or use two separate disks.
sudo wipefs -a /dev/nvmeXn1
sudo parted -s /dev/nvmeXn1 mklabel gpt
sudo parted -s /dev/nvmeXn1 mkpart virgin  ext4 0%   50%
sudo parted -s /dev/nvmeXn1 mkpart scratch ext4 50%  100%
sudo partprobe /dev/nvmeXn1            # -> nvmeXn1p1 (virgin), nvmeXn1p2 (scratch)

# 2. Filesystem on VIRGIN, then load the snapshot at the FS root.
sudo mkfs.ext4 -F -L besu-virgin /dev/nvmeXn1p1
sudo mkdir -p /mnt/virgin && sudo mount /dev/nvmeXn1p1 /mnt/virgin
#    ... rsync / download / restore the Besu data dir INTO /mnt/virgin so that
#        /mnt/virgin/database, /mnt/virgin/caches, ... exist (NOT /mnt/virgin/besu/...).
sudo ls -la /mnt/virgin               # sanity: datadir layout at the root
sync && sudo umount /mnt/virgin       # MUST be cleanly unmounted before init

# 3. Leave SCRATCH (nvmeXn1p2) raw and unmounted; init clones virgin onto it.
```

> NB: on AWS instance-store NVMe (e.g. i3en), all of this lives on **ephemeral**
> storage. A stop/terminate wipes it (redo mkfs + reload snapshot + `init`); a
> reboot keeps the data but invalidates incremental recovery — see Troubleshooting.

#### B. Build the tooling

```bash
# schelk itself (needs Rust >= 1.85; edition-2024). Build as your user, then
# copy into place — sudo sanitises PATH so ~/.cargo/bin is invisible to the helper.
git clone https://github.com/tempoxyz/schelk /tmp/schelk
cargo install --path /tmp/schelk --root "$HOME/.local"
sudo install -m 0755 "$HOME/.local/bin/schelk" /usr/local/bin/schelk

# thin-provisioning-tools >= 1.0 for a FAST per-test reset. The distro's 0.9.0
# `era_invalidate` makes every reset take minutes; 1.0+ (Rust) makes it seconds.
sudo apt-get install -y clang libudev-dev libdevmapper-dev pkg-config build-essential
git clone https://github.com/device-mapper-utils/thin-provisioning-tools /tmp/tpt
cd /tmp/tpt && cargo build --release          # -> target/release/pdata_tools
sudo install -m 0755 target/release/pdata_tools /usr/local/sbin/pdata_tools
for t in era_invalidate era_check era_dump era_restore; do
    sudo ln -sf /usr/local/sbin/pdata_tools /usr/local/sbin/$t   # /usr/local/sbin wins under sudo
done
```

#### C. `schelk init` (heavy, one-time: full virgin → scratch clone)

```bash
cd ~/stateful-bench-replay
sudo scripts/schelk.sh init \
    --bin /usr/local/bin/schelk \
    --virgin /dev/nvmeXn1p1 --scratch /dev/nvmeXn1p2 --ramdisk /dev/ram0 \
    --mount-point /data/besu-overlay/test/merged --fstype ext4 \
    --ramdisk-size-kb 8388608

# Allow passwordless sudo for the schelk helper (alongside overlay.sh + docker).
sudo tee -a /etc/sudoers.d/besu-bench >/dev/null <<EOF
$USER ALL=(root) NOPASSWD: /home/$USER/stateful-bench-replay/scripts/schelk.sh
EOF
```

#### D. Configure + run

Set the `schelk:` block in `config.yaml` (see `config.example.yaml`) to the same
devices, and — since a schelk box has no overlay snapshot dir — make schelk the
default so you do not have to pass the flag every time:

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
./runBenchmark.sh --reset-backend schelk --dry-run                       # expect "reset backend: schelk"
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
- **schelk reset is slow (`era_invalidate version 0.9.0 is slow`)**: the distro
  `thin-provisioning-tools` is the bottleneck — the per-test `restore` runs
  `era_invalidate` every time. Build `>= 1.0` (Rust) and install it ahead of the
  packaged copy (see [phase B](#b-build-the-tooling)); resets drop from minutes
  to seconds.
- **`schelk.sh status` segfaults / `bash: segfault ... in libc.so.6`**: an old
  `schelk.sh` shadowed the `schelk` binary with a same-named shell function and
  recursed (only when called by hand *without* `--bin`; the sweep always passes
  it). Fixed in the current script — refresh your checkout, or pass
  `--bin /usr/local/bin/schelk` on manual calls.
- **`overlay.sh: snapshot dir not found: /data/besu`** on a schelk box: the run
  fell back to the OverlayFS backend, whose snapshot directory no longer exists.
  Pass `--reset-backend schelk` or set `run.reset_backend: schelk` in the config.
- **Engine API timeout**: every host path in `besu.extra_mounts` must
  exist on the host; otherwise `docker run` silently creates an empty
  directory there and Besu fails before it logs anything.
- **JWT secret missing** is auto-handled: if `besu.jwt_secret_path`
  doesn't exist the runner copies from `/data/jwt.hex` or
  `~/.besu/jwt.hex`, and otherwise generates a fresh 32-byte hex secret
  in place. The same file is bind-mounted into the container so runner
  and Besu always agree.
