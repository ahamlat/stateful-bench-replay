import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import run


def config(root: Path, *, fixture_format: str = "stateful_engine"):
    return SimpleNamespace(
        input=SimpleNamespace(dir=root),
        tests=SimpleNamespace(
            format=fixture_format,
            fixtures_subdir="fixtures",
            setup_subdir="setup",
            testing_subdir="testing",
            filter="*",
            order="alphabetical",
        ),
    )


def payload(block_hash: str, *, np_version="5", fcu_version="3"):
    return {
        "params": [{"blockHash": block_hash, "parentHash": run.ZERO_HASH}],
        "newPayloadVersion": np_version,
        "forkchoiceUpdatedVersion": fcu_version,
    }


class StatefulFixtureInputTests(unittest.TestCase):
    def test_builds_new_payload_and_forkchoice_requests(self):
        block_hash = "0x" + "11" * 32

        lines = run._fixture_payload_requests(
            [payload(block_hash)], "fixture setupEngineNewPayloads"
        )

        self.assertEqual(len(lines), 2)
        new_payload, forkchoice = map(json.loads, lines)
        self.assertEqual(new_payload["method"], "engine_newPayloadV5")
        self.assertEqual(new_payload["params"][0]["blockHash"], block_hash)
        self.assertEqual(
            forkchoice["method"], "engine_forkchoiceUpdatedV3"
        )
        self.assertEqual(
            forkchoice["params"][0],
            {
                "headBlockHash": block_hash,
                "safeBlockHash": run.ZERO_HASH,
                "finalizedBlockHash": run.ZERO_HASH,
            },
        )
        self.assertIsNone(forkchoice["params"][1])

    def test_discovers_nested_fixture_dictionary_and_loads_phases(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture_dir = root / "fixtures" / "for_amsterdam_at_0100M" / "stateful"
            fixture_dir.mkdir(parents=True)
            setup_hash = "0x" + "22" * 32
            test_hash = "0x" + "33" * 32
            (fixture_dir / "sload.json").write_text(json.dumps({
                "sload-test": {
                    "setupEngineNewPayloads": [payload(setup_hash)],
                    "engineNewPayloads": [payload(test_hash)],
                }
            }))
            pre_run = root / "fixtures" / "pre_run"
            pre_run.mkdir()
            (pre_run / "ignored.json").write_text(json.dumps({
                "session": {"engineNewPayloads": [payload(setup_hash)]}
            }))
            cfg = config(root)

            names = run.discover_tests(cfg, None, None)
            setup, testing, source = run._stateful_test_requests(
                cfg, "sload-test"
            )

            self.assertEqual(names, ["sload-test"])
            self.assertEqual(len(setup), 2)
            self.assertEqual(len(testing), 2)
            self.assertIn("sload.json::sload-test", source)
            self.assertEqual(json.loads(testing[0])["params"][0]["blockHash"], test_hash)

    def test_auto_format_keeps_legacy_layout(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "setup").mkdir()
            (root / "testing").mkdir()
            cfg = config(root, fixture_format="auto")

            self.assertEqual(run._test_format(cfg), "legacy")

    def test_strict_replay_rejects_bad_pre_run_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = run.SweepLog(Path(tmp) / "logs")
            cfg = SimpleNamespace(run=SimpleNamespace(fail_fast=False))
            try:
                ok = run.replay_requests(
                    cfg, b"", None, ["not-json"], "pre-run.request", log,
                    require_all_valid=True,
                )
            finally:
                log.close()

            self.assertFalse(ok)

    def test_copies_missing_genesis_from_snapshot_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = root / "besu"
            snapshot.mkdir()
            src = root / "genesis.json"
            src.write_text('{"config":{}}')
            dest = root / "tmp" / "genesis.json"
            log = run.SweepLog(root / "logs")
            cfg = SimpleNamespace(
                input=SimpleNamespace(dir=root / "artifacts"),
                besu=SimpleNamespace(
                    data_snapshot_dir=snapshot,
                    jwt_secret_path=root / "jwt",
                    extra_args=["--genesis-file=/tmp/genesis.json"],
                    extra_mounts=[f"{dest}:/tmp/genesis.json:ro"],
                ),
            )
            (root / "artifacts").mkdir()
            try:
                run.ensure_genesis_file(cfg, log)
            finally:
                log.close()
            self.assertEqual(dest.read_text(), '{"config":{}}')


if __name__ == "__main__":
    unittest.main()
