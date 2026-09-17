import json
import os
import tempfile
import unittest
import unittest.mock
from io import StringIO
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
            match_chain_head=True,
        ),
    )


def payload(block_hash: str, *, np_version="5", fcu_version="3", parent=None):
    return {
        "params": [{"blockHash": block_hash, "parentHash": parent or run.ZERO_HASH}],
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

    def test_filters_fixtures_to_pre_run_head(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            head = "0x" + "aa" * 32
            other = "0x" + "bb" * 32
            d1 = root / "fixtures" / "for_amsterdam_at_0100M" / "stateful"
            d2 = root / "fixtures" / "for_amsterdam_at_0120M" / "stateful"
            d1.mkdir(parents=True)
            d2.mkdir(parents=True)
            (d1 / "sstore.json").write_text(json.dumps({
                "sstore-jochemnet": {
                    "setupEngineNewPayloads": [payload("0x" + "11" * 32, parent=head)],
                    "engineNewPayloads": [payload("0x" + "22" * 32)],
                }
            }))
            (d2 / "sstore.json").write_text(json.dumps({
                "sstore-genesis": {
                    "setupEngineNewPayloads": [payload("0x" + "33" * 32, parent=other)],
                    "engineNewPayloads": [payload("0x" + "44" * 32)],
                }
            }))
            cfg = config(root)
            names = run.discover_tests(cfg, "*sstore*", None)
            log = run.SweepLog(root / "logs")
            try:
                kept = run.filter_stateful_tests_for_head(cfg, names, head, log)
                limited = run._apply_limit(kept, 1)
            finally:
                log.close()

            self.assertEqual(set(names), {"sstore-jochemnet", "sstore-genesis"})
            self.assertEqual(kept, ["sstore-jochemnet"])
            self.assertEqual(limited, ["sstore-jochemnet"])

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

    def test_replay_logs_and_counts_syncing_payloads(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = run.SweepLog(Path(tmp) / "logs")
            cfg = SimpleNamespace(
                run=SimpleNamespace(fail_fast=False, request_timeout_s=1),
                besu=SimpleNamespace(engine_url="http://127.0.0.1:8551"),
            )
            line = json.dumps({
                "jsonrpc": "2.0",
                "method": "engine_newPayloadV5",
                "params": [{}],
            })

            def fake_post(_cfg, _secret, _session, _raw):
                return 200, {"result": {"status": "SYNCING"}}, None

            original = run.post_engine_line
            run.post_engine_line = fake_post
            try:
                ok = run.replay_requests(
                    cfg, b"", None, [line], "sstore.json::case", log,
                    phase="testing",
                )
            finally:
                run.post_engine_line = original
                log.close()
            events = (Path(tmp) / "logs" / "events.log").read_text()

            self.assertTrue(ok)
            self.assertEqual(log.failure_total, 1)
            self.assertIn("FAILED", events)
            self.assertIn("status=SYNCING", events)
            self.assertIn("parent block unknown", events)

    def test_baseline_out_dir_falls_back_to_sudo(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out = root / "data" / "besu-bumped"
            log = run.SweepLog(root / "logs")
            calls: list[list[str]] = []

            def fake_run(cmd, check=True, capture=False):
                calls.append(cmd)
                os.makedirs(out, exist_ok=True)
                return SimpleNamespace(returncode=0, stdout="", stderr="")

            original = run._run
            run._run = fake_run
            try:
                with unittest.mock.patch.object(
                    Path, "mkdir", side_effect=PermissionError
                ):
                    run.ensure_baseline_out_dir(out, log)
            finally:
                run._run = original
                log.close()

            self.assertTrue(out.is_dir())
            self.assertEqual(calls, [["sudo", "-n", "mkdir", "-p", str(out)]])

    def test_rpc_method_reads_prefix_only(self):
        huge = '{"jsonrpc":"2.0","method":"engine_newPayloadV5","params":["' + ("x" * 10000) + '"]}'
        self.assertEqual(run._rpc_method(huge), "engine_newPayloadV5")
        self.assertEqual(run._rpc_method("not-json"), "")

    def test_replay_file_streams_without_read_text(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "pre-run.request"
            line = json.dumps({
                "jsonrpc": "2.0",
                "method": "engine_forkchoiceUpdatedV3",
                "params": [],
            })
            path.write_text(line + "\n" + line + "\n")
            log = run.SweepLog(Path(tmp) / "logs")
            cfg = SimpleNamespace(
                run=SimpleNamespace(fail_fast=False, request_timeout_s=1),
                besu=SimpleNamespace(engine_url="http://127.0.0.1:8551"),
            )
            posted: list[str] = []

            def fake_post(_cfg, _secret, _session, raw):
                posted.append(raw)
                return 200, {"result": {"payloadStatus": {"status": "VALID"}}}, None

            original = run.post_engine_line
            run.post_engine_line = fake_post
            try:
                ok = run.replay_file(cfg, b"secret", None, path, log, phase="prepare")
            finally:
                run.post_engine_line = original
                log.close()

            self.assertTrue(ok)
            self.assertEqual(len(posted), 2)

    def test_detects_besu_rejected_options(self):
        logs = (
            "[0.056s][warning][aot] Failed to link AdapterHandlerEntry\n"
            "Unknown options: '--Xbal-state-root-timeout=-1', "
            "'--Xbal-trust-state-root=false'\n"
            "\nTo display full help:\n"
        )

        self.assertIn("--Xbal-state-root-timeout", run._rejected_options(logs))
        self.assertIsNone(run._rejected_options("Besu is starting\n"))

    def test_rewind_fcu_line_points_head_at_pre_run_hash(self):
        head = "0x" + "ef" * 32
        body = json.loads(run.rewind_forkchoice_line(head, 4))
        self.assertEqual(body["method"], "engine_forkchoiceUpdatedV4")
        self.assertEqual(body["params"][0]["headBlockHash"], head)
        self.assertEqual(body["params"][0]["safeBlockHash"], run.ZERO_HASH)
        self.assertIsNone(body["params"][1])

    def test_isolation_rewind_is_valid(self):
        self.assertEqual(run._validate_isolation("rewind"), "rewind")
        self.assertEqual(run._validate_isolation("RESTART"), "restart")
        with self.assertRaises(ValueError):
            run._validate_isolation("container-recreate")

    def test_warns_when_rewind_points_at_stateful_fixtures(self):
        cfg = SimpleNamespace(
            run=SimpleNamespace(isolation="rewind"),
            tests=SimpleNamespace(
                fixtures_subdir="eest-payloads/geth/blockchain_tests_stateful_engine",
            ),
        )
        buf = StringIO()
        with unittest.mock.patch("sys.stderr", buf):
            run.warn_if_rewind_fixture_mix(cfg)
        self.assertIn("does not contain 'compute'", buf.getvalue())

    def test_no_warn_when_rewind_fixtures_are_compute(self):
        cfg = SimpleNamespace(
            run=SimpleNamespace(isolation="rewind"),
            tests=SimpleNamespace(
                fixtures_subdir="eest-payloads-compute/geth/blockchain_tests_stateful_engine",
            ),
        )
        buf = StringIO()
        with unittest.mock.patch("sys.stderr", buf):
            run.warn_if_rewind_fixture_mix(cfg)
        self.assertEqual(buf.getvalue(), "")

    def test_rewind_canonical_head_sends_fcu_and_optional_sethead(self):
        head = "0x" + "aa" * 32
        posted = []
        cfg = SimpleNamespace(
            run=SimpleNamespace(
                rewind_fcu_version=4,
                rewind_debug_sethead=True,
                post_test_sleep_s=0,
                request_timeout_s=1,
            ),
            besu=SimpleNamespace(
                extra_args=["--rpc-http-port=8545"],
                engine_url="http://127.0.0.1:8551",
            ),
        )
        log = run.SweepLog(Path(tempfile.mkdtemp()) / "logs")

        def fake_post(_cfg, _secret, _session, raw):
            posted.append(json.loads(raw))
            return 200, {"result": {"payloadStatus": {"status": "VALID"}}}, None

        def fake_sethead(_besu, number):
            posted.append({"method": "debug_setHead", "n": number})
            return True, ""

        original_post = run.post_engine_line
        original_set = run.debug_set_head
        original_query = run.query_chain_head
        run.post_engine_line = fake_post
        run.debug_set_head = fake_sethead
        run.query_chain_head = lambda _besu: (24443820, head)
        try:
            ok = run.rewind_canonical_head(
                cfg, b"s", None, log, 24443820, head,
            )
        finally:
            run.post_engine_line = original_post
            run.debug_set_head = original_set
            run.query_chain_head = original_query
            log.close()

        self.assertTrue(ok)
        self.assertEqual(posted[0]["method"], "engine_forkchoiceUpdatedV4")
        self.assertEqual(posted[1]["method"], "debug_setHead")
        self.assertEqual(posted[1]["n"], 24443820)


if __name__ == "__main__":
    unittest.main()
