"""Budget report checks using synthetic samples; these do not run benchmarks."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("compare_budgets", Path(__file__).with_name("compare_budgets.py"))
budgets = importlib.util.module_from_spec(spec)
spec.loader.exec_module(budgets)


MACHINE = {
    "model": "fixture", "cpu": "fixture CPU", "physical_cores": 4,
    "logical_cores": 8, "ram_bytes": 16 * 1024**3, "architecture": "fixture",
    "os_version": "fixture OS", "filesystem": {"path": "/fixture", "type": "fixture FS"},
    "load_sampling": "start/end observations", "process_niceness": 0,
}


def sample(build="before", mode="relaxed", requests=100, clients=2, keyspace=64, seed=1337, rate=100):
    rows = []
    for scenario in budgets.SCENARIOS.split(","):
        mix = dict.fromkeys(budgets.KINDS, 0)
        mix["get_chunk" if scenario == "simulation" else "set_block"] = requests
        blocks = requests * (9 if scenario == "simulation" else 1)
        rows.append({"test": scenario, "completed_requests": requests,
                     "completed_blocks": blocks, "returned_area_chunks": 0, "request_mix": mix,
                     "duration_s": requests / rate, "throughput_req_s": rate,
                     "blocks_per_request": blocks / requests, "throughput_blocks_s": blocks * rate / requests,
                     "latency_ms": {"p50": 0.25, "p99": 0.75}})
    report = {"server_mode": "spawn", "requests": requests, "requested_clients": clients,
              "pipeline": 1, "io_timeout_ms": budgets.IO_TIMEOUT_MS, "keyspace": keyspace, "seed": seed, "durability_mode": mode,
              "source_commit": build, "source_state": "clean", "build_type": "Release",
              "benchmark_source_sha256": "a" * 64, "chunk_width_blocks": 3,
              "chunk_height_blocks": 3, "results": rows}
    return {"report": report, "command": [f"/{build}/chunkdb_server_bench"],
            "binary_sha256": "b" * 64, "load_before": [1, 2, 3], "load_after": [2, 3, 4], "stderr": ""}


class BudgetReports(unittest.TestCase):
    def test_actual_blocks_geometry_and_clipped_area_validation(self):
        report = sample()["report"]
        row = report["results"][0]
        row["request_mix"]["set_block"] = 99
        row["request_mix"]["get_area"] = 1
        row.update(returned_area_chunks=1, completed_blocks=108,
                   blocks_per_request=1.08, throughput_blocks_s=108)
        budgets.validate_run(report, "relaxed", 100, 2, 64, 1337)
        row["completed_blocks"] = 117
        with self.assertRaisesRegex(ValueError, "block count"):
            budgets.validate_run(report, "relaxed", 100, 2, 64, 1337)

    def test_missing_metrics_and_invalid_values_are_rejected(self):
        changes = [lambda r: r.update(io_timeout_ms=30000),
                   lambda r: r.pop("benchmark_source_sha256"),
                   lambda r: r.update(benchmark_source_sha256="wrong"),
                   lambda r: r["results"][0].update(throughput_req_s=float("nan")),
                   lambda r: r["results"][0].update(throughput_blocks_s=200),
                   lambda r: r["results"][0]["request_mix"].update(set_block=99)]
        for change in changes:
            with self.subTest(change=change):
                report = sample()["report"]
                change(report)
                with self.assertRaises(ValueError):
                    budgets.validate_run(report, "relaxed", 100, 2, 64, 1337)

    def test_serialized_duration_and_rate_rounding_are_valid(self):
        report = sample(rate=1234.567891)["report"]
        for row in report["results"]:
            row["duration_s"] = round(row["duration_s"], 6)
            row["throughput_req_s"] = round(row["throughput_req_s"], 3)
            row["throughput_blocks_s"] = round(row["throughput_blocks_s"], 6)
        budgets.validate_run(report, "relaxed", 100, 2, 64, 1337)

    def test_median_range_percentiles_and_load(self):
        samples = {side: [sample(side, rate=rate) for rate in rates]
                   for side, rates in (("before", [80, 100, 140]), ("after", [90, 110, 150]))}
        result = budgets.summarize_profile("relaxed", "relaxed", 100, 2, 64, True, samples, 5, None)
        self.assertEqual(result["results"][0]["before"]["throughput_req_s"],
                         {"median": 100, "min": 80, "max": 140, "runs": 3})
        self.assertEqual(result["results"][0]["before"]["p50_ms"]["median"], 0.25)
        self.assertAlmostEqual(result["results"][0]["change_percent"], 10)
        self.assertEqual(result["load_average_range"], [{"min": 1, "max": 2}, {"min": 2, "max": 3}, {"min": 3, "max": 4}])
        self.assertTrue(result["passed"])

    def test_geometry_build_type_and_binary_mutation_are_rejected(self):
        for field, value in (("chunk_width_blocks", 4), ("build_type", "Debug")):
            samples = {"before": [sample()], "after": [sample("after")]}
            samples["after"][0]["report"][field] = value
            with self.assertRaisesRegex(ValueError, "same geometry and build type"):
                budgets.summarize_profile("relaxed", "relaxed", 100, 2, 64, True, samples, 5, None)
        samples = {"before": [sample(), sample()], "after": [sample("after"), sample("after")]}
        samples["before"][1]["binary_sha256"] = "c" * 64
        with self.assertRaisesRegex(ValueError, "changed between runs"):
            budgets.summarize_profile("relaxed", "relaxed", 100, 2, 64, True, samples, 5, None)

    def run_main(self, arguments, after_rate=100):
        calls = []
        def fake_run(build, mode, requests, clients, keyspace, seed, watch):
            calls.append((build, seed, watch))
            return sample(build, mode, requests, clients, keyspace, seed, after_rate if build == "after" else 100)
        with mock.patch.object(budgets, "run_once", side_effect=fake_run), \
             mock.patch.object(budgets, "machine_info", return_value=MACHINE), \
             mock.patch.object(budgets, "load_average", return_value=[1, 2, 3]), \
             contextlib.redirect_stdout(io.StringIO()) as output, contextlib.redirect_stderr(io.StringIO()):
            status = budgets.main(["before", "after"] + arguments)
        return status, calls, output.getvalue()

    def test_report_only_exit_zero_and_relaxed_drop_exit_one(self):
        status, _, output = self.run_main(["--runs", "1", "--profiles", "fsync-wal"], after_rate=20)
        self.assertEqual(status, 0)
        self.assertIn("reported only", output)
        status, _, output = self.run_main(["--runs", "1", "--profiles", "relaxed"], after_rate=20)
        self.assertEqual(status, 1)
        self.assertIn("OVER BUDGET", output)

    def test_default_fifteen_alternating_pairs_and_watch(self):
        status, calls, _ = self.run_main(["--profiles", "relaxed", "--watch", "default"])
        self.assertEqual(status, 0)
        self.assertEqual(len(calls), 30)
        self.assertEqual(calls[:4], [("before", 1337, None), ("after", 1337, "default"),
                                    ("after", 1338, "default"), ("before", 1338, None)])
        self.assertEqual(calls[-2:], [("before", 1351, None), ("after", 1351, "default")])

    def test_json_and_markdown_use_one_measurement_set(self):
        with tempfile.TemporaryDirectory() as directory:
            json_path, md_path = Path(directory) / "report.json", Path(directory) / "report.md"
            status, calls, output = self.run_main(["--runs", "3", "--profiles", "relaxed",
                                                  "--json", str(json_path), "--markdown", str(md_path)])
            report = json.loads(json_path.read_text())
            self.assertEqual(status, 0)
            self.assertEqual(len(calls), 6)
            self.assertEqual(output, "")
            self.assertEqual(md_path.read_text(), budgets.render_markdown(report))
            self.assertEqual(report["profiles"][0]["samples"]["after"][1]["order"], 1)
            self.assertIn("| Verdict | Change req/s |", md_path.read_text())
            self.assertIn("fixture CPU", md_path.read_text())

    def test_run_once_preserves_raw_output_and_detects_binary_changes(self):
        report = sample()["report"]
        child = mock.Mock(stdout="starting benchmark\n" + json.dumps(report), stderr="fixture diagnostic\n")
        with mock.patch.object(budgets, "file_hash", return_value="b" * 64), \
             mock.patch.object(budgets, "load_average", side_effect=[[1, 2, 3], [2, 3, 4]]), \
             mock.patch.object(budgets, "free_port", return_value=12345), \
             mock.patch.object(budgets.subprocess, "run", return_value=child) as run:
            value = budgets.run_once("before", "relaxed", 100, 2, 64, 1337, "world")
        self.assertEqual(value["report"], report)
        self.assertEqual(value["stderr"], "fixture diagnostic\n")
        self.assertEqual(value["load_after"], [2, 3, 4])
        self.assertEqual(run.call_args.args[0][-2:], ["--watch", "world"])
        with mock.patch.object(budgets, "file_hash", side_effect=["b" * 64, "c" * 64]), \
             mock.patch.object(budgets, "load_average", return_value=None), \
             mock.patch.object(budgets, "free_port", return_value=12345), \
             mock.patch.object(budgets.subprocess, "run", return_value=child):
            with self.assertRaisesRegex(ValueError, "changed during measurement"):
                budgets.run_once("before", "relaxed", 100, 2, 64, 1337)

    def test_invalid_arguments_fail_before_machine_or_measurement(self):
        for options in (["--runs", "0"], ["--profiles", ""], ["--profiles", "unknown"],
                        ["--profiles", "relaxed,relaxed"], ["--budget", "nan"],
                        ["--json", "--markdown"], ["--json", "same", "--markdown", "same"]):
            with self.subTest(options=options), mock.patch.object(budgets, "machine_info") as machine, \
                 mock.patch.object(budgets, "run_once") as run, contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as raised:
                    budgets.main(["before", "after"] + options)
                self.assertEqual(raised.exception.code, 2)
                machine.assert_not_called()
                run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
