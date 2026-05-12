import json
import os
import threading

import pytest

from reporting import DeployReporter


@pytest.fixture
def reporter(tmp_path):
    json_path = tmp_path / "deploy_summary.json"
    return DeployReporter(json_path=str(json_path))


class TestDeployReporter:
    def test_starts_empty_when_file_missing(self, reporter):
        assert reporter.get_summary() == {}

    def test_log_creates_object_and_category(self, reporter):
        reporter.log("Account", "Success")
        assert reporter.get_summary() == {"Account": {"Success": 1}}

    def test_log_increments_existing_category(self, reporter):
        reporter.log("Account", "Success")
        reporter.log("Account", "Success")
        reporter.log("Account", "Success")
        assert reporter.get_summary()["Account"]["Success"] == 3

    def test_log_tracks_multiple_categories_per_object(self, reporter):
        reporter.log("Account", "Success")
        reporter.log("Account", "Failed")
        reporter.log("Account", "Failed")
        assert reporter.get_summary()["Account"] == {"Success": 1, "Failed": 2}

    def test_log_tracks_multiple_objects(self, reporter):
        reporter.log("Account", "Success")
        reporter.log("Case", "Skipped")
        summary = reporter.get_summary()
        assert summary["Account"]["Success"] == 1
        assert summary["Case"]["Skipped"] == 1

    def test_log_persists_to_disk_immediately(self, tmp_path):
        json_path = tmp_path / "deploy_summary.json"
        rep = DeployReporter(json_path=str(json_path))
        rep.log("Account", "Success")

        assert json_path.exists()
        with open(json_path) as f:
            data = json.load(f)
        assert data == {"Account": {"Success": 1}}

    def test_new_instance_loads_existing_file(self, tmp_path):
        json_path = tmp_path / "deploy_summary.json"
        first = DeployReporter(json_path=str(json_path))
        first.log("Account", "Success")
        first.log("Case", "Failed")

        second = DeployReporter(json_path=str(json_path))
        assert second.get_summary() == {
            "Account": {"Success": 1},
            "Case": {"Failed": 1},
        }

    def test_clear_empties_state_and_removes_file(self, tmp_path):
        json_path = tmp_path / "deploy_summary.json"
        rep = DeployReporter(json_path=str(json_path))
        rep.log("Account", "Success")
        assert json_path.exists()

        rep.clear()

        assert rep.get_summary() == {}
        assert not json_path.exists()

    def test_clear_when_no_file_does_not_raise(self, tmp_path):
        json_path = tmp_path / "missing.json"
        rep = DeployReporter(json_path=str(json_path))
        rep.clear()
        assert rep.get_summary() == {}

    def test_corrupt_json_falls_back_to_empty(self, tmp_path):
        json_path = tmp_path / "deploy_summary.json"
        json_path.write_text("not valid json {{{")

        rep = DeployReporter(json_path=str(json_path))
        assert rep.get_summary() == {}

    def test_concurrent_log_calls_are_thread_safe(self, tmp_path):
        json_path = tmp_path / "deploy_summary.json"
        rep = DeployReporter(json_path=str(json_path))

        n_threads = 20
        per_thread = 50

        def worker():
            for _ in range(per_thread):
                rep.log("Account", "Success")

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert rep.get_summary()["Account"]["Success"] == n_threads * per_thread
