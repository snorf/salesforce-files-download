import asyncio
import sys

import pytest
from rich.console import Console

import download


def _mapping(tmp_path, monkeypatch, *source_objects):
    path = tmp_path / "object_mapping.csv"
    path.write_text("Source Org Object,Target Org Object\n" + "".join(f"{o},{o}\n" for o in source_objects))
    monkeypatch.setattr(download, "csv_file", str(path))


@pytest.fixture
def no_side_effects(monkeypatch):
    """Fail the test if anything would clear the output directory, log in or start a download."""
    def forbidden(*args, **kwargs):
        raise AssertionError("must not be reached")
    monkeypatch.setattr(download, "preflight_checks", lambda mode: None)
    monkeypatch.setattr(download, "load_config", lambda: {})
    monkeypatch.setattr(download, "prepare_output_directory", forbidden)
    monkeypatch.setattr(download, "run_batch_mode", forbidden)
    monkeypatch.setattr(download, "run_cli_mode", forbidden)


class TestLoadSourceObjects:
    def test_record_objects_are_returned_in_order(self, tmp_path, monkeypatch):
        _mapping(tmp_path, monkeypatch, "Account", "Case")

        assert download.load_source_objects(Console()) == ["Account", "Case"]

    @pytest.mark.parametrize("file_object", ["ContentVersion", "contentdocument", "ContentDocumentLink"])
    def test_file_object_as_source_is_rejected(self, tmp_path, monkeypatch, file_object):
        _mapping(tmp_path, monkeypatch, "Account", file_object)

        with pytest.raises(SystemExit) as exc:
            download.load_source_objects(Console())
        assert exc.value.code == 1

    def test_empty_mapping_is_rejected(self, tmp_path, monkeypatch):
        _mapping(tmp_path, monkeypatch)

        with pytest.raises(SystemExit):
            download.load_source_objects(Console())


class TestBatchModeOrder:
    def test_invalid_mapping_stops_before_output_dir_is_cleared(self, tmp_path, monkeypatch, no_side_effects):
        _mapping(tmp_path, monkeypatch, "ContentVersion")
        monkeypatch.setattr(sys, "argv", ["download.py"])

        with pytest.raises(SystemExit) as exc:
            asyncio.run(download.main())
        assert exc.value.code == 1


class TestDeployWithMetadataOnly:
    @pytest.mark.parametrize("argv", [
        ["--mode", "cli", "-q", "SELECT Id FROM Case", "-so", "Case", "--metadata-only", "--deploy"],
        ["--deploy", "--extra", "--metadata-only"],
    ])
    def test_deploy_is_rejected_since_metadata_only_has_no_files(self, monkeypatch, argv):
        monkeypatch.setattr(sys, "argv", ["download.py"] + argv)

        with pytest.raises(SystemExit) as exc:
            download.parse_args()
        assert exc.value.code == 2

    def test_deploy_alone_is_accepted(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["download.py", "--deploy"])

        assert download.parse_args().deploy
