import sys

import pytest

import download


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
