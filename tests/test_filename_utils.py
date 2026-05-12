import os

import pytest

from filename_utils import create_filename, sanitize_filename


class TestSanitizeFilename:
    def test_empty_string_returns_untitled(self):
        assert sanitize_filename("") == "untitled"

    def test_none_returns_untitled(self):
        assert sanitize_filename(None) == "untitled"

    def test_plain_name_passes_through(self):
        assert sanitize_filename("ReportQ4") == "ReportQ4"

    def test_truncates_to_max_length(self):
        long_title = "a" * 500
        assert sanitize_filename(long_title, max_length=50) == "a" * 50

    def test_default_max_length_is_200(self):
        assert len(sanitize_filename("a" * 500)) == 200

    def test_unix_replaces_problem_chars(self, monkeypatch):
        monkeypatch.setattr(os, "name", "posix")
        # Unix bad chars: ; : ! * / \
        assert sanitize_filename("foo;bar:baz!qux*quux/x\\y") == "foo_bar_baz_qux_quux_x_y"

    def test_unix_strips_surrounding_whitespace(self, monkeypatch):
        monkeypatch.setattr(os, "name", "posix")
        assert sanitize_filename("  hello  ") == "hello"

    def test_unix_keeps_dots_and_dashes(self, monkeypatch):
        monkeypatch.setattr(os, "name", "posix")
        assert sanitize_filename("my.file-v2") == "my.file-v2"

    def test_windows_replaces_bad_chars(self, monkeypatch):
        monkeypatch.setattr(os, "name", "nt")
        # Windows bad chars: < > : " / \ | ? *
        assert sanitize_filename('a<b>c:d"e/f\\g|h?i*j') == "a_b_c_d_e_f_g_h_i_j"

    def test_windows_strips_control_chars(self, monkeypatch):
        monkeypatch.setattr(os, "name", "nt")
        assert sanitize_filename("foo\x00bar\x1fbaz") == "foo_bar_baz"

    def test_windows_strips_trailing_dots_and_spaces(self, monkeypatch):
        monkeypatch.setattr(os, "name", "nt")
        assert sanitize_filename("report. ") == "report"

    @pytest.mark.parametrize(
        "reserved",
        ["CON", "PRN", "AUX", "NUL", "COM1", "LPT9", "con.txt", "Prn.log"],
    )
    def test_windows_reserved_names_get_underscore_prefix(self, monkeypatch, reserved):
        monkeypatch.setattr(os, "name", "nt")
        assert sanitize_filename(reserved).startswith("_")

    def test_windows_non_reserved_similar_name_untouched(self, monkeypatch):
        monkeypatch.setattr(os, "name", "nt")
        # CONSOLE doesn't match the reserved-name regex
        assert sanitize_filename("CONSOLE") == "CONSOLE"

    def test_unix_falls_back_to_untitled_when_only_whitespace(self, monkeypatch):
        monkeypatch.setattr(os, "name", "posix")
        assert sanitize_filename("   ") == "untitled"

    def test_windows_falls_back_to_untitled_when_only_bad_chars_stripped(self, monkeypatch):
        monkeypatch.setattr(os, "name", "nt")
        # All chars become '.' or ' ' then stripped → empty → "untitled"
        assert sanitize_filename(". . .") == "untitled"


class TestCreateFilename:
    def test_default_pattern(self):
        result = create_filename(
            output_directory="out",
            content_document_id="069XX0001",
            title="Report",
            file_extension="pdf",
        )
        assert result == f"out{os.sep}069XX0001-Report.pdf"

    def test_appends_separator_if_missing(self):
        result = create_filename(
            output_directory="out",
            content_document_id="A",
            title="B",
            file_extension="txt",
        )
        assert result.startswith(f"out{os.sep}")

    def test_does_not_double_separator(self):
        result = create_filename(
            output_directory="out" + os.sep,
            content_document_id="A",
            title="B",
            file_extension="txt",
        )
        assert result.startswith(f"out{os.sep}")
        assert os.sep + os.sep not in result

    def test_title_is_sanitized(self, monkeypatch):
        monkeypatch.setattr(os, "name", "posix")
        result = create_filename(
            output_directory="out",
            content_document_id="A",
            title="weird:name",
            file_extension="pdf",
        )
        assert "weird_name" in result
        assert ":" not in os.path.basename(result)

    def test_custom_pattern_with_linked_entity_and_version(self):
        result = create_filename(
            output_directory="out",
            content_document_id="DOC1",
            title="Spec",
            file_extension="pdf",
            linked_entity_name="Acme",
            version_number="3",
            filename_pattern="{0}{4}/{1}-{2}-v{5}.{3}",
        )
        assert result == f"out{os.sep}Acme/DOC1-Spec-v3.pdf"

    def test_none_linked_entity_renders_empty(self):
        result = create_filename(
            output_directory="out",
            content_document_id="DOC1",
            title="Spec",
            file_extension="pdf",
            linked_entity_name=None,
            filename_pattern="{4}-{1}",
        )
        assert result == "-DOC1"

    def test_none_version_renders_empty(self):
        result = create_filename(
            output_directory="out",
            content_document_id="DOC1",
            title="Spec",
            file_extension="pdf",
            version_number=None,
            filename_pattern="v{5}",
        )
        assert result == "v"

    def test_empty_output_directory_does_not_prepend_separator(self):
        result = create_filename(
            output_directory="",
            content_document_id="A",
            title="B",
            file_extension="txt",
        )
        assert result == "A-B.txt"
