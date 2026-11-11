import concurrent.futures
import csv

import pytest
import requests

from download_functions import (
    build_file_type_condition,
    build_s3_uri,
    download_file,
    query_content_document_links,
    reserve_unique_filename,
    split_into_batches,
    used_filenames,
)


@pytest.fixture(autouse=True)
def reset_used_filenames():
    used_filenames.clear()
    yield
    used_filenames.clear()


class TestReserveUniqueFilename:
    def test_first_use_returns_input(self):
        assert reserve_unique_filename("out/file.txt") == "out/file.txt"

    def test_second_use_appends_suffix(self):
        reserve_unique_filename("out/file.txt")
        assert reserve_unique_filename("out/file.txt") == "out/file_1.txt"

    def test_third_use_increments_suffix(self):
        reserve_unique_filename("out/file.txt")
        reserve_unique_filename("out/file.txt")
        assert reserve_unique_filename("out/file.txt") == "out/file_2.txt"

    def test_suffix_preserves_extension(self):
        reserve_unique_filename("a/b/report.tar.gz")
        # os.path.splitext only splits on the last dot
        assert reserve_unique_filename("a/b/report.tar.gz") == "a/b/report.tar_1.gz"

    def test_no_extension(self):
        reserve_unique_filename("README")
        assert reserve_unique_filename("README") == "README_1"

    def test_does_not_collide_with_pre_existing_suffix(self):
        # Pre-reserve the suffix that would normally be generated
        assert reserve_unique_filename("out/file.txt") == "out/file.txt"
        assert reserve_unique_filename("out/file_1.txt") == "out/file_1.txt"
        # Next "file.txt" must skip _1 and go to _2
        assert reserve_unique_filename("out/file.txt") == "out/file_2.txt"

    def test_thread_safety_under_contention(self):
        # All threads reserving the same filename should produce
        # distinct results, with the count matching the call count.
        n = 100
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
            results = list(ex.map(lambda _: reserve_unique_filename("x.txt"), range(n)))

        assert len(set(results)) == n
        assert "x.txt" in results
        # The remaining 99 should all be of the form x_<k>.txt with distinct k
        suffixed = [r for r in results if r != "x.txt"]
        assert len(suffixed) == n - 1
        nums = sorted(int(r[len("x_"):-len(".txt")]) for r in suffixed)
        assert nums == list(range(1, n))


class TestSplitIntoBatches:
    def test_empty_input(self):
        assert list(split_into_batches([], 10)) == []

    def test_batch_larger_than_input(self):
        assert list(split_into_batches([1, 2, 3], 100)) == [[1, 2, 3]]

    def test_exact_multiple(self):
        assert list(split_into_batches([1, 2, 3, 4], 2)) == [[1, 2], [3, 4]]

    def test_remainder_batch(self):
        assert list(split_into_batches([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]

    def test_batch_size_one(self):
        assert list(split_into_batches([1, 2, 3], 1)) == [[1], [2], [3]]

    def test_accepts_generator_input(self):
        def gen():
            yield from range(5)

        assert list(split_into_batches(gen(), 2)) == [[0, 1], [2, 3], [4]]

    def test_dict_items_preserved(self):
        data = [{"Id": "a"}, {"Id": "b"}, {"Id": "c"}]
        batches = list(split_into_batches(data, 2))
        assert batches == [[{"Id": "a"}, {"Id": "b"}], [{"Id": "c"}]]


class FakeSalesforce:
    def __init__(self, record_ids):
        self.record_ids = record_ids
        self.queries = []

    def query_all(self, query):
        self.queries.append(query)
        if query.startswith("SELECT Id FROM Account"):
            return {"records": [{"Id": record_id} for record_id in self.record_ids]}
        return {"records": [{"ContentDocumentId": f"DOC{len(self.queries)}"}]}


class TestQueryContentDocumentLinks:
    def test_query_without_limit_is_used_as_semi_join(self):
        sf = FakeSalesforce([])
        query_content_document_links(sf, "SELECT Id FROM Account", batch_size=100)
        assert len(sf.queries) == 1
        assert "LinkedEntityId in (SELECT Id FROM Account)" in sf.queries[0]

    def test_limit_query_is_resolved_to_ids_outside_semi_join(self):
        sf = FakeSalesforce(["001A", "001B"])
        query_content_document_links(sf, "SELECT Id FROM Account limit 2", batch_size=100)
        assert sf.queries[0] == "SELECT Id FROM Account limit 2"
        assert "LinkedEntityId in ('001A', '001B')" in sf.queries[1]
        assert all("LIMIT" not in q.upper() for q in sf.queries[1:])

    def test_limit_query_ids_are_batched_and_links_combined(self):
        sf = FakeSalesforce(["001A", "001B", "001C"])
        links = query_content_document_links(sf, "SELECT Id FROM Account LIMIT 3", batch_size=2)
        assert len(sf.queries) == 3
        assert "('001A', '001B')" in sf.queries[1]
        assert "('001C')" in sf.queries[2]
        assert links == [{"ContentDocumentId": "DOC2"}, {"ContentDocumentId": "DOC3"}]

    def test_limit_query_matching_no_records_makes_no_link_query(self):
        sf = FakeSalesforce([])
        assert query_content_document_links(sf, "SELECT Id FROM Account LIMIT 100", batch_size=100) == []
        assert len(sf.queries) == 1


class TestBuildFileTypeCondition:
    def test_no_filter_excludes_only_snote(self):
        assert build_file_type_condition(None) == "FileExtension != 'snote'"
        assert build_file_type_condition("") == "FileExtension != 'snote'"

    def test_plain_values_are_included(self):
        assert build_file_type_condition("'EXCEL_M', 'EXCEL'") == "FileType IN ('EXCEL_M', 'EXCEL')"

    def test_caret_value_is_excluded_and_snote_still_excluded(self):
        assert build_file_type_condition("'^JSON'") == (
            "FileExtension != 'snote' AND FileType NOT IN ('JSON')"
        )

    def test_caret_outside_quotes_is_also_excluded(self):
        assert build_file_type_condition("^'JSON', ^'XML'") == (
            "FileExtension != 'snote' AND FileType NOT IN ('JSON', 'XML')"
        )

    def test_double_quoted_config_value_is_unwrapped(self):
        assert build_file_type_condition('"\'^JSON\'"') == (
            "FileExtension != 'snote' AND FileType NOT IN ('JSON')"
        )
        assert build_file_type_condition('"\'EXCEL_M\', \'EXCEL\'"') == "FileType IN ('EXCEL_M', 'EXCEL')"

    def test_include_and_exclude_can_be_combined(self):
        assert build_file_type_condition("'PDF', 'EXCEL', '^JSON'") == (
            "FileType IN ('PDF', 'EXCEL') AND FileType NOT IN ('JSON')"
        )


class TestBuildS3Uri:
    BASE = "s3://bucket/OrgId=00Dxx/Period=0/files/"
    CHECKSUM = "0000aaaabbbbccccddddeeeeffff0000"

    def test_uri_uses_first_four_checksum_chars_as_prefix(self):
        assert build_s3_uri(self.BASE, self.CHECKSUM, "pdf") == (
            "s3://bucket/OrgId=00Dxx/Period=0/files/0000/0000aaaabbbbccccddddeeeeffff0000.pdf"
        )

    def test_extension_case_is_kept_as_heimdall_writes_it(self):
        assert build_s3_uri(self.BASE, self.CHECKSUM, "PDF").endswith(".PDF")

    def test_file_type_used_when_extension_missing(self):
        assert build_s3_uri(self.BASE, self.CHECKSUM, "", "PDF").endswith(f"{self.CHECKSUM}.PDF")

    def test_null_extension_falls_back_to_unknown_file_type(self):
        # Salesforce returns FileExtension as null for files without extension; Heimdall stores them as .UNKNOWN
        assert build_s3_uri(self.BASE, self.CHECKSUM, None, "UNKNOWN").endswith(f"{self.CHECKSUM}.UNKNOWN")

    def test_base_without_trailing_slash_is_normalised(self):
        assert build_s3_uri(self.BASE.rstrip("/"), self.CHECKSUM, "pdf") == build_s3_uri(self.BASE, self.CHECKSUM, "pdf")

    @pytest.mark.parametrize("base, checksum, ext", [
        ("", CHECKSUM, "pdf"),
        (BASE, "", "pdf"),
        (BASE, CHECKSUM, ""),
    ])
    def test_empty_when_key_cannot_be_built(self, base, checksum, ext):
        assert build_s3_uri(base, checksum, ext) == ""


class FakeSessionSalesforce:
    sf_instance = "example.my.salesforce.com"
    session_id = "SESSION"


def _record(**overrides):
    record = {
        "Id": "068A", "ContentDocumentId": "069A", "Title": "Report", "FileExtension": "pdf",
        "FileType": "PDF", "OwnerId": "005A", "VersionNumber": "1",
        "Checksum": "0000aaaabbbbccccddddeeeeffff0000", "VersionData": "/services/data/v60.0/x",
    }
    record.update(overrides)
    return record


def _run_download_file(tmp_path, metadata_only, s3_base_uri, record=None):
    results_path = tmp_path / "files.csv"
    links = [{"ContentDocumentId": "069A", "LinkedEntityId": "001A", "LinkedEntity": {"Name": "Acme"}}]
    result = download_file((
        record or _record(), str(tmp_path) + "/", FakeSessionSalesforce(), str(results_path),
        links, "ContentDocumentId", "{0}{1}-{2}.{3}", metadata_only, s3_base_uri,
    ))
    rows = list(csv.reader(results_path.open(encoding="UTF-8")))
    return result, rows


class TestDownloadFileMetadataOnly:
    def test_metadata_only_writes_csv_row_without_http_request(self, tmp_path, monkeypatch):
        def fail_get(*args, **kwargs):
            raise AssertionError("metadata-only must not download")
        monkeypatch.setattr(requests, "get", fail_get)

        result, rows = _run_download_file(tmp_path, metadata_only=True, s3_base_uri="")

        assert result.startswith("Recorded")
        assert rows[0][0] == "068A"
        assert not (tmp_path / "069A-Report.pdf").exists()

    def test_s3_uri_column_appended_when_base_uri_set(self, tmp_path, monkeypatch):
        monkeypatch.setattr(requests, "get", lambda *a, **k: pytest.fail("no download expected"))

        _, rows = _run_download_file(tmp_path, metadata_only=True, s3_base_uri="s3://b/files/")

        assert len(rows[0]) == 12
        assert rows[0][-1] == "s3://b/files/0000/0000aaaabbbbccccddddeeeeffff0000.pdf"

    def test_csv_row_unchanged_when_base_uri_unset(self, tmp_path, monkeypatch):
        monkeypatch.setattr(requests, "get", lambda *a, **k: pytest.fail("no download expected"))

        _, rows = _run_download_file(tmp_path, metadata_only=True, s3_base_uri="")

        assert len(rows[0]) == 11

    def test_missing_checksum_gives_empty_s3_uri(self, tmp_path, monkeypatch):
        monkeypatch.setattr(requests, "get", lambda *a, **k: pytest.fail("no download expected"))

        _, rows = _run_download_file(tmp_path, True, "s3://b/files/", record=_record(Checksum=""))

        assert rows[0][-1] == ""

    def test_title_with_comma_and_quote_round_trips_through_dict_reader(self, tmp_path, monkeypatch):
        # deploy.py reads files.csv with csv.DictReader defaults, so the writer must use standard quoting
        monkeypatch.setattr(requests, "get", lambda *a, **k: pytest.fail("no download expected"))
        title = 'Offer, "final" | v2'

        _run_download_file(tmp_path, True, "", record=_record(Title=title))

        fields = ["ContentVersionOldId", "FirstPublicationId", "FirstPublicationName", "ContentDocumentId", "Title"]
        row = next(csv.DictReader((tmp_path / "files.csv").open(encoding="UTF-8"), fieldnames=fields, restkey="rest"))
        assert row["Title"] == title
        assert len(row["rest"]) == 6
