import concurrent.futures
import csv

import pytest
import requests

from download_functions import (
    build_file_type_condition,
    DEFAULT_DOWNLOAD_URL_PATTERN,
    build_download_url,
    csv_header,
    download_file,
    METADATA_CSV_HEADER,
    fetch_files,
    format_size,
    query_content_document_links,
    reserve_unique_filename,
    split_into_batches,
    used_filenames,
    validate_download_url_pattern,
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


CHECKSUM = "0000aaaabbbbccccddddeeeeffff0000"
S3_PATTERN = "s3://bucket/OrgId=00Dxx/Period=0/files/{checksum_prefix}/{checksum}.{extension}"
INSTANCE = "example.my.salesforce.com"


def _record(**overrides):
    record = {
        "Id": "068A", "ContentDocumentId": "069A", "Title": "Report", "FileExtension": "pdf",
        "FileType": "PDF", "OwnerId": "005A", "VersionNumber": "1",
        "Checksum": CHECKSUM, "ContentSize": 29491, "VersionData": "/services/data/v60.0/sobjects/ContentVersion/068A/VersionData",
    }
    record.update(overrides)
    return record


class TestBuildDownloadUrl:
    def test_default_pattern_gives_salesforce_rest_url(self):
        assert build_download_url(DEFAULT_DOWNLOAD_URL_PATTERN, INSTANCE, _record()) == (
            "https://example.my.salesforce.com/services/data/v60.0/sobjects/ContentVersion/068A/VersionData"
        )

    def test_s3_pattern_gives_heimdall_key_with_four_char_checksum_prefix(self):
        assert build_download_url(S3_PATTERN, INSTANCE, _record()) == (
            "s3://bucket/OrgId=00Dxx/Period=0/files/0000/0000aaaabbbbccccddddeeeeffff0000.pdf"
        )

    def test_extension_case_is_kept_as_heimdall_writes_it(self):
        assert build_download_url(S3_PATTERN, INSTANCE, _record(FileExtension="PDF")).endswith(".PDF")

    @pytest.mark.parametrize("file_extension", ["", None])
    def test_missing_extension_falls_back_to_file_type(self, file_extension):
        # Heimdall stores files without extension as {checksum}.UNKNOWN (FileType)
        url = build_download_url(S3_PATTERN, INSTANCE, _record(FileExtension=file_extension, FileType="UNKNOWN"))
        assert url.endswith(f"{CHECKSUM}.UNKNOWN")

    def test_missing_value_used_by_pattern_gives_empty_url(self):
        assert build_download_url(S3_PATTERN, INSTANCE, _record(Checksum="")) == ""
        assert build_download_url(S3_PATTERN, INSTANCE, _record(Checksum=None)) == ""

    def test_missing_value_not_used_by_pattern_is_ignored(self):
        assert build_download_url(DEFAULT_DOWNLOAD_URL_PATTERN, INSTANCE, _record(Checksum="")) != ""


class TestValidateDownloadUrlPattern:
    @pytest.mark.parametrize("pattern", [DEFAULT_DOWNLOAD_URL_PATTERN, S3_PATTERN, "https://x/{content_version_id}"])
    def test_supported_patterns_are_accepted(self, pattern):
        validate_download_url_pattern(pattern)

    @pytest.mark.parametrize("pattern", ["https://{host}{version_data}", "s3://b/{0}", "s3://b/{}"])
    def test_unknown_or_positional_placeholder_is_rejected_before_any_work(self, pattern):
        with pytest.raises(ValueError, match="Unknown placeholder"):
            validate_download_url_pattern(pattern)


class FakeSessionSalesforce:
    sf_instance = INSTANCE
    session_id = "SESSION"


def _run_download_file(tmp_path, metadata_only, download_url_pattern=DEFAULT_DOWNLOAD_URL_PATTERN, record=None):
    results_path = tmp_path / "files.csv"
    links = [{"ContentDocumentId": "069A", "LinkedEntityId": "001A", "LinkedEntity": {"Name": "Acme"}}]
    result = download_file((
        record or _record(), str(tmp_path) + "/", FakeSessionSalesforce(), str(results_path),
        links, "ContentDocumentId", "{0}{1}-{2}.{3}", metadata_only, download_url_pattern,
    ))
    # restkey collects values beyond the header, so a row/header length mismatch shows up as an "EXTRA" key
    rows = list(csv.DictReader(results_path.open(encoding="UTF-8"), fieldnames=csv_header(metadata_only), restkey="EXTRA"))
    return result, rows


@pytest.fixture
def no_http(monkeypatch):
    def fail_get(*args, **kwargs):
        raise AssertionError("metadata-only must not download")
    monkeypatch.setattr(requests, "get", fail_get)


@pytest.fixture
def fake_http(monkeypatch):
    class Response:
        ok = True
        content = b"%PDF"
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: Response())


class TestDownloadFileMetadataOnly:
    def test_checksum_and_content_size_are_written(self, tmp_path, no_http):
        _, rows = _run_download_file(tmp_path, metadata_only=True)

        assert rows[0]["Checksum"] == CHECKSUM
        assert rows[0]["ContentSize"] == "29491"

    def test_missing_checksum_and_size_are_written_as_empty(self, tmp_path, no_http):
        _, rows = _run_download_file(tmp_path, True, record=_record(Checksum=None, ContentSize=None))

        assert rows[0]["Checksum"] == ""
        assert rows[0]["ContentSize"] == ""

    def test_empty_file_size_is_written_as_zero(self, tmp_path, no_http):
        _, rows = _run_download_file(tmp_path, True, record=_record(ContentSize=0))

        assert rows[0]["ContentSize"] == "0"

    def test_metadata_only_writes_csv_row_without_http_request(self, tmp_path, no_http):
        result, rows = _run_download_file(tmp_path, metadata_only=True)

        assert result.startswith("Recorded")
        assert rows[0]["ContentVersionId"] == "068A"
        assert not (tmp_path / "069A-Report.pdf").exists()

    def test_metadata_only_uses_plain_salesforce_names_and_no_local_paths(self, tmp_path, no_http):
        _, rows = _run_download_file(tmp_path, metadata_only=True)

        assert "EXTRA" not in rows[0]
        assert rows[0]["ContentVersionId"] == "068A"
        assert rows[0]["LinkedEntityId"] == "001A"
        assert rows[0]["LinkedEntityName"] == "Acme"
        assert not {"ContentVersionOldId", "FirstPublicationId", "VersionData", "PathOnClient"} & set(METADATA_CSV_HEADER)

    def test_metadata_only_has_file_extension_since_title_usually_lacks_it(self, tmp_path, no_http):
        _, rows = _run_download_file(tmp_path, metadata_only=True)

        assert rows[0]["FileExtension"] == "pdf"

    @pytest.mark.parametrize("file_extension", ["", None])
    def test_missing_file_extension_is_written_as_empty(self, tmp_path, no_http, file_extension):
        _, rows = _run_download_file(tmp_path, True, record=_record(FileExtension=file_extension))

        assert rows[0]["FileExtension"] == ""

    def test_download_url_defaults_to_salesforce_rest_url(self, tmp_path, no_http):
        _, rows = _run_download_file(tmp_path, metadata_only=True)

        assert rows[0]["DownloadUrl"] == f"https://{INSTANCE}/services/data/v60.0/sobjects/ContentVersion/068A/VersionData"

    def test_download_url_follows_configured_pattern(self, tmp_path, no_http):
        _, rows = _run_download_file(tmp_path, metadata_only=True, download_url_pattern=S3_PATTERN)

        assert rows[0]["DownloadUrl"] == f"s3://bucket/OrgId=00Dxx/Period=0/files/0000/{CHECKSUM}.pdf"

    def test_missing_checksum_gives_empty_download_url_but_row_is_written(self, tmp_path, no_http):
        _, rows = _run_download_file(tmp_path, True, S3_PATTERN, record=_record(Checksum=""))

        assert rows[0]["ContentVersionId"] == "068A"
        assert rows[0]["DownloadUrl"] == ""

    def test_title_with_comma_and_quote_round_trips_through_dict_reader(self, tmp_path, no_http):
        title = 'Offer, "final" | v2'

        _, rows = _run_download_file(tmp_path, True, record=_record(Title=title))

        assert rows[0]["Title"] == title
        assert "EXTRA" not in rows[0]


class TestDownloadFileDeployFormat:
    def test_download_mode_keeps_the_columns_deploy_reads(self, tmp_path, fake_http):
        # deploy_functions.py reads these by name; renaming them breaks deploy.py
        result, rows = _run_download_file(tmp_path, metadata_only=False)

        assert result.startswith("Saved")
        assert "EXTRA" not in rows[0]
        assert rows[0]["ContentVersionOldId"] == "068A"
        assert rows[0]["FirstPublicationId"] == "001A"
        assert rows[0]["PathOnClient"] == str(tmp_path / "069A-Report.pdf")

    def test_title_with_comma_and_quote_round_trips_for_deploy(self, tmp_path, fake_http):
        # deploy.py reads files.csv with csv.DictReader defaults, so the writer must use standard quoting
        title = 'Offer, "final" | v2'

        _, rows = _run_download_file(tmp_path, False, record=_record(Title=title))

        assert rows[0]["Title"] == title
        assert "EXTRA" not in rows[0]


class FakeContentVersionSalesforce(FakeSessionSalesforce):
    def __init__(self, records):
        self.records = records

    def query(self, query):
        return {"records": self.records}


class TestFetchFilesTotalSize:
    def test_total_bytes_sums_content_size_of_all_exported_files(self, tmp_path, no_http):
        records = [
            _record(Id="068A", ContentDocumentId="069A", ContentSize=1000),
            _record(Id="068B", ContentDocumentId="069B", ContentSize=2048),
            _record(Id="068C", ContentDocumentId="069C", ContentSize=None),
        ]
        links = [{"ContentDocumentId": r["ContentDocumentId"]} for r in records]

        stats = fetch_files(
            sf=FakeContentVersionSalesforce(records), content_document_links=links,
            folder_output_directory=str(tmp_path) + "/", results_path=str(tmp_path / "files.csv"),
            filename_pattern="{0}{1}-{2}.{3}", metadata_only=True,
        )

        assert stats["total"] == 3
        assert stats["success"] == 3
        assert stats["total_bytes"] == 3048

    def test_no_files_gives_zero_total_bytes(self, tmp_path):
        stats = fetch_files(sf=FakeContentVersionSalesforce([]), content_document_links=[])
        assert stats["total_bytes"] == 0


class TestFormatSize:
    @pytest.mark.parametrize("num_bytes, expected", [
        (0, "0 bytes"),
        (1023, "1023 bytes"),
        (1536, "1.5 KB"),
        (5 * 1024 ** 2, "5.0 MB"),
        (int(2.5 * 1024 ** 3), "2.5 GB"),
        (3 * 1024 ** 4, "3.0 TB"),
    ])
    def test_sizes_use_1024_based_units(self, num_bytes, expected):
        assert format_size(num_bytes) == expected
