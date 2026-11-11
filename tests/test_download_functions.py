import concurrent.futures

import pytest

from download_functions import (
    build_file_type_condition,
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
