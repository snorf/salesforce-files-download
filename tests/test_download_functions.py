import concurrent.futures

import pytest

from download_functions import (
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
