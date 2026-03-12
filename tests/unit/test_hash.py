import hashlib
from pathlib import Path

from lte_pm_platform.utils.hash import file_sha256


def test_file_sha256(tmp_path: Path) -> None:
    path = tmp_path / "sample.txt"
    path.write_text("lte-pm\n")

    assert file_sha256(path) == hashlib.sha256(b"lte-pm\n").hexdigest()


def test_file_sha256_is_content_based_not_path_based(tmp_path: Path) -> None:
    first = tmp_path / "a.txt"
    second = tmp_path / "nested" / "b.txt"
    second.parent.mkdir()
    first.write_text("same-content\n")
    second.write_text("same-content\n")

    assert file_sha256(first) == file_sha256(second)
