import tarfile
import zipfile
from pathlib import Path

from lte_pm_platform.pipeline.ingest.zip_reader import iter_csv_members


def test_iter_csv_members_only_yields_csv_files(tmp_path: Path) -> None:
    zip_path = tmp_path / "sample.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("a.csv", "COLLECTTIME,C380340003\n20260304143000,1\n")
        archive.writestr("nested/b.csv", "COLLECTTIME,C380340003\n20260304143000,2\n")
        archive.writestr("ignore.txt", "skip")

    members = [(name, stream.read()) for name, stream in iter_csv_members(zip_path)]

    assert [name for name, _ in members] == ["a.csv", "nested/b.csv"]
    assert "20260304143000" in members[0][1]


def test_iter_csv_members_supports_tar_gz_archives(tmp_path: Path) -> None:
    archive_path = tmp_path / "sample.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        csv_path = tmp_path / "a.csv"
        csv_path.write_text("COLLECTTIME,C380340003\n20260304143000,1\n", encoding="utf-8")
        archive.add(csv_path, arcname="inner/a.csv")

    members = [(name, stream.read()) for name, stream in iter_csv_members(archive_path)]

    assert members[0][0] == "inner/a.csv"
    assert "20260304143000" in members[0][1]
