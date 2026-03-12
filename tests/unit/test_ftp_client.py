from datetime import datetime
from pathlib import Path

from lte_pm_platform.pipeline.ingest.ftp_client import FtpClient, is_zte_pm_zip_filename


class FakeFTP:
    def __init__(
        self,
        names: list[str] | None = None,
        payloads: dict[str, bytes] | None = None,
    ) -> None:
        self.names = names or []
        self.payloads = payloads or {}
        self.connected: tuple[str, int] | None = None
        self.logged_in: tuple[str, str] | None = None
        self.passive_mode: bool | None = None
        self.cwd_path: str | None = None
        self.quit_called = False
        self.closed = False

    def connect(self, host: str, port: int) -> None:
        self.connected = (host, port)

    def login(self, user: str, passwd: str) -> None:
        self.logged_in = (user, passwd)

    def set_pasv(self, passive_mode: bool) -> None:
        self.passive_mode = passive_mode

    def cwd(self, remote_directory: str) -> None:
        self.cwd_path = remote_directory

    def nlst(self) -> list[str]:
        return self.names

    def retrbinary(self, command: str, callback) -> None:  # noqa: ANN001
        remote_filename = command.removeprefix("RETR ")
        callback(self.payloads[remote_filename])

    def quit(self) -> None:
        self.quit_called = True

    def close(self) -> None:
        self.closed = True


class FailingFTP(FakeFTP):
    def retrbinary(self, command: str, callback) -> None:  # noqa: ANN001
        callback(b"partial")
        raise RuntimeError("download failed")


def build_client(fake_ftp: FakeFTP) -> FtpClient:
    return FtpClient(
        host="ftp.example.com",
        port=21,
        username="user",
        password="pass",
        remote_directory="/pm",
        passive_mode=True,
        ftp_factory=lambda: fake_ftp,
    )


def test_zte_pm_filename_pattern_matching() -> None:
    assert is_zte_pm_zip_filename("UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1015.tar.gz")
    assert is_zte_pm_zip_filename("/remote/path/UMEID_LTEFDD_PM_COMMON_ZTE_20260305_1015_R1.tar.gz")
    assert not is_zte_pm_zip_filename("UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1015.csv")
    assert not is_zte_pm_zip_filename("random.zip")


def test_list_candidate_files_filters_remote_listing() -> None:
    fake_ftp = FakeFTP(
        names=[
            "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1015_R1.tar.gz",
            "not_pm.zip",
            "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1015.tar.gz",
        ]
    )
    client = build_client(fake_ftp)

    candidates = client.list_candidate_files()

    assert candidates == [
        "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1015.tar.gz",
        "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1015_R1.tar.gz",
    ]
    assert fake_ftp.connected == ("ftp.example.com", 21)
    assert fake_ftp.logged_in == ("user", "pass")
    assert fake_ftp.passive_mode is True
    assert fake_ftp.cwd_path == "/pm"
    assert fake_ftp.quit_called is True


def test_list_candidate_details_applies_latest_only_policy() -> None:
    fake_ftp = FakeFTP(
        names=[
            "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1015.tar.gz",
            "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1015_R1.tar.gz",
            "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1030.tar.gz",
        ]
    )
    client = build_client(fake_ftp)

    candidates = client.list_candidate_details(
        start=datetime(2026, 3, 5, 10, 15),
        end=datetime(2026, 3, 5, 10, 31),
        revision_policy="latest-only",
    )

    assert [(candidate.filename, candidate.revision) for candidate in candidates] == [
        ("UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1015_R1.tar.gz", 1),
        ("UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1030.tar.gz", 0),
    ]


def test_download_file_writes_to_local_path(tmp_path: Path) -> None:
    filename = "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1015.tar.gz"
    payload = b"zip-bytes"
    fake_ftp = FakeFTP(payloads={filename: payload})
    client = build_client(fake_ftp)

    local_path = client.download_file(filename, tmp_path)

    assert local_path == tmp_path / filename
    assert local_path.read_bytes() == payload
    assert fake_ftp.quit_called is True


def test_download_file_cleans_up_temp_file_on_failure(tmp_path: Path) -> None:
    filename = "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1015.tar.gz"
    client = build_client(FailingFTP(payloads={filename: b"unused"}))

    try:
        client.download_file(filename, tmp_path)
    except RuntimeError as exc:
        assert str(exc) == "download failed"
    else:
        raise AssertionError("Expected download failure")

    assert not (tmp_path / filename).exists()
    assert list(tmp_path.glob("*.part")) == []
