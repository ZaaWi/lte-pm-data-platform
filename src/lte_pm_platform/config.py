import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_list(name: str) -> tuple[str, ...]:
    value = os.getenv(name, "")
    if not value.strip():
        return ()
    normalized = value.replace("\n", ",")
    items = [item.strip() for item in normalized.split(",")]
    return tuple(item for item in items if item)


@dataclass(frozen=True)
class Settings:
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: int
    ftp_host: str
    ftp_port: int
    ftp_username: str
    ftp_password: str
    ftp_remote_directory: str
    ftp_remote_directories: tuple[str, ...]
    ftp_passive_mode: bool

    @property
    def postgres_dsn(self) -> str:
        return (
            f"dbname={self.postgres_db} "
            f"user={self.postgres_user} "
            f"password={self.postgres_password} "
            f"host={self.postgres_host} "
            f"port={self.postgres_port}"
        )


def get_settings() -> Settings:
    ftp_remote_directories = _env_list("FTP_REMOTE_DIRECTORIES")
    ftp_remote_directory = os.getenv("FTP_REMOTE_DIRECTORY", "/")
    if not ftp_remote_directories and ftp_remote_directory:
        ftp_remote_directories = (ftp_remote_directory,)
    return Settings(
        postgres_db=os.getenv("POSTGRES_DB", "lte_pm"),
        postgres_user=os.getenv("POSTGRES_USER", "lte_pm"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "lte_pm"),
        postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        ftp_host=os.getenv("FTP_HOST", ""),
        ftp_port=int(os.getenv("FTP_PORT", "21")),
        ftp_username=os.getenv("FTP_USERNAME", ""),
        ftp_password=os.getenv("FTP_PASSWORD", ""),
        ftp_remote_directory=ftp_remote_directory,
        ftp_remote_directories=ftp_remote_directories,
        ftp_passive_mode=_env_bool("FTP_PASSIVE_MODE", True),
    )
