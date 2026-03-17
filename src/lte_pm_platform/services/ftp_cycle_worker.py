from __future__ import annotations

import logging
import threading

from lte_pm_platform.config import Settings
from lte_pm_platform.db.connection import get_connection
from lte_pm_platform.services.operation_service import OperationService

logger = logging.getLogger(__name__)


class FtpCycleRunWorker:
    def __init__(self, *, settings: Settings, poll_interval_seconds: float = 2.0) -> None:
        self.settings = settings
        self.poll_interval_seconds = poll_interval_seconds
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run_loop, name="ftp-cycle-run-worker", daemon=True)

    def start(self) -> None:
        if self._thread.is_alive():
            return
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=5.0)

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            processed = False
            try:
                with get_connection(self.settings) as connection:
                    service = OperationService(connection=connection, settings=self.settings)
                    claimed = service.claim_next_ftp_cycle_run()
                    if claimed is not None:
                        processed = True
                        run_id = int(claimed["id"])
                        logger.info("ftp_cycle_run_worker_claimed run_id=%s", run_id)
                        service.execute_ftp_cycle_run(run_id=run_id)
            except Exception as exc:  # pragma: no cover - defensive worker boundary
                logger.exception("ftp_cycle_run_worker_error error=%s", exc)
            if not processed:
                self._stop_event.wait(self.poll_interval_seconds)
