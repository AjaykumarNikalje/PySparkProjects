import logging
from pathlib import Path
from datetime import datetime

class Logger:
    """
    Custom logger utility that logs to both console and file.
    Safely handles local and cluster environments.
    """

    def __init__(self, log_dir: str, log_file_name: str, level: str = "INFO", enable_file_log: bool = True):
        self._log_dir = log_dir
        self._log_file_name = log_file_name
        self._level = level.upper()
        self._enable_file_log = enable_file_log
        self._logger = None
        self._initialize_logger()

    @property
    def log_dir(self) -> str:
        return self._log_dir

    @property
    def log_file_name(self) -> str:
        return self._log_file_name

    @property
    def level(self) -> str:
        return self._level

    @property
    def enable_file_log(self) -> bool:
        return self._enable_file_log

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    # ----------------------------------------
    # Internal Setup
    # ----------------------------------------
    def _initialize_logger(self):
        """Initialize the logger with safe directory creation."""
        log_dir_path = Path(self._log_dir)

        safe_log_path = log_dir_path

        try:
            safe_log_path.mkdir(parents=True, exist_ok=True)
        except OSError:
            # Fallback (e.g., Spark executor)
            tmp_path = Path("/tmp/logs")
            tmp_path.mkdir(parents=True, exist_ok=True)
            safe_log_path = tmp_path

        log_file = safe_log_path / f"{self._log_file_name}.log"

        # --- Configure logger ---
        logger = logging.getLogger("AirbnbPipelineLogger")
        logger.setLevel(self._level)

        # Avoid duplicate handlers
        if not logger.handlers:
            formatter = logging.Formatter(
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )

            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

            # File handler (optional)
            if self._enable_file_log:
                file_handler = logging.FileHandler(log_file)
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)

        logger.info(f"Logger initialized (Level={self._level}, File Logging={self._enable_file_log})")
        logger.info(f"Log File: {log_file}")
        print(f"[DEBUG] Logger writing to file: {log_file}")

        self._logger = logger

    # ----------------------------------------
    # Public Methods
    # ----------------------------------------
    def info(self, message: str):
        self._logger.info(message)

    def warning(self, message: str):
        self._logger.warning(message)

    def error(self, message: str):
        self._logger.error(message)

    def debug(self, message: str):
        self._logger.debug(message)

    def exception(self, message: str):
        self._logger.exception(message)
