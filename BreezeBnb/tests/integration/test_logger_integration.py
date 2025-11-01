import logging
import os
import pytest
from pathlib import Path
from breezebnb.utils.logger import Logger
from pathlib import Path

@pytest.fixture
def temp_log_dir(tmp_path):
    """Fixture that provides a clean temporary log directory"""
    log_dir = tmp_path / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir

def test_get_logger_creates_log_file_and_writes():
    """
    Integration test for get_logger using project-level logs/ path.
    Ensures log file is created under ./logs and message is written.
    """

    log_file_name = "test_log"

    # Act — Create logger and write a message
    logger = Logger(str(temp_log_dir), log_file_name, level="INFO", enable_file_log=True)
    logger.info("This is a test log entry")
    
    internal_logger = logger.logger

    # Detect the actual log file from FileHandler
    log_file = None
    for handler in internal_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.flush()
            log_file = Path(handler.baseFilename)

    print(f"Logger is writing to: {log_file}")

    #  If the logger wrote somewhere else, don’t overwrite it
    assert log_file and log_file.exists(), f" Log file was not created: {log_file}"

    #  Verify log file content
    contents = log_file.read_text()
    assert "This is a test log entry" in contents, " Log message missing in file"
    assert "INFO" in contents, " Log level missing in log content"

    print(f" Log file verified successfully: {log_file}")

    #  Clean up (optional)
    log_file.unlink(missing_ok=True)


def test_logger_adds_only_one_set_of_handlers(temp_log_dir):
    """
    Ensures Logger() doesn’t add duplicate handlers on reinitialization.
    """
    log_file_name = "duplicate_handler_test"

    # First call
    logger1 = Logger(str(temp_log_dir), log_file_name)
    handler_count_1 = len(logger1.logger.handlers)

    # Second call
    logger2 = Logger(str(temp_log_dir), log_file_name)
    handler_count_2 = len(logger2.logger.handlers)

    # Both loggers refer to same instance
    assert logger1.logger is logger2.logger, " Logger should return the same logger instance"
    assert handler_count_1 == handler_count_2, f" Duplicate handlers added: {handler_count_1} → {handler_count_2}"

