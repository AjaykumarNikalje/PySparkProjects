import os
import logging
import pytest
from pyspark.sql import SparkSession
from breezebnb.data_access.data_reader import DataReader


@pytest.fixture
def logger(tmp_path):
    """Create a temporary file logger for testing"""
    log_file = tmp_path / "test_log.log"
    logger = logging.getLogger("TestDataReaderLogger")
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def test_read_invalid_path_raises_exception(spark, logger):
    reader = DataReader( logger)
    invalid_path = "resources/input/missing_file.csv"

    with pytest.raises(FileNotFoundError):
        reader.read_listings(spark,invalid_path)

    logger.info(" test_read_invalid_path_raises_exception passed successfully")
