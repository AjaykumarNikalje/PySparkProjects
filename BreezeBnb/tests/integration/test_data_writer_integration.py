import os
import logging
import shutil
import pytest
from pyspark.sql import SparkSession, Row
from breezebnb.data_access.data_writer import DataWriter

@pytest.fixture
def temp_output_dir(tmp_path):
    """Temporary directory for test outputs."""
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    return str(output_dir)

@pytest.fixture
def logger(tmp_path):
    """Temporary logger writing to file."""
    log_file = tmp_path / "writer_test.log"
    logger = logging.getLogger("TestDataWriterLogger")
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def test_write_csv_creates_file(spark, temp_output_dir, logger):
    # Arrange
    data = [
        (1, "Alice", 100.0),
        (2, "Bob", 150.0)
    ]
    columns = ["id", "name", "price"]
    df = spark.createDataFrame(data, columns)

    writer = DataWriter(logger)
    subdir = "output"

    # Act
    writer.write_csv(df,subdir,temp_output_dir)

    # Assert
    full_path = os.path.join(temp_output_dir, subdir)
    files = os.listdir(full_path)
    assert any(f.endswith(".csv") or f.startswith("part") for f in files), \
        f"No CSV file found in {full_path}"

    logger.info(" test_write_csv_creates_file passed successfully")

def test_write_parquet_creates_file(spark, temp_output_dir, logger):
    # Arrange
    data = [
        (1, "Charlie", 200.0),
        (2, "Dana", 250.0)
    ]
    columns = ["id", "name", "price"]
    df = spark.createDataFrame(data, columns)

    writer = DataWriter(logger)
    subdir = "parquet_output"

    # Act
    writer.write_parquet(df,subdir,temp_output_dir)

    # Assert
    full_path = os.path.join(temp_output_dir, subdir)
    files = os.listdir(full_path)
    assert any(f.endswith(".parquet") for f in files), \
        f"No Parquet file found in {full_path}"

    logger.info(" test_write_parquet_creates_file passed successfully")

def test_write_csv_invalid_path_raises_error(spark, logger):
    # Arrange
    data = [(1, "Eve", 300.0)]
    columns = ["id", "name", "price"]
    df = spark.createDataFrame(data, columns)

    # Simulate invalid path (no permission)
    invalid_path = "/invalid_directory/test_output"
    subdir = "csv_output"
    writer = DataWriter(logger)

    # Act + Assert
    with pytest.raises(RuntimeError):
        writer.write_csv(df,subdir ,invalid_path)

    logger.info(" test_write_csv_invalid_path_raises_error passed successfully")
