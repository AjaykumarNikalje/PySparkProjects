import pytest
from pyspark.sql import SparkSession, Row
from src.breezebnb.reporting.analytics_reporter import AnalyticsReporter
from pyspark.sql import functions as F
from chispa import assert_df_equality
from decimal import Decimal
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType,IntegerType
# -----------------------------------------------------------------------------
# FIXTURES
# -----------------------------------------------------------------------------

@pytest.fixture
def reporter(tmp_path, spark, mocker):
    """Initialize AnalyticsReporter with a mock logger."""
    logger = mocker.MagicMock()
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    return AnalyticsReporter(logger)
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# TEST get_avg_price_per_neighbourhood()
# -----------------------------------------------------------------------------
def test_get_avg_price_per_neighbourhood_filters_correctly(spark, reporter, tmp_path):
    # Arrange
    data = [
        ("2025-01", "Soho", 20, 150.00, "2025-01-15", "source1.csv", "scheduled", "log1"),
        ("2025-01", "Chelsea", 15, 250.00, "2025-01-15", "source1.csv", "scheduled", "log1"),
        ("2025-02", "Soho", 10, 180.00, "2025-02-10", "source2.csv", "adhoc", "log2"),
    ]

    columns = [
        "year_month", "neighbourhood_cleansed", "listing_count", "avg_price",
        "processing_timestamp", "source_file", "run_type", "final_log_file_name"
    ]

    df = spark.createDataFrame(data, columns)

    # Save mock CSV file (to simulate real input)
    output_path = tmp_path / "input" / "avgPricePerNightPerNeighborhood"
    df.write.mode("overwrite").option("header", True).csv(str(output_path))

    # Act
    result_df = reporter.get_avg_price_per_neighbourhood(spark=spark,input_dir=tmp_path / "input", avg_price_filename="avgPricePerNightPerNeighborhood", year_month="2025-01",neighbourhood="Soho").withColumn("avg_price", F.col("avg_price").cast(DecimalType(10, 2)))
    # Expected DataFrame
    expected_data = [
        ("2025-01", "Soho", "150.00")
    ]
    expected_columns = ["year_month", "neighbourhood_cleansed", "avg_price"]
    expected_df = spark.createDataFrame(expected_data, expected_columns).withColumn("avg_price", F.col("avg_price").cast(DecimalType(10, 2)))

    # Assert (compare DataFrames)
    assert_df_equality(
        result_df,
        expected_df,
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=False
    )

    # Additional sanity checks
    reporter.logger.info.assert_any_call(" Filtered by month: 2025-01")
    reporter.logger.info.assert_any_call(" Filtered by neighbourhood: Soho")


# -----------------------------------------------------------------------------
# TEST get_top_over_under_priced()
# -----------------------------------------------------------------------------
def test_get_top_over_under_priced_returns_top10(spark, reporter, tmp_path):
    # Arrange - Create mock data
    data = [
        (
            "2025-01", "Soho", f"id_{i}", "$100.00", "2025-01-02",
            100 + i, 1, 120.0,
            (100 + i - 120.0),  # price_diff
            ((100 + i - 120.0) / 120.0 * 100),  # pct_diff
            "2025-01-15", "source.csv", "scheduled", "logfile"
        )
        for i in range(10)
    ]

    columns = [
        "year_month", "neighbourhood_cleansed", "id", "price", "last_scraped", "price_num",
        "listing_count", "avg_price", "price_diff", "pct_diff",
        "processing_timestamp", "source_file", "run_type", "final_log_file_name"
    ]

    df = spark.createDataFrame(data, columns)

    # Write mock over/under priced data
    input_dir=tmp_path / "input" 
    over_path = tmp_path / "input" / "top10OverPricedListings"
    under_path = tmp_path / "input" / "top10UnderPricedListings"
    df.write.mode("overwrite").option("header", True).csv(str(over_path))
    df.write.mode("overwrite").option("header", True).csv(str(under_path))
        
    # Act
    over_df, under_df = reporter.get_top_over_under_priced(spark,input_dir,"top10OverPricedListings","top10UnderPricedListings",10,year_month="2025-01", neighbourhood="Soho")

    # Fix schema mismatches by casting
    over_df = over_df.withColumn("price_num", F.col("price_num").cast(DecimalType(10, 2))) \
                    .withColumn("listing_count", F.col("listing_count").cast(IntegerType())) \
                    .withColumn("avg_price", F.col("avg_price").cast(DecimalType(10, 2))) \
                    .withColumn("price_diff", F.col("price_diff").cast(DecimalType(10, 2))) \
                    .withColumn("pct_diff", F.col("pct_diff").cast(DecimalType(10, 2)))

    under_df = under_df.withColumn("price_num", F.col("price_num").cast(DecimalType(10, 2))) \
                    .withColumn("listing_count", F.col("listing_count").cast(IntegerType())) \
                    .withColumn("avg_price", F.col("avg_price").cast(DecimalType(10, 2))) \
                    .withColumn("price_diff", F.col("price_diff").cast(DecimalType(10, 2))) \
                    .withColumn("pct_diff", F.col("pct_diff").cast(DecimalType(10, 2)))

    # Assert
    assert over_df.count() == 10
    assert under_df.count() == 10

    # Validate sorting order
    over_values = [float(row["pct_diff"]) for row in over_df.collect()]
    under_values = [float(row["pct_diff"]) for row in under_df.collect()]
    assert over_values == sorted(over_values, reverse=True)
    assert under_values == sorted(under_values, reverse=False)

    reporter.logger.info.assert_any_call(" Filtered by month: 2025-01")
    reporter.logger.info.assert_any_call(" Filtered by neighbourhood: Soho")


# -----------------------------------------------------------------------------
# TEST Default Behavior (No filters)
# -----------------------------------------------------------------------------

def test_get_avg_price_without_filters(spark, reporter, tmp_path):
    # Arrange
    data = [
        ("2025-01", "Soho", 20, 150.00, "2025-01-15", "source1.csv", "scheduled", "log1"),
        ("2025-02", "Chelsea", 15, 250.00, "2025-02-10", "source2.csv", "adhoc", "log2"),
    ]

    columns = [
        "year_month", "neighbourhood_cleansed", "listing_count", "avg_price",
        "processing_timestamp", "source_file", "run_type", "final_log_file_name"
    ]

    df = spark.createDataFrame(data, columns)

    # Save test input data
    output_path = tmp_path / "input" / "avgPricePerNightPerNeighborhood"
    output_dir=tmp_path / "input" 
    df.write.mode("overwrite").option("header", True).csv(str(output_path))

    # Act
    result_df = reporter.get_avg_price_per_neighbourhood(spark, output_dir,"avgPricePerNightPerNeighborhood", year_month=None, neighbourhood="ALL").withColumn("avg_price", F.col("avg_price").cast(DecimalType(10, 2)))
    result_df.show()
    # Expected DataFrame — only includes selected + ordered columns
    expected_data = [
        ("2025-01", "Soho", 150.00),
        ("2025-02", "Chelsea", 250.00)
    ]
    expected_columns = ["year_month", "neighbourhood_cleansed", "avg_price"]
    expected_df = spark.createDataFrame(expected_data, expected_columns).withColumn("avg_price", F.col("avg_price").cast(DecimalType(10, 2)))
    expected_df.show()
    
    # Assert using chispa
    assert_df_equality(
        result_df,
        expected_df,
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=False
    )

    #Optional sanity checks (for logging)
    reporter.logger.info.assert_any_call("Initialized AnalyticsReporter")

