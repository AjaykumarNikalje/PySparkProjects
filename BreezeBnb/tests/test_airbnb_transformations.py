import pytest
from chispa import assert_df_equality
from src.breezebnb.transformations.airbnb_transformations import AirbnbTransformations
from jobs.run_airbnb_metrics import AirbnbMetricsPipeline
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from decimal import Decimal

def test_clean_and_prepare_removes_invalid_prices(spark):
    """
    Verify that clean_and_prepare filters invalid price and neighbourhood rows,
    and returns expected cleaned DataFrame.
    """

    # --- Arrange (test input data) ---
    data = [
        ("Listing 1", "Soho", "$100.00", "2025-10-01"),   
        ("Listing 2", "Soho", None, "2025-10-01"),        
        ("Listing 3", None, "$80.00", "2025-10-01"),      
        ("Listing 4", "", "$80.00", "2025-10-01"),       
        ("Listing 5", "", "$10.00", "2025-10-01"),       
    ]
    columns = ["name", "neighbourhood_cleansed", "price", "last_scraped"]
    df = spark.createDataFrame(data, columns)

    pipeline = AirbnbMetricsPipeline("2025-01", "adhoc","/Users/ajaykumarnikalje/Desktop/UKStudyProjects/PythonCode/BreezeBnb_Ver1_Test/PySparkProjects/BreezeBnb/jobs/config.yaml")

    # --- Act ---
    cleaned_df, errored_df = AirbnbTransformations.clean_and_prepare(df)

    # --- Expected cleaned data ---
    expected_data = [
    ("Listing 1", "Soho", "$100.00", "2025-10-01",  Decimal("100.00"), "2025-10")
    ]
    
    expected_schema = StructType([
    StructField("name", StringType(), True),
    StructField("neighbourhood_cleansed", StringType(), True),
    StructField("price", StringType(), True),
    StructField("last_scraped", StringType(), True),
    StructField("price_num", DecimalType(10, 2), True),
    StructField("year_month", StringType(), True),
    ])
    
    expected_df = spark.createDataFrame(expected_data,expected_schema)

    # --- Assert ---
    assert_df_equality(
        cleaned_df.select(expected_df.columns),
        expected_df,
        ignore_row_order=True,
        ignore_nullable=True,
        ignore_column_order=True
    )
    
def test_compute_avg_price_by_neighbourhood(spark):
    """
    Test average price computation grouped by year_month and neighbourhood_cleansed.
    """

    # --- Arrange ---
    data = [
        # Month 2025-10
        ("2025-10", "Soho", 100.0),
        ("2025-10", "Soho", 200.0),
        ("2025-10", "Chelsea", 300.0),
        ("2025-10", "Chelsea", 500.0),
        ("2025-10", "Camden", 50.0),
        ("2025-10", "Camden", 150.0),

        # Month 2025-09
        ("2025-09", "Soho", 120.0),
        ("2025-09", "Chelsea", 280.0),
        ("2025-09", "Camden", 80.0),
    ]
    columns = ["year_month", "neighbourhood_cleansed", "price_num"]
    df = spark.createDataFrame(data, columns)

    # --- Act ---
    avg_df = AirbnbTransformations.compute_avg_price(df)

    # --- Expected Output ---
    expected_data = [
        ("2025-10", "Soho", Decimal("150.00")),
        ("2025-10", "Chelsea", Decimal("400.00")),
        ("2025-10", "Camden", Decimal("100.00")),
        ("2025-09", "Soho", Decimal("120.00")),
        ("2025-09", "Chelsea", Decimal("280.00")),
        ("2025-09", "Camden", Decimal("80.00")),
    ]
    expected_columns = ["year_month", "neighbourhood_cleansed", "avg_price"]
    expected_df = spark.createDataFrame(expected_data, expected_columns)

    # --- Assert ---
    assert_df_equality(
        avg_df.select(expected_df.columns),
        expected_df,
        ignore_row_order=True,
        ignore_nullable=True,
        ignore_column_order=True
    )


    
def test_top10_over_and_underpriced_more_than_10(spark):
    
    # Arrange
    data = [
    ("2025-10", "Soho", 80.0, 100.0, -20.0, -20.0),
    ("2025-10", "Soho", 85.0, 100.0, -15.0, -15.0),
    ("2025-10", "Soho", 90.0, 100.0, -10.0, -10.0),
    ("2025-10", "Soho", 95.0, 100.0, -5.0, -5.0),
    ("2025-10", "Soho", 100.0, 100.0, 0.0, 0.0),
    ("2025-10", "Soho", 105.0, 100.0, 5.0, 5.0),
    ("2025-10", "Soho", 110.0, 100.0, 10.0, 10.0),
    ("2025-10", "Soho", 115.0, 100.0, 15.0, 15.0),
    ("2025-10", "Soho", 120.0, 100.0, 20.0, 20.0),
    ("2025-10", "Soho", 125.0, 100.0, 25.0, 25.0),
    ("2025-10", "Soho", 130.0, 100.0, 30.0, 30.0),
    ("2025-10", "Soho", 135.0, 100.0, 35.0, 35.0),
    ("2025-10", "Soho", 140.0, 100.0, 40.0, 40.0),
    ("2025-10", "Soho", 145.0, 100.0, 45.0, 45.0),
    ("2025-10", "Soho", 150.0, 100.0, 50.0, 50.0),
    ("2025-10", "Soho", 155.0, 100.0, 55.0, 55.0),
    ("2025-10", "Soho", 160.0, 100.0, 60.0, 60.0),
    ("2025-10", "Soho", 165.0, 100.0, 65.0, 65.0),
    ("2025-10", "Soho", 170.0, 100.0, 70.0, 70.0),
    ("2025-10", "Soho", 175.0, 100.0, 75.0, 75.0),
    ("2025-10", "Soho", 180.0, 100.0, 80.0, 80.0),
    ("2025-10", "Soho", 185.0, 100.0, 85.0, 85.0),
    ("2025-10", "Soho", 190.0, 100.0, 90.0, 90.0),
    ("2025-10", "Soho", 195.0, 100.0, 95.0, 95.0),
    ]


    expected_overpriced_data = [
    ("2025-10", "Soho", 150.0, 100.0, 50.0, 50.0),
    ("2025-10", "Soho", 155.0, 100.0, 55.0, 55.0),
    ("2025-10", "Soho", 160.0, 100.0, 60.0, 60.0),
    ("2025-10", "Soho", 165.0, 100.0, 65.0, 65.0),
    ("2025-10", "Soho", 170.0, 100.0, 70.0, 70.0),
    ("2025-10", "Soho", 175.0, 100.0, 75.0, 75.0),
    ("2025-10", "Soho", 180.0, 100.0, 80.0, 80.0),
    ("2025-10", "Soho", 185.0, 100.0, 85.0, 85.0),
    ("2025-10", "Soho", 190.0, 100.0, 90.0, 90.0),
    ("2025-10", "Soho", 195.0, 100.0, 95.0, 95.0),
    ]

    expected_underpriced_data = [
        ("2025-10", "Soho", 80.0, 100.0, -20.0, -20.0),
        ("2025-10", "Soho", 85.0, 100.0, -15.0, -15.0),
        ("2025-10", "Soho", 90.0, 100.0, -10.0, -10.0),
        ("2025-10", "Soho", 95.0, 100.0, -5.0, -5.0),
        ("2025-10", "Soho", 100.0, 100.0, 0.0, 0.0),
        ("2025-10", "Soho", 105.0, 100.0, 5.0, 5.0),
        ("2025-10", "Soho", 110.0, 100.0, 10.0, 10.0),
        ("2025-10", "Soho", 115.0, 100.0, 15.0, 15.0),
        ("2025-10", "Soho", 120.0, 100.0, 20.0, 20.0),
        ("2025-10", "Soho", 125.0, 100.0, 25.0, 25.0),
    ]


    columns = ["year_month", "neighbourhood_cleansed", "price_num", "avg_price","price_diff","pct_diff"]
    df = spark.createDataFrame(data, columns)
    over_df = spark.createDataFrame(expected_overpriced_data, columns).withColumn("price_num", F.col("price_num").cast(DecimalType(10, 2))) \
                    .withColumn("avg_price", F.col("avg_price").cast(DecimalType(10, 2))) 
                    
    under_df = spark.createDataFrame(expected_underpriced_data, columns).withColumn("price_num", F.col("price_num").cast(DecimalType(10, 2))) \
                    .withColumn("avg_price", F.col("avg_price").cast(DecimalType(10, 2))) 

    # Act
    top_over, top_under = AirbnbTransformations.get_top10_over_under(df,10)
    
    top_over=top_over.select(columns).withColumn("price_num", F.col("price_num").cast(DecimalType(10, 2))) \
                    .withColumn("avg_price", F.col("avg_price").cast(DecimalType(10, 2))) 
    top_under=top_under.select(columns).withColumn("price_num", F.col("price_num").cast(DecimalType(10, 2))) \
                    .withColumn("avg_price", F.col("avg_price").cast(DecimalType(10, 2))) 
    
    # Assert (compare DataFrames)
    assert_df_equality(
        over_df,
        top_over,
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=False
    )
    
    assert_df_equality(
        under_df,
        top_under,
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=False
    )
    







