# reporting/analytics_reporter.py
import os
from pyspark.sql import SparkSession, DataFrame, functions as F,Window
from breezebnb.utils.logger import Logger
from breezebnb.data_access.data_reader import DataReader
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType,IntegerType

class AnalyticsReporter:
    """
    Analytics Reporter
    ------------------
    Provides methods to query and summarize Airbnb metrics
    for reporting and dashboard insights.
    """

    def __init__(self,logger=None):
        self.logger = logger or Logger("logs", "analytics_reporter", level="INFO")

    # --------------------------------------------------------------------------
    #  Average Price per Night by Neighbourhood Each Month
    # --------------------------------------------------------------------------
    def get_avg_price_per_neighbourhood(
        self,
        spark,
        input_dir,
        avg_price_filename,
        year_month: str = None,
        neighbourhood: str = None
    ) -> DataFrame:
        """
        Returns average price per night by neighbourhood (optionally filtered by month and neighbourhood).
        """

        try:
            
            reader = DataReader(self.logger)
            df=reader.read_avg_price_neighborhood(spark,input_dir,avg_price_filename)

            if year_month:
                df = df.filter(F.col("year_month") == year_month)
                self.logger.info(f" Filtered by month: {year_month}")

            if neighbourhood!="ALL":
                df = df.filter(F.lower(F.col("neighbourhood_cleansed")) == neighbourhood.lower())
                self.logger.info(f" Filtered by neighbourhood: {neighbourhood}")

            df = df.select("year_month", "neighbourhood_cleansed", "avg_price").orderBy(
                F.desc("avg_price")
            )
            self.logger.info(f" Loaded avgPricePerNightPerNeighborhood: {df.count()} records")
            self.logger.info(f"Initialized AnalyticsReporter")


            return df

        except Exception as e:
            self.logger.error(f" Failed to fetch avg price data: {str(e)}", exc_info=True)
            raise

    # --------------------------------------------------------------------------
    # Top 10 Over / Under Priced Listings
    # --------------------------------------------------------------------------
    def get_top_over_under_priced(
        self,
        spark,
        input_dir,
        topN_overpriced_filename,
        topN_underpriced_filename,
        topN=10,
        year_month: str = None,
        neighbourhood: str = None
    ) -> (DataFrame, DataFrame):
        """
        Returns top 10 over-priced and under-priced listings.
        Filters optionally by year_month and neighbourhood.
        """

        try:
            topN=int(topN)
            reader = DataReader(self.logger)
            self.logger.info(f" Loaded over/under-priced datasets")
            df_over,df_under=reader.read_top_over_under_priced(spark,input_dir,topN_overpriced_filename,topN_underpriced_filename)
            
            # Apply filters if given
            if year_month:
                df_over = df_over.filter(F.col("year_month") == year_month)
                df_under = df_under.filter(F.col("year_month") == year_month)
                self.logger.info(f" Filtered by month: {year_month}")

            if neighbourhood!="ALL":
                df_over = df_over.filter(F.lower(F.col("neighbourhood_cleansed")) == neighbourhood.lower())
                df_under = df_under.filter(F.lower(F.col("neighbourhood_cleansed")) == neighbourhood.lower())
                self.logger.info(f" Filtered by neighbourhood: {neighbourhood}")
                # Return top 10 results (sorted by percentage difference)
                df_over = df_over.orderBy(F.desc("pct_diff")).limit(topN)
                df_under = df_under.orderBy(F.asc("pct_diff")).limit(topN)
            else:
                window_spec_over = Window.partitionBy("neighbourhood_cleansed").orderBy(F.col("pct_diff").desc())
                window_spec_under = Window.partitionBy("neighbourhood_cleansed").orderBy(F.col("pct_diff").asc())
                
                df_over = (
                    df_over.withColumn("rank", F.row_number().over(window_spec_over))
                    .filter(F.col("rank") <= topN)
                    .orderBy(F.asc("neighbourhood_cleansed"), F.desc("pct_diff"))
                )
                
                df_under = (
                    df_under.withColumn("rank", F.row_number().over(window_spec_under))
                    .filter(F.col("rank") <= topN)
                    .orderBy(F.asc("neighbourhood_cleansed"),F.asc("pct_diff"))
                )

            return df_over, df_under

        except Exception as e:
            self.logger.error(f" Failed to fetch over/under-priced data: {str(e)}", exc_info=True)
            raise








