from pyspark.sql import functions as F
from datetime import datetime
import os
from breezebnb.data_access.data_writer import DataWriter
from pyspark.sql import Window

class AirbnbTransformations:
    
    @staticmethod
    def clean_and_prepare(df):

        
        df = df.withColumn("price_num",
                        F.when(
                            F.trim(F.col("price")).isNotNull() & (F.trim(F.col("price")) != ""),
                            F.round(
                                F.regexp_replace(F.col("price"), r"[\$,]", "").cast("decimal(10,2)"),
                                2
                            )
                        ).otherwise(None)
                        ).withColumn(
                        "last_scraped",
                        F.when(F.trim(F.col("last_scraped"))=="",F.lit(None)).otherwise(F.col("last_scraped"))
                        ).withColumn(
                        "year_month",
                        F.date_format(F.to_date(F.col("last_scraped")), "yyyy-MM")
                        )
                                
        error_records_df = df.filter(
        (F.col("price_num").isNull()) |
        (F.col("last_scraped").isNull()) |
        (F.col("price_num") == 0) |
        (F.trim(F.col("neighbourhood_cleansed")) =="" ) |
        (F.col("neighbourhood_cleansed").isNull())
        )
                        
        df_final= df.filter(
            (F.col("price_num").isNotNull())
            & (F.col("price_num") > 0)
            & (F.col("last_scraped").isNotNull())
            & (F.col("neighbourhood_cleansed") !="")
            #& (F.lower(F.col("neighbourhood_cleansed")) == "soho")
            & (F.col("neighbourhood_cleansed").isNotNull())
        )
        return df_final,error_records_df

    @staticmethod
    def compute_avg_price(df):
        """Compute avg price per neighbourhood per month"""
        return (
            df.groupBy("year_month", "neighbourhood_cleansed")
            .agg(
                F.count("*").alias("listing_count"),
                F.round(F.avg("price_num"), 2).alias("avg_price").cast("decimal(10,2)"),
            )
        )
    
    def compute_price_diffs(df_clean, avg_df):
        """
        Join the cleaned Airbnb listings with the average price per neighbourhood_cleansed and
        compute the absolute and percentage difference for each listing.

        Args:
            df_clean (DataFrame): Cleaned listings DataFrame containing
                                ['id', 'neighbourhood_cleansed', 'price', 'year_month']
            avg_df (DataFrame): Average price DataFrame containing
                                ['year_month', 'neighbourhood_cleansed', 'avg_price']

        Returns:
            DataFrame: Joined DataFrame with columns:
                    ['id', 'neighbourhood_cleansed', 'price', 'avg_price',
                        'price_diff', 'pct_diff', 'year_month']
        """

        # Join on year_month + neighbourhood_cleansed
        joined_df = df_clean.join(
            avg_df, on=["year_month", "neighbourhood_cleansed"], how="inner"
        )

        # Compute absolute and percentage difference
        joined_df = (
            joined_df
            .withColumn("price_diff", F.round(F.col("price_num") - F.col("avg_price"), 2).cast("decimal(10,2)") )
            .withColumn(
                "pct_diff",
                (F.round((F.col("price_num") - F.col("avg_price")) / F.col("avg_price") * 100, 2)).cast("decimal(10,2)")
            )
        )

        return joined_df

    
    @staticmethod
    def get_top10_over_under(df,num):
        """Return two dataframes: top over/underpriced"""
        window_spec_over = Window.partitionBy("neighbourhood_cleansed").orderBy(F.col("pct_diff").desc())
        window_spec_under = Window.partitionBy("neighbourhood_cleansed").orderBy(F.col("pct_diff").asc())
        
        df_over_ranked = (
            df.withColumn("rank", F.row_number().over(window_spec_over))
            .filter(F.col("rank") <= num)
            .drop("rank")
        )
        
        df_under_ranked = (
            df.withColumn("rank", F.row_number().over(window_spec_under))
            .filter(F.col("rank") <= num)
            .drop("rank")
        )
        return df_over_ranked, df_under_ranked    
    
    @staticmethod
    def add_tracking_columns(df, input_path: str, final_log_file_name:str, run_type: str = "scheduled"):
        """
        Adds minimal tracking columns:
        - processing_timestamp: when the data was processed
        - source_file: name of the input file
        - run_type: 'current' or 'backfill'
        """
        df = (
            df.withColumn("processing_timestamp", F.current_timestamp())
            .withColumn("source_file", F.lit(os.path.basename(input_path)))
            .withColumn("run_type", F.lit(run_type))
            .withColumn("final_log_file_name", F.lit(final_log_file_name))
        )
        return df
