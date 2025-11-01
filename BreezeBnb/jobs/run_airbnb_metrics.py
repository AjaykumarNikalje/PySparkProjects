# jobs/run_airbnb_metrics.py
import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from breezebnb.utils.logger import Logger
from breezebnb.data_access.data_reader import DataReader
from breezebnb.data_access.data_writer import DataWriter
from breezebnb.transformations.airbnb_transformations import AirbnbTransformations

from breezebnb.utils.meta_logger import log_pipeline_metadata
from breezebnb.utils.config import Config


class AirbnbMetricsPipeline:
    """
    Airbnb Data Engineering Pipeline
    --------------------------------
    Reads listings data, performs cleaning, transformation, and
    saves outputs for business insights.
    """

    def __init__(self, month: str, run_type: str, config_path="config.yaml"):
        self.month = month
        self.run_type = run_type

        # Load config from YAML file
        self.config = Config(config_path)
        self.logger=self._get_logger(self.config)

        
    
    def _get_logger(self,config):
        # --- Logging configuration ---
        enable_file_log = config.enable_file_log
        log_dir = config.log_dir
        level = config.level
        log_file_name = config.log_file_name

        # --- Initialize logger ---
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        final_log_file_name = f"{log_file_name}_{timestamp}"
        
        logger = Logger(
            log_dir,
            final_log_file_name,
            level,
            enable_file_log
        )
        return logger    
    
    def get_input_file_path(self, config,month):
        input_path = config.input_path
        input_file_name = config.input_file_name
        input_file_extension =config.input_file_extension
        file_name = f"{input_file_name}_{month}.{input_file_extension}"
        file_path = os.path.join(input_path, file_name)
        return file_path
            

        
    def run(self,spark):
        """Main execution for Airbnb metrics pipeline."""
        self.logger.info("Starting Airbnb Metrics Pipeline")
        # --- Construct file paths dynamically ---

        try:
            # --- Input/output paths ---
            
            listing_file_path=self.get_input_file_path(self.config,self.month)
            
            output_path = self.config.output_path
            avg_price_filename = self.config.avg_price_filename
            topN_overpriced_filename=self.config.topN_overpriced_filename
            topN_underpriced_filename = self.config.topN_underpriced_filename
            
            error_path = self.config.error_path
            
            # --- Processing configuration ---
            top_k = self.config.top_k
            
            
            # --- Initialize data components ---
            reader = DataReader(self.logger)
            writer = DataWriter(self.logger)
        
            df = (
                reader.read_listings(spark,listing_file_path)
                .selectExpr("id", "neighbourhood_cleansed", "price", "last_scraped")
            )

            raw_count = df.count()
            if raw_count == 0:
                raise ValueError("No data found in input DataFrame. Check input path or filter conditions.")

            self.logger.info(f"Data read successfully: {raw_count} records")

            # --- Step 2: Clean & Transform ---
            df_clean, error_df = AirbnbTransformations.clean_and_prepare(df)
            final_error_records_df = AirbnbTransformations.add_tracking_columns(
                error_df, listing_file_path, self.logger.log_file_name, self.run_type
            )
            
            DataWriter(self.logger).write_csv(final_error_records_df, self.config.error_file_name,error_path)
            self.logger.info("Data cleaned and prepared")

            # --- Step 3: Compute average prices ---
            avg_df = AirbnbTransformations.compute_avg_price(df_clean)
            self.logger.info("Computed average price per neighbourhood per month")

            # --- Step 4: Compute over/under-priced listings ---
            joined_df = AirbnbTransformations.compute_price_diffs(df_clean, avg_df)
            top_over_df, top_under_df = AirbnbTransformations.get_top10_over_under(joined_df, top_k)
            self.logger.info("Identified top 10 over- and under-priced listings")

            # --- Step 5: Add metadata tracking ---
            final_avg_df = AirbnbTransformations.add_tracking_columns(
                avg_df, listing_file_path, self.logger.log_file_name, self.run_type
            )
            final_top_over_df = AirbnbTransformations.add_tracking_columns(
                top_over_df, listing_file_path, self.logger.log_file_name, self.run_type
            )
            final_top_under_df = AirbnbTransformations.add_tracking_columns(
                top_under_df, listing_file_path, self.logger.log_file_name, self.run_type
            )

            # --- Step 6: Write outputs ---
            writer.write_csv(final_avg_df, avg_price_filename,output_path)
            writer.write_csv(final_top_over_df, topN_overpriced_filename,output_path)
            writer.write_csv(final_top_under_df, topN_underpriced_filename,output_path)

            # --- Step 7: Log metadata ---
            log_pipeline_metadata(
                final_log_file_name=self.logger.log_file_name,
                log_dir=self.logger.log_dir,
                month=self.month,
                run_type=self.run_type,
                file_path=listing_file_path,
                status="SUCCESS",
                message=f"Pipeline completed successfully with {raw_count} records."
            )

            self.logger.info("Pipeline completed successfully")

        except Exception as e:
            error_message = f"{type(e).__name__}: {str(e)}"
            self.logger.error(f"Pipeline failed with error: {error_message}")
            self.logger.exception(e)

            log_pipeline_metadata(
                final_log_file_name=self.logger.log_file_name,
                log_dir=self.logger.log_dir,
                month=self.month,
                run_type=self.run_type,
                file_path=listing_file_path,
                status="FAILED",
                message=error_message
            )
            raise
    
    def get_spark(self,appName):
            spark = (
        SparkSession.builder
        .appName(appName)
        .master("local[*]")
        .getOrCreate()
        )
            return spark

    def stop(self,spark):
        """Stop Spark session cleanly"""
        if spark:
            spark.stop()
            print("Spark session stopped.")
            
            

def main(month,run_type,config_path="config.yaml" ):
    
    pipeline = AirbnbMetricsPipeline(month, run_type, config_path)
    
    # --- Initialize Spark ---
    spark=pipeline.get_spark("AirbnbMetricsPipeline")
            
    pipeline.run(spark)
    pipeline.stop(spark)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        month = datetime.now().strftime("%Y-%m")
        run_type = "scheduled"
        config_path="config.yaml"
    else:
        month = sys.argv[1]
        run_type = sys.argv[2] if len(sys.argv) > 2 else "adhoc"
        config_path=sys.argv[3]

    print(f" Month: {month}")
    print(f" Run Type: {run_type}")

    main(month,run_type,config_path)
