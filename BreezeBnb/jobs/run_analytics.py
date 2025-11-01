# jobs/run_analytics.py
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from breezebnb.utils.logger import Logger
from breezebnb.data_access.data_reader import DataReader
from breezebnb.data_access.data_writer import DataWriter
from breezebnb.reporting.analytics_reporter import AnalyticsReporter
from breezebnb.utils.config import Config

class AirbnbAnalyticsJob:
    """
    Airbnb Analytics Job
    --------------------
    Reads pipeline outputs and generates insights for business users.
    """

    def __init__(self, config_file_name):
        print(f"[DEBUG] config_path type: {type(config_file_name)}, value: {config_file_name}")
        # Load config from YAML file
        self.config = Config(config_file_name)
        self.logger=self._get_logger(self.config)
        
    def get_spark(self,appName):
            spark = (
        SparkSession.builder
        .appName(appName)
        .master("local[*]")
        .getOrCreate()
        )
            return spark    

        
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
    
    def stop(self,spark):
        """Stop Spark session cleanly"""
        if spark:
            spark.stop()
            print("Spark session stopped.")
    
    def run(self, year_month: str = None, neighbourhood: str = None,topN=10):
        
        try:
            input_path = self.config.output_path
            avg_price_filename = self.config.avg_price_filename
            topN_overpriced_filename=self.config.topN_overpriced_filename
            topN_underpriced_filename = self.config.topN_underpriced_filename
            
            spark = self.get_spark("AirbnbMetricsPipeline")
        
            reader = DataReader(self.logger)
            reporter = AnalyticsReporter(self.logger)  

            self.logger.info(f" Year-Month: {year_month}")
            self.logger.info(f" Neighbourhood: {neighbourhood}")

            self.logger.info(f" Starting Airbnb Analytics Job for {year_month} ...")
            self.logger.info(f" Input Directory: {input_path}")
            
            # Average Price Report
            avg_df = reporter.get_avg_price_per_neighbourhood(spark=spark,input_dir=input_path,avg_price_filename=avg_price_filename,year_month=year_month, neighbourhood=neighbourhood)
            
            self.logger.info(" Generated Average Price Report")
            avg_df.show(10, truncate=False)

            # Top Over / Under Priced Listings
            over_df, under_df = reporter.get_top_over_under_priced(
                spark,
                input_path,
                topN_overpriced_filename,
                topN_underpriced_filename,
                topN,
                year_month,
                neighbourhood 
                )
            
            self.logger.info(" Generated Top N Over/Under Priced Listings")
            self.logger.info(" Top N Over-Priced Listings:")
            over_df.show(50, truncate=False)
            self.logger.info(" Top N Under-Priced Listings:")
            under_df.show(50, truncate=False)

            self.logger.info(" All reports successfully shown to user.")

        except Exception as e:
            self.logger.error(f" Analytics Job Failed: {e}", exc_info=True)
            raise

        finally:
            self.stop(spark)
            self.logger.info(" Spark Session Stopped.")


# --------------------------------------------------------------------------
# MAIN ENTRY POINT
# --------------------------------------------------------------------------
def main(year_month: str = None, neighbourhood: str = None, config_path: str ="",topN=10):
    """
    Main entry point for Airbnb Analytics Job.
    Can be run via:
        python3 -m jobs.run_analytics 2025-01 Soho
    """
    
    # Load config from YAML file
    job = AirbnbAnalyticsJob(config_path)
    job.run(year_month, neighbourhood,topN)


if __name__ == "__main__":
    #  CLI arguments like: python3 -m jobs.run_analytics 2025-01 Soho
    year_month = None
    neighbourhood = None
    config="config.yaml"
    topN=10
    
    if len(sys.argv) > 1:
        year_month = sys.argv[1]
    else:
        year_month = datetime.now().strftime("%Y-%m")

    if len(sys.argv) > 2:
        neighbourhood = sys.argv[2]
    else:
        neighbourhood = "ALL"
        
    if len(sys.argv) > 3:
        config = sys.argv[3]
    else:
        config = ""
    
    if len(sys.argv) > 4:
        topN = sys.argv[4]
    else:
        topN = 10
        
    print(year_month)
    print(neighbourhood)    
    print(config)    
    print(topN)    

    main(year_month,neighbourhood,config,topN)
