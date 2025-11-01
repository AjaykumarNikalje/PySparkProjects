# data_access/data_writer.py
import os
import traceback
from pyspark.sql import DataFrame
from breezebnb.utils.logger import Logger
class DataWriter:
    """
    Handles safe writing of DataFrames to CSV and Parquet formats.
    Includes exception handling and logging for production use.
    """

    def __init__(self, logger:Logger):
        self.logger = logger

    def write_parquet(self, df: DataFrame, subdir: str,base_output_path:str):
        """
        Write a DataFrame to Parquet safely.
        """
        
        try:
            path = os.path.join(base_output_path, subdir)

            #  Ensure directory exists
            os.makedirs(path, exist_ok=True)

            #  Perform the write
            df.write.mode("overwrite").parquet(path)

            msg = f" Successfully written Parquet file: {path}"
            print(msg)
            if self.logger:
                self.logger.info(msg)

        except Exception as e:
            err_msg = f" Failed to write Parquet to {path}: {str(e)}"
            print(err_msg)
            if self.logger:
                self.logger.error(err_msg)
                self.logger.error(traceback.format_exc())
            raise RuntimeError(err_msg) from e

    def write_csv(self, df: DataFrame, subdir: str, base_output_path:str):
        """
        Write a DataFrame to CSV safely.
        Coalesces to 1 file for easy inspection and sharing.
        """
        
        try:
            path = os.path.join(base_output_path, subdir)
            
            #  Ensure directory exists
            os.makedirs(path, exist_ok=True)

            #  Write CSV
            df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)

            msg = f" Successfully written CSV file: {path}"
            
            self.logger.info(msg)

        except Exception as e:
            err_msg = f" Failed to write CSV to {path}: {str(e)}"
            self.logger.error(err_msg)
            self.logger.error(traceback.format_exc())
            raise RuntimeError(err_msg) from e
        
