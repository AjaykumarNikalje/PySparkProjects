from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql.types import StructType
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType,IntegerType
#import logging 
import os
from breezebnb.utils.logger import Logger

class DataReader:
    def __init__(self,logger:Logger):
        self.logger = logger
        
    def read_top_over_under_priced(self,spark,input_dir,topN_overpriced_filename,topN_underpriced_filename):
        
            top10_price_diff_schema = StructType([
                StructField("year_month", StringType(), True),               
                StructField("neighbourhood_cleansed", StringType(), True),   
                StructField("id", StringType(), True),                       
                StructField("price", StringType(), True),                    
                StructField("last_scraped", StringType(), True),             
                StructField("price_num", DecimalType(10, 2), True),          
                StructField("listing_count", IntegerType(), True),           
                StructField("avg_price", DecimalType(10, 2), True),          
                StructField("price_diff", DecimalType(10, 2), True),        
                StructField("pct_diff", DecimalType(10, 2), True),           
                StructField("processing_timestamp", StringType(), True),     
                StructField("source_file", StringType(), True),              
                StructField("run_type", StringType(), True),                 
                StructField("final_log_file_name", StringType(), True)       
            ])

            df_over = spark.read.option("header", True).option("quote", '"').option("escape", '"').option("multiLine", True).schema(top10_price_diff_schema).csv(os.path.join(input_dir, topN_overpriced_filename))
            df_under = spark.read.option("header", True).option("quote", '"').option("escape", '"').option("multiLine", True).schema(top10_price_diff_schema).csv(os.path.join(input_dir, topN_underpriced_filename))    
            return df_over,df_under
        
    def read_avg_price_neighborhood(self,spark,input_dir,avg_price_filename):
        
        avg_price_per_neighbourhood_schema = StructType([
            StructField("year_month", StringType(), True),               
            StructField("neighbourhood_cleansed", StringType(), True),   
            StructField("listing_count", IntegerType(), True),           
            StructField("avg_price", DecimalType(10, 2), True),         
            StructField("processing_timestamp", StringType(), True),     
            StructField("source_file", StringType(), True),              
            StructField("run_type", StringType(), True),                 
            StructField("final_log_file_name", StringType(), True)       
        ])

        df = spark.read.option("header", True).option("quote", '"').option("escape", '"').option("multiLine", True).schema(avg_price_per_neighbourhood_schema).csv(os.path.join(input_dir, avg_price_filename))
        return df

    def read_listings(self, spark, input_path: str, schema: StructType = None) -> DataFrame:
        """
        Read Airbnb listings CSV file.
        If a schema is provided, Spark reads using that schema (no type inference).
        If schema is None, Spark infers schema automatically.
        """
        try:
            if not os.path.exists(input_path):
                raise FileNotFoundError(f" Input path not found: {input_path}")
            
            reader = (
                spark.read
                    .option("header", True)
                    .option("quote", '"')
                    .option("escape", '"')
                    .option("multiLine", True)
            )

            if schema:
                reader = reader.schema(schema)
            else:
                reader = reader.option("inferSchema", True)

            df = reader.csv(input_path)
            
            return df
        except FileNotFoundError as e:
            msg = f" File not found while reading: {input_path} | {e}"
            if self.logger:
                self.logger.error(msg)
            raise

        except Exception as e:
            err_msg = f" Failed to read file {input_path}: {str(e)}"
            if self.logger:
                self.logger.error(err_msg)
            raise RuntimeError(err_msg) from e
