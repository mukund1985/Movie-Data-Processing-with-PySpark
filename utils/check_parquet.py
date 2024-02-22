from pyspark.sql import SparkSession
import sys
import os
from logging_config import get_logger

def get_log_file_path(data_type):
    log_dir = os.getenv("LOG_DIR", "logs/")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file_name = f"check_parquet_{data_type}.log"
    return os.path.join(log_dir, log_file_name)

def setup_logger(log_file_path):
    return get_logger('check_parquet', log_file=log_file_path)

def read_and_show_parquet(file_path, logger):
    spark = SparkSession.builder.appName("Parquet Check Utility").getOrCreate()
    
    try:
        logger.info(f"Attempting to read Parquet file: {file_path}")
        df = spark.read.parquet(file_path)
        df.show()
        logger.info("Displayed Parquet file contents successfully.")
    except Exception as e:
        logger.error(f"Failed to read or display Parquet file: {str(e)}")
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: check_parquet.py <path_to_parquet_file>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    data_type = "movie_ratings" if "movie_ratings" in file_path else "top_movies"
    log_file_path = get_log_file_path(data_type)
    print(f"Logging details in {log_file_path}")
    logger = setup_logger(log_file_path)
    read_and_show_parquet(file_path, logger)
