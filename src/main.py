from pyspark.sql import SparkSession
import os
from datetime import datetime
from data_processing import transform_data
from logging_config import get_logger

# Setting up logging
logger = get_logger(__name__)

# Check if we are running in a CI environment
CI_ENV = os.getenv('CI', 'false') == 'true'

# Setting up environment variables
DATA_PATH = os.getenv("DATA_PATH", "data/")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "output/")
VERSIONING = datetime.now().strftime("%Y%m%d%H%M%S")

def read_data(spark, file_name, schema, data_path=DATA_PATH):
    """
    Read data from a file.

    Args:
        spark (SparkSession): Spark session object.
        file_name (str): Name of the file to read.
        schema (str): Schema of the data to be read.
        data_path (str): Path to the data directory. Defaults to DATA_PATH.

    Returns:
        DataFrame: DataFrame containing the read data.
    """
    file_path = os.path.join(data_path, file_name)
    logger.info(f"Reading data from {file_path}")
    return spark.read.option("delimiter", "::").csv(file_path, schema=schema)

def write_data(df, output_path, file_name):
    """
    Write data to a file.

    Args:
        df (DataFrame): DataFrame containing the data to be written.
        output_path (str): Path to the output directory.
        file_name (str): Name of the file to be written.
    """
    if not CI_ENV:  # Skip writing files in CI to save time and resources
        full_path = os.path.join(output_path, f"{file_name}_{VERSIONING}")
        logger.info(f"Writing data to {full_path}")
        df.write.mode("overwrite").parquet(full_path)

def main():
    # Starting Spark session
    spark = SparkSession.builder.appName("MovieLens Data Processing").getOrCreate()
    
    try:
        # In CI, use a smaller dataset if available
        test_data_path = os.getenv("TEST_DATA_PATH", DATA_PATH) if CI_ENV else DATA_PATH

        movies_df = read_data(spark, "movies.dat", "movieId INT, title STRING, genres STRING", data_path=test_data_path)
        ratings_df = read_data(spark, "ratings.dat", "userId INT, movieId INT, rating FLOAT, timestamp LONG", data_path=test_data_path)
        
        movie_ratings_df, top_movies_df = transform_data(movies_df, ratings_df)
        
        # Only write data if not in CI environment
        if not CI_ENV:
            write_data(movie_ratings_df, OUTPUT_PATH, "movie_ratings")
            write_data(top_movies_df, OUTPUT_PATH, "top_movies")
        
        logger.info("Data processing and writing completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session has been stopped.")

if __name__ == "__main__":
    main()
