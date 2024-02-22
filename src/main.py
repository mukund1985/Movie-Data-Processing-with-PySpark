from pyspark.sql import SparkSession
import os
from datetime import datetime
from data_processing import transform_data
from logging_config import get_logger

logger = get_logger(__name__)

DATA_PATH = os.getenv("DATA_PATH", "data/")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "output/")
VERSIONING = datetime.now().strftime("%Y%m%d%H%M%S")

def read_data(spark, file_name, schema):
    file_path = os.path.join(DATA_PATH, file_name)
    logger.info(f"Reading data from {file_path}")
    return spark.read.option("delimiter", "::").csv(file_path, schema=schema)

def write_data(df, output_path, file_name):
    full_path = os.path.join(output_path, f"{file_name}_{VERSIONING}")
    logger.info(f"Writing data to {full_path}")
    df.write.mode("overwrite").parquet(full_path)

def main():
    spark = SparkSession.builder.appName("MovieLens Data Processing").getOrCreate()
    
    try:
        movies_df = read_data(spark, "movies.dat", "movieId INT, title STRING, genres STRING")
        ratings_df = read_data(spark, "ratings.dat", "userId INT, movieId INT, rating FLOAT, timestamp LONG")
        
        movie_ratings_df, top_movies_df = transform_data(movies_df, ratings_df)
        
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
