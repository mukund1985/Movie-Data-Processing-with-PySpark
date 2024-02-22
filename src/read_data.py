import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, max as max_, min as min_, avg, rank
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Adjusting the path insert to avoid E501
path_to_append = '/Users/mukundpandey/git_repo/newday_de_task/src'
sys.path.insert(0, path_to_append)
from logging_config import get_logger  # noqa: E402 to bypass the E402 error

# Setup logging without trailing whitespace
logger = get_logger('data_processing')

# Setting environment variables
DATA_PATH = os.getenv("DATA_PATH", "data/")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "output/")
VERSIONING = datetime.now().strftime("%Y%m%d%H%M%S")

def transform_data(movies_df, ratings_df):
    try:
        logger.info("Starting data transformation.")
        
        # Adjusting the aggregation to fit within the line length
        ratings_stats = ratings_df.groupBy("movieId").agg(
            max_("rating").alias("max_rating"),
            min_("rating").alias("min_rating"),
            avg("rating").alias("avg_rating")
        )
        
        movie_ratings = movies_df.join(ratings_stats, "movieId")
        
        window_spec = Window.partitionBy("userId").orderBy(col("rating").desc())
        top_movies = ratings_df.withColumn(
            "rank", rank().over(window_spec)
        ).filter(col("rank") <= 3)
        
        logger.info("Data transformation completed successfully.")
        return movie_ratings, top_movies
    except Exception:
        logger.error("Data transformation failed.", exc_info=True)
        raise

if __name__ == "__main__":
    spark = None
    try:
        spark = SparkSession.builder.appName("MovieLens Data Processing").getOrCreate()
        logger.info("Spark session started.")
        
        # Defining schema inline to reduce line length
        movie_schema = StructType([
            StructField("movieId", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("genres", StringType(), True),
        ])
        
        rating_schema = StructType([
            StructField("userId", IntegerType(), True),
            StructField("movieId", IntegerType(), True),
            StructField("rating", FloatType(), True),
            StructField("timestamp", IntegerType(), True),
        ])
        
        # Adjusting read methods to fit within the line length
        movies_df = spark.read.option("delimiter", "::").schema(movie_schema)\
            .csv(os.path.join(DATA_PATH, "movies.dat"))
        ratings_df = spark.read.option("delimiter", "::").schema(rating_schema)\
            .csv(os.path.join(DATA_PATH, "ratings.dat"))
        
        logger.info("Data read successfully.")
        
        movie_ratings_df, top_movies_df = transform_data(movies_df, ratings_df)
        
        movie_ratings_output = os.path.join(OUTPUT_PATH, f"movie_ratings_{VERSIONING}")
        top_movies_output = os.path.join(OUTPUT_PATH, f"top_movies_{VERSIONING}")
        
        movie_ratings_df.write.mode("overwrite").parquet(movie_ratings_output)
        top_movies_df.write.mode("overwrite").parquet(top_movies_output)
        logger.info(f"Data written to {movie_ratings_output} and {top_movies_output}.")
    except Exception:
        logger.error("An error occurred in the main block.", exc_info=True)
    finally:
        if spark:
            spark.stop()
        logger.info("Spark session stopped.")
