import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, max as max_, min as min_, avg, rank

# Importing necessary modules

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
    """
    Perform data transformation.

    Args:
        movies_df (DataFrame): DataFrame containing movie data.
        ratings_df (DataFrame): DataFrame containing ratings data.

    Returns:
        movie_ratings (DataFrame): Transformed DataFrame containing movie ratings statistics.
        top_movies (DataFrame): Transformed DataFrame containing top-rated movies for each user.
    """
    try:
        logger.info("Starting data transformation.")
        
        # Aggregate calculations for ratings
        ratings_stats = ratings_df.groupBy("movieId").agg(
            max_("rating").alias("max_rating"),
            min_("rating").alias("min_rating"),
            avg("rating").alias("avg_rating")
        )
        
        # Join movies with their ratings statistics
        movie_ratings = movies_df.join(ratings_stats, "movieId")
        
        # Ranking movies for each user
        window_spec = Window.partitionBy("userId").orderBy(col("rating").desc())
        top_movies = ratings_df.withColumn("rank", rank().over(window_spec)).filter(col("rank") <= 3)
        
        logger.info("Data transformation completed successfully.")
        return movie_ratings, top_movies
    except Exception:
        logger.error("Data transformation failed.", exc_info=True)
        raise

if __name__ == "__main__":
    # Starting Spark session
    spark = SparkSession.builder.appName("MovieLens Data Processing").getOrCreate()
    logger.info("Spark session started.")
    
    try:
        # Reading movies data
        movies_df = spark.read.option("delimiter", "::").csv(
            os.path.join(DATA_PATH, "movies.dat"),
            schema="movieId INT, title STRING, genres STRING"
        )
        # Reading ratings data
        ratings_df = spark.read.option("delimiter", "::").csv(
            os.path.join(DATA_PATH, "ratings.dat"),
            schema="userId INT, movieId INT, rating FLOAT, timestamp LONG"
        )
        
        logger.info("Data read successfully.")
        
        # Debugging schema and data
        logger.info("Movies DataFrame Schema:")
        movies_df.printSchema()
        logger.info("Movies DataFrame Sample:")
        movies_df.show(5, truncate=False)

        logger.info("Ratings DataFrame Schema:")
        ratings_df.printSchema()
        logger.info("Ratings DataFrame Sample:")
        ratings_df.show(5, truncate=False)
        
        # Proceed with transformation
        movie_ratings_df, top_movies_df = transform_data(movies_df, ratings_df)
        
        # Preparing output paths
        movie_ratings_output = os.path.join(OUTPUT_PATH, f"movie_ratings_{VERSIONING}")
        top_movies_output = os.path.join(OUTPUT_PATH, f"top_movies_{VERSIONING}")
        
        # Writing the transformed data to parquet files
        movie_ratings_df.write.mode("overwrite").parquet(movie_ratings_output)
        top_movies_df.write.mode("overwrite").parquet(top_movies_output)
        
        logger.info(f"Data written to {movie_ratings_output} and {top_movies_output}.")
    except Exception:
        logger.error("An error occurred in the main block.", exc_info=True)
    finally:
        if spark:
            spark.stop()
        logger.info("Spark session stopped.")
