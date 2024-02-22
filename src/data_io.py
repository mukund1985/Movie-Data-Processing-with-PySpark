import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, max as max_, min as min_, avg, rank

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
        print("Movies DataFrame Schema:")
        movies_df.printSchema()
        print("Movies DataFrame Sample:")
        movies_df.show(5, truncate=False)

        print("Ratings DataFrame Schema:")
        ratings_df.printSchema()
        print("Ratings DataFrame Sample:")
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
