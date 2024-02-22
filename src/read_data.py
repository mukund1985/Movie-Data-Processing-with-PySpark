import os
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, max as max_, min as min_, avg, rank
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from src.logging_config import setup_logging

# Set up logging
logger = setup_logging('data_processing')

# Environment variables for configuration
DATA_PATH = os.getenv("DATA_PATH", "data/")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "output/")

def transform_data(movies_df, ratings_df):
    try:
        logger.info("Starting data transformation.")
        # Calculate statistics for each movie
        ratings_stats = ratings_df.groupBy("movieId").agg(
            max_("rating").alias("max_rating"),
            min_("rating").alias("min_rating"),
            avg("rating").alias("avg_rating")
        )
        
        # Join movies with their ratings statistics
        movie_ratings = movies_df.join(ratings_stats, "movieId")
        
        # Rank the movies for each user and get the top 3
        windowSpec = Window.partitionBy("userId").orderBy(col("rating").desc())
        top_movies = ratings_df.withColumn("rank", rank().over(windowSpec)).filter(col("rank") <= 3)
        
        logger.info("Data transformation completed successfully.")
        return movie_ratings, top_movies
    except Exception as e:
        logger.error("Data transformation failed.", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        spark = SparkSession.builder.appName("MovieLens Data Processing").getOrCreate()
        logger.info("Spark session started.")
        
        # Define schemas for movies and ratings
        movieSchema = StructType([
            StructField("movieId", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("genres", StringType(), True),
        ])
        
        ratingSchema = StructType([
            StructField("userId", IntegerType(), True),
            StructField("movieId", IntegerType(), True),
            StructField("rating", FloatType(), True),
            StructField("timestamp", IntegerType(), True),
        ])
        
        # Read the movies and ratings data
        movies_df = spark.read.option("delimiter", "::").schema(movieSchema).csv(os.path.join(DATA_PATH, "movies.dat"))
        ratings_df = spark.read.option("delimiter", "::").schema(ratingSchema).csv(os.path.join(DATA_PATH, "ratings.dat"))
        
        logger.info("Data read successfully.")
        
        # Transform the data
        movie_ratings_df, top_movies_df = transform_data(movies_df, ratings_df)
        
        # Versioned output directory
        movie_ratings_output = os.path.join(OUTPUT_PATH, f"movie_ratings_{VERSIONING}")
        top_movies_output = os.path.join(OUTPUT_PATH, f"top_movies_{VERSIONING}")

        # Writing data with versioning
        movie_ratings_df.write.mode("overwrite").parquet(movie_ratings_output)
        top_movies_df.write.mode("overwrite").parquet(top_movies_output)
        logger.info(f"Data written to {movie_ratings_output} and {top_movies_output}.")

    except Exception as e:
        logger.error("An error occurred in the main block.", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")
