import os
import logging
import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, max as max_, min as min_, avg, rank
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Set up environment variables for paths
DATA_PATH = os.getenv("DATA_PATH", "data/")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "output/")
LOG_DIR = os.getenv("LOG_DIR", "logs/")
VERSIONING = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

# Ensure the logs directory exists
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f"data_processing_{VERSIONING}.log")),
        logging.StreamHandler()
    ]
)

def transform_data(movies_df, ratings_df):
    try:
        logging.info("Starting data transformation.")
        # Calculate statistics for each movie
        ratings_stats = ratings_df.groupBy("movieId").agg(
            max_("rating").alias("max_rating"),
            min_("rating").alias("min_rating"),
            avg("rating").alias("avg_rating")
        ).cache()
        
        # Join movies with their ratings statistics
        movie_ratings = movies_df.join(ratings_stats, on="movieId")
        
        # Define window spec for ranking movies per user
        windowSpec = Window.partitionBy("userId").orderBy(col("rating").desc())
        top_movies = ratings_df.withColumn("rank", rank().over(windowSpec)).where(col("rank") <= 3)
        
        logging.info("Data transformation completed successfully.")
        return movie_ratings, top_movies
    except Exception as e:
        logging.error("Error during data transformation: %s", e, exc_info=True)
        raise

if __name__ == "__main__":
    try:
        spark = SparkSession.builder.appName("MovieLens Data Processing").getOrCreate()
        logging.info("Spark session started.")
        
        # Define schema for movies and ratings
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
        
        # Load data into DataFrames
        movies_df = spark.read.option("delimiter", "::").schema(movieSchema).csv(os.path.join(DATA_PATH, "movies.dat"))
        ratings_df = spark.read.option("delimiter", "::").schema(ratingSchema).csv(os.path.join(DATA_PATH, "ratings.dat"))
        
        # Perform data transformation
        movie_ratings_df, top_movies_df = transform_data(movies_df, ratings_df)
        
        # Define output directories with versioning
        movie_ratings_output = os.path.join(OUTPUT_PATH, f"movie_ratings_{VERSIONING}")
        top_movies_output = os.path.join(OUTPUT_PATH, f"top_movies_{VERSIONING}")
        
        # Write the transformed data to output
        movie_ratings_df.write.mode("overwrite").parquet(movie_ratings_output)
        top_movies_df.write.mode("overwrite").parquet(top_movies_output)
        logging.info("Data written to output successfully.")
        
    except Exception as e:
        logging.error("An error occurred in the main block: %s", e, exc_info=True)
    finally:
        spark.stop()
        logging.info("Spark session stopped.")
