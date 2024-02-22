import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col
from datetime import datetime
import os
from src.logging_config import setup_logging
from src.read_data import transform_data  # Make sure this is correctly imported

# Initialize logger for testing
logger = setup_logging('test_data_processing')

@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder.master("local[2]").appName("TestSession").getOrCreate()
    logger.info("Spark session for testing started.")
    yield spark_session
    spark_session.stop()
    logger.info("Spark session for testing stopped.")

def test_transform_data(spark):
    logger.info("Testing the transform_data function.")

    # Define schema for movies and ratings
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

    # Define sample movie data
    movie_data = [(1, "Toy Story (1995)", "Adventure|Animation|Children|Comedy|Fantasy"),
                  (2, "Jumanji (1995)", "Adventure|Children|Fantasy")]
    # Define sample rating data
    rating_data = [(1, 1, 5.0, 964982703),
                   (1, 2, 3.0, 964982703),
                   (2, 1, 2.0, 964982703),
                   (2, 2, 3.0, 964982703)]

    # Create DataFrame for movies and ratings
    movies_df = spark.createDataFrame(movie_data, schema=movie_schema)
    ratings_df = spark.createDataFrame(rating_data, schema=rating_schema)

    # Call the transform_data function from read_data.py
    movie_ratings_df, top_movies_df = transform_data(movies_df, ratings_df)

    # Assert statements to validate the transformation
    assert movie_ratings_df.filter(col("movieId") == 1).select("avg_rating").collect()[0][0] == 3.5, "Avg rating for MovieId 1 should be 3.5"
    assert top_movies_df.filter(col("userId") == 1).count() == 2, "User 1 should have 2 top movies"
    logger.info("Test for transform_data function passed.")
