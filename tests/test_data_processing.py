import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col
from src.data_processing import transform_data
from src.logging_config import get_logger

# Adjust the path to include the 'src' directory where 'data_processing.py' is located
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# Setting up logging for this test module
logger = get_logger('test_data_processing')

@pytest.fixture(scope="session")
def spark():
    """
    Pytest fixture for creating a Spark session.
    """
    spark_session = SparkSession.builder.master("local[2]").appName("TestSession").getOrCreate()
    logger.info("Spark session for testing started.")
    yield spark_session
    spark_session.stop()
    logger.info("Spark session for testing stopped.")

def test_transform_data(spark):
    """
    Test the transform_data function for correctness.
    """
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

    # Define sample data
    movie_data = [(1, "Toy Story (1995)", "Adventure|Animation|Children|Comedy|Fantasy"),
                  (2, "Jumanji (1995)", "Adventure|Children|Fantasy")]
    rating_data = [(1, 1, 5.0, 964982703),
                   (1, 2, 3.0, 964982703),
                   (2, 1, 2.0, 964982703),
                   (2, 2, 3.0, 964982703)]

    # Create DataFrames
    movies_df = spark.createDataFrame(movie_data, schema=movie_schema)
    ratings_df = spark.createDataFrame(rating_data, schema=rating_schema)

    # Perform transformation using the function from 'data_processing.py'
    movie_ratings_df, top_movies_df = transform_data(movies_df, ratings_df)

    # Assertions to validate the transformation logic
    assert movie_ratings_df.filter(col("movieId") == 1).select("avg_rating").collect()[0][0] == 3.5, \
        "Avg rating for MovieId 1 should be 3.5"
    assert top_movies_df.filter(col("userId") == 1).count() == 2, \
        "User 1 should have 2 top movies"
    
    logger.info("Test for transform_data function passed.")
