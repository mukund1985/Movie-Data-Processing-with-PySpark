import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col
import sys
import os

# Adjust the import path dynamically
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from src.read_data import transform_data

@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder.master("local[2]").appName("TestSession").getOrCreate()
    yield spark_session
    spark_session.stop()

def test_transform_data(spark):
    # Define sample movie and rating data
    movie_data = [(1, "Toy Story (1995)", "Adventure|Animation|Children|Comedy|Fantasy"),
                  (2, "Jumanji (1995)", "Adventure|Children|Fantasy")]
    rating_data = [(1, 1, 5.0, 964982703),
                   (1, 2, 3.0, 964982703),
                   (2, 1, 2.0, 964982703),
                   (2, 2, 3.0, 964982703)]

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

    # Create DataFrame for movies and ratings
    movies_df = spark.createDataFrame(movie_data, schema=movie_schema)
    ratings_df = spark.createDataFrame(rating_data, schema=rating_schema)

    # Call the transform_data function
    movie_ratings_df, top_movies_df = transform_data(movies_df, ratings_df)

    # Assert statements to validate the transformation
    assert movie_ratings_df.filter(col("movieId") == 1).select("avg_rating").collect()[0][0] == 3.5
    assert top_movies_df.filter(col("userId") == 1).count() == 2