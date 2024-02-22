import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col
import sys
import os
sys.path.append(os.path.abspath('/Users/mukundpandey/git_repo/newday_de_task/src'))
from read_data import transform_data




@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("TestSession").getOrCreate()

def test_transform_data(spark):
    # Define sample movie data
    movie_data = [(1, "Toy Story (1995)", "Adventure|Animation|Children|Comedy|Fantasy"),
                  (2, "Jumanji (1995)", "Adventure|Children|Fantasy")]

    # Define sample rating data
    rating_data = [(1, 1, 5.0, 964982703),
                   (1, 2, 3.0, 964982703),
                   (2, 1, 2.0, 964982703),
                   (2, 2, 3.0, 964982703)]

    # Define schema for movies
    movie_schema = StructType([
        StructField("movieId", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("genres", StringType(), True),
    ])
    
    # Define schema for ratings
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