from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, max as max_, min as min_, avg, rank

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MovieLens Data Processing") \
    .getOrCreate()

# Define the schema for movies and ratings
movieSchema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True)
])

ratingSchema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", IntegerType(), True)
])

# Read the data into DataFrames using the defined schema
movies_df = spark.read.option("delimiter", "::").schema(movieSchema).csv("/Users/mukundpandey/git_repo/newday_de_task/data/movies.dat")
ratings_df = spark.read.option("delimiter", "::").schema(ratingSchema).csv("/Users/mukundpandey/git_repo/newday_de_task/data/ratings.dat")

# Data processing
# Calculate max, min, and average rating for each movie
ratings_stats_df = ratings_df.groupBy("movieId").agg(
    max_("rating").alias("max_rating"),
    min_("rating").alias("min_rating"),
    avg("rating").alias("avg_rating")
)

# Join movies with rating stats
movie_ratings_df = movies_df.join(ratings_stats_df, on="movieId")

# Find each user's top 3 movies based on ratings
windowSpec = Window.partitionBy("userId").orderBy(col("rating").desc())
top_movies_df = ratings_df.withColumn("rank", rank().over(windowSpec)) \
    .filter(col("rank") <= 3)

# Write the data to disk in an efficient format
movie_ratings_df.write.parquet("/Users/mukundpandey/git_repo/newday_de_task/output/movie_ratings")
top_movies_df.write.parquet("/Users/mukundpandey/git_repo/newday_de_task/output/top_movies")

# Stop the Spark session
spark.stop()
