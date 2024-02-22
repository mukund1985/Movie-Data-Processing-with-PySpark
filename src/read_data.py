from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, max as max_, min as min_, avg, rank
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Function to perform data transformation
def transform_data(movies_df, ratings_df):
    ratings_stats = ratings_df.groupBy("movieId").agg(
        max_("rating").alias("max_rating"),
        min_("rating").alias("min_rating"),
        avg("rating").alias("avg_rating")
    )
    movie_ratings = movies_df.join(ratings_stats, "movieId")
    
    windowSpec = Window.partitionBy("userId").orderBy(col("rating").desc())
    top_movies = ratings_df.withColumn("rank", rank().over(windowSpec)).filter(col("rank") <= 3)
    
    return movie_ratings, top_movies

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MovieLens Data Processing").getOrCreate()
    
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
    
    movies_df = spark.read.option("delimiter", "::").schema(movieSchema).csv("/Users/mukundpandey/git_repo/newday_de_task/data/movies.dat")
    ratings_df = spark.read.option("delimiter", "::").schema(ratingSchema).csv("/Users/mukundpandey/git_repo/newday_de_task/data/ratings.dat")
    
    movie_ratings_df, top_movies_df = transform_data(movies_df, ratings_df)
    
    movie_ratings_df.write.parquet("/Users/mukundpandey/git_repo/newday_de_task/output/movie_ratings")
    top_movies_df.write.parquet("/Users/mukundpandey/git_repo/newday_de_task/output/top_movies")
    
    spark.stop()
