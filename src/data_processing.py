from pyspark.sql import Window
from pyspark.sql.functions import col, max as max_, min as min_, avg, rank
from logging_config import get_logger

# Importing necessary modules

# Setting up logging
logger = get_logger('data_processing')

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
        # Starting data transformation
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
        
        # Data transformation completed successfully
        logger.info("Data transformation completed successfully.")
        return movie_ratings, top_movies
    except Exception as e:
        # Logging error if data transformation fails
        logger.error(f"Data transformation failed: {str(e)}", exc_info=True)
        raise
