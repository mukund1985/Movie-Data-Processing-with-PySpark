from pyspark.sql import Window
from pyspark.sql.functions import col, max as max_, min as min_, avg, rank
from logging_config import get_logger

logger = get_logger('data_processing')

def transform_data(movies_df, ratings_df):
    try:
        logger.info("Starting data transformation.")
        
        ratings_stats = ratings_df.groupBy("movieId").agg(
            max_("rating").alias("max_rating"),
            min_("rating").alias("min_rating"),
            avg("rating").alias("avg_rating")
        )
        
        movie_ratings = movies_df.join(ratings_stats, "movieId")
        
        window_spec = Window.partitionBy("userId").orderBy(col("rating").desc())
        top_movies = ratings_df.withColumn("rank", rank().over(window_spec)).filter(col("rank") <= 3)
        
        logger.info("Data transformation completed successfully.")
        return movie_ratings, top_movies
    except Exception as e:
        logger.error(f"Data transformation failed: {str(e)}", exc_info=True)
        raise
