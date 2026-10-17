"""Create top-10 unseen movie recommendations for each ALS user."""

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, explode, row_number, round as spark_round
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


MODEL_PATH = "/movielens/models/als_v1"
RATINGS_PATH = "/movielens/clean/ratings"
MOVIES_PATH = "/movielens/clean/movies"
OUTPUT_PATH = "/movielens/recommendations/top10"
NUM_CANDIDATES = 20
TOP_N = 10

spark = SparkSession.builder.appName("MovieLens10M-Top10").getOrCreate()

ratings_schema = StructType(
    [
        StructField("user_id", IntegerType(), nullable=False),
        StructField("movie_id", IntegerType(), nullable=False),
        StructField("rating", StringType(), nullable=False),
        StructField("rating_ts", StringType(), nullable=False),
    ]
)
movies_schema = StructType(
    [
        StructField("movie_id", IntegerType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("genres", StringType(), nullable=True),
    ]
)

ratings = (
    spark.read.option("sep", "\t")
    .schema(ratings_schema)
    .csv(RATINGS_PATH)
    .select("user_id", "movie_id")
)
movies = (
    spark.read.option("sep", "\t")
    .schema(movies_schema)
    .csv(MOVIES_PATH)
)
model = ALSModel.load(MODEL_PATH)

# Ask ALS for extra candidates so removing already-rated movies still leaves
# up to ten unseen recommendations for most users.
candidates = (
    model.recommendForAllUsers(NUM_CANDIDATES)
    .select("user_id", explode("recommendations").alias("recommendation"))
    .select(
        "user_id",
        col("recommendation.movie_id").alias("movie_id"),
        col("recommendation.rating").alias("predicted_rating"),
    )
)

unseen = candidates.join(ratings, ["user_id", "movie_id"], "left_anti")
ranking = Window.partitionBy("user_id").orderBy(
    col("predicted_rating").desc(), col("movie_id").asc()
)

top_recommendations = (
    unseen.withColumn("recommendation_rank", row_number().over(ranking))
    .filter(col("recommendation_rank") <= TOP_N)
    .join(movies, "movie_id", "inner")
    .select(
        "user_id",
        "recommendation_rank",
        "movie_id",
        "title",
        "genres",
        spark_round(col("predicted_rating"), 4).alias("predicted_rating"),
    )
)

top_recommendations.write.mode("errorifexists").option("sep", "\t").csv(
    OUTPUT_PATH
)
print(f"saved_recommendations={OUTPUT_PATH}")

spark.stop()
