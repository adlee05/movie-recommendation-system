"""Evaluate the saved ALS model on the original seeded holdout split."""

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, count, explode, lit, log2, row_number, sum as spark_sum, when
from pyspark.sql.types import DoubleType, IntegerType, LongType, StructField, StructType


RATINGS_PATH = "/movielens/clean/ratings"
MODEL_PATH = "/movielens/models/als_v1"
NUM_CANDIDATES = 20
TOP_N = 10
RELEVANT_RATING = 4.0

schema = StructType(
    [
        StructField("user_id", IntegerType(), nullable=False),
        StructField("movie_id", IntegerType(), nullable=False),
        StructField("rating", DoubleType(), nullable=False),
        StructField("rating_ts", LongType(), nullable=False),
    ]
)

spark = SparkSession.builder.appName("MovieLens10M-ALS-Evaluation").getOrCreate()

ratings = (
    spark.read.option("sep", "\t")
    .schema(schema)
    .csv(RATINGS_PATH)
    .select("user_id", "movie_id", "rating")
)
training, test = ratings.randomSplit([0.8, 0.2], seed=42)
model = ALSModel.load(MODEL_PATH)

# Rating prediction quality on the same seeded holdout split used for training.
predictions = model.transform(test)
rmse = RegressionEvaluator(
    metricName="rmse", labelCol="rating", predictionCol="prediction"
).evaluate(predictions)

# Rank the model's top candidates after removing training interactions, then
# score against held-out ratings of at least four stars.
candidate_window = Window.partitionBy("user_id").orderBy(
    col("recommendation.rating").desc(), col("recommendation.movie_id").asc()
)
candidates = (
    model.recommendForAllUsers(NUM_CANDIDATES)
    .select("user_id", explode("recommendations").alias("recommendation"))
    .withColumn("candidate_rank", row_number().over(candidate_window))
    .filter(col("candidate_rank") <= NUM_CANDIDATES)
    .select(
        "user_id",
        col("recommendation.movie_id").alias("movie_id"),
        "candidate_rank",
    )
)

unseen = candidates.join(training.select("user_id", "movie_id"), ["user_id", "movie_id"], "left_anti")
rank_window = Window.partitionBy("user_id").orderBy("candidate_rank")
top_recommendations = (
    unseen.withColumn("recommendation_rank", row_number().over(rank_window))
    .filter(col("recommendation_rank") <= TOP_N)
)

relevant = test.filter(col("rating") >= RELEVANT_RATING).select("user_id", "movie_id")
relevant_counts = relevant.groupBy("user_id").agg(count(lit(1)).alias("relevant_count"))
hits = (
    top_recommendations.join(relevant, ["user_id", "movie_id"], "inner")
    .groupBy("user_id")
    .agg(
        count(lit(1)).alias("hit_count"),
        spark_sum(1.0 / log2(col("recommendation_rank") + 1)).alias("dcg"),
    )
)

per_user = relevant_counts.join(hits, "user_id", "left").fillna(
    {"hit_count": 0, "dcg": 0.0}
)
ideal_dcg = sum(
    when(col("relevant_count") >= rank, 1.0 / log2(lit(rank + 1))).otherwise(0.0)
    for rank in range(1, TOP_N + 1)
)
metrics = per_user.select(
    (col("hit_count") / lit(float(TOP_N))).alias("precision_at_10"),
    (col("hit_count") / col("relevant_count")).alias("recall_at_10"),
    (col("dcg") / ideal_dcg).alias("ndcg_at_10"),
).agg(
    spark_sum("precision_at_10").alias("precision_sum"),
    spark_sum("recall_at_10").alias("recall_sum"),
    spark_sum("ndcg_at_10").alias("ndcg_sum"),
    count(lit(1)).alias("users_with_relevant_test_ratings"),
)
summary = metrics.first()
evaluated_users = summary["users_with_relevant_test_ratings"]

print(f"test_rmse={rmse:.4f}")
print(f"relevant_rating_threshold={RELEVANT_RATING:.1f}")
print(f"candidate_pool_per_user={NUM_CANDIDATES}")
print(f"users_with_relevant_test_ratings={evaluated_users}")
print(f"precision_at_10={summary['precision_sum'] / evaluated_users:.4f}")
print(f"recall_at_10={summary['recall_sum'] / evaluated_users:.4f}")
print(f"ndcg_at_10={summary['ndcg_sum'] / evaluated_users:.4f}")

spark.stop()
