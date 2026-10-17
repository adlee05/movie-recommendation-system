"""Train and evaluate an ALS rating model from Pig's clean HDFS output."""

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, LongType, StructField, StructType


RATINGS_PATH = "/movielens/clean/ratings"
MODEL_PATH = "/movielens/models/als_v1"

schema = StructType(
    [
        StructField("user_id", IntegerType(), nullable=False),
        StructField("movie_id", IntegerType(), nullable=False),
        StructField("rating", DoubleType(), nullable=False),
        StructField("rating_ts", LongType(), nullable=False),
    ]
)

spark = SparkSession.builder.appName("MovieLens10M-ALS").getOrCreate()

ratings = (
    spark.read.option("sep", "\t")
    .schema(schema)
    .csv(RATINGS_PATH)
    .select("user_id", "movie_id", "rating")
)

training, test = ratings.randomSplit([0.8, 0.2], seed=42)

als = ALS(
    userCol="user_id",
    itemCol="movie_id",
    ratingCol="rating",
    rank=20,
    maxIter=10,
    regParam=0.1,
    seed=42,
    coldStartStrategy="drop",
    nonnegative=True,
)

model = als.fit(training)
predictions = model.transform(test)

evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction",
)
rmse = evaluator.evaluate(predictions)
print(f"test_rmse={rmse:.4f}")

model.write().save(MODEL_PATH)
print(f"saved_model={MODEL_PATH}")

spark.stop()
