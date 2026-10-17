# Stage 5: Top-N recommendations and Hive integration

`recommend_top10.py` loads the saved ALS model, requests 20 candidates per user, excludes movies in that user's ratings, keeps the highest-scoring ten remaining candidates, joins movie titles/genres, and writes tab-separated recommendations to HDFS. Hive can expose that output through `top_recommendations`.

## Generate recommendations

From the project root, use the venv created for the Spark job:

```text
PYSPARK_DRIVER_PYTHON="$PWD/movie/spark/.venv/bin/python" PYSPARK_PYTHON="$PWD/movie/spark/.venv/bin/python" spark-submit --master 'local[4]' --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 movie/spark/recommend_top10.py
```

The script uses the existing model at `/movielens/models/als_v1` and writes rows to `/movielens/recommendations/top10`. It requests 20 candidates per user, then removes previously rated movies and keeps up to ten recommendations. The output columns are user ID, rank, movie ID, title, genres, and predicted rating. The output path must not already exist; the script fails instead of replacing prior results.

Verify the output in HDFS:

```text
hadoop fs -ls -h /movielens/recommendations/top10
hadoop fs -cat '/movielens/recommendations/top10/part-*' | wc -l
```

The directory should contain `part-*` output and `_SUCCESS`; the row count should be positive. Rows are limited to ten per user and exclude movies present in that user's ratings. A user may have fewer than ten unseen rows if enough of their top 20 candidates were already rated.

The run completed successfully on 2026-10-17. HDFS contained `_SUCCESS` and four part files; concatenating them produced 698,742 rows.

## Register and inspect recommendations in Hive

The HiveServer2/Beeline connection used earlier is available in the user's terminal. From the project root, register the external table:

```text
./installers/apache-hive-4.2.1-bin/bin/beeline -u 'jdbc:hive2://localhost:10000/default' -n adlee -f movie/hive/04_recommendations.hql
```

Then query a small sample in Beeline:

```sql
USE movielens;
SELECT * FROM top_recommendations LIMIT 20;
```

Hive's prior aggregation-query failure is not required for this integration; this table definition and sample read are independent of ALS training. Hive registration succeeded: `SHOW TABLES` listed `top_recommendations`, and `SELECT * FROM top_recommendations LIMIT 20` returned 20 rows, including ten ranked recommendations each for users 1 and 6. Recommendation generation and Hive integration are complete. Do not rerun recommendation generation into an existing output path without inspecting that path first.
