# MovieLens 10M Recommendation Pipeline

## Abstract

This project builds an end-to-end movie recommendation pipeline with Apache Hadoop HDFS, Apache Pig, Apache Spark MLlib, and Apache Hive. The MovieLens 10M ratings and movie metadata are ingested into HDFS, validated and converted into typed tab-separated records, used to train an explicit-feedback Alternating Least Squares (ALS) model, and scored to generate ranked recommendations for users. Hive exposes the recommendation output as a queryable external table.

The pipeline completed successfully on 17 October 2026. The cleaned ratings output contained 10,000,054 records and the cleaned movie metadata contained 10,681 records. The recommendation output contained 698,742 rows. Offline evaluation reported a test RMSE of 0.7983 and macro Precision@10, Recall@10, and NDCG@10 of 0.0084, 0.0132, and 0.0090 respectively. The ranking metrics are based on a 20-candidate pool and count held-out ratings of at least four stars as relevant.

## Project objectives

- Store the source data in HDFS and process it with a distributed data workflow.
- Validate and normalize ratings and movie metadata before model training.
- Train an ALS collaborative filtering model from user–movie ratings.
- Generate up to ten unseen recommendations per user, with titles and genres.
- Query recommendation output through Hive.
- Evaluate rating prediction and top-N ranking on held-out interactions.

## Dataset

The project uses the MovieLens 10M dataset. The input files use the delimiter `::`:

- `ratings.dat`: user ID, movie ID, rating, and Unix timestamp.
- `movies.dat`: movie ID, title, and genre string.
- `tags.dat`: user-generated tags; it was ingested into HDFS but is not used by the current cleaning, ALS, or recommendation jobs.

The cleaned HDFS paths are `/movielens/clean/ratings` and `/movielens/clean/movies`. Pig output verification matched the source record counts: 10,000,054 ratings and 10,681 movies. Each cleaned record is tab-separated. Ratings have the fields `user_id`, `movie_id`, `rating`, and `rating_ts`; movie records have `movie_id`, `title`, and `genres`.

## Architecture and workflow

1. **HDFS ingestion.** Raw dataset files were placed under `/movielens/raw/`.
2. **Pig ETL.** `movie/pig/movielens_etl.pig` extracts fields from the delimiter-based records, validates numeric IDs, rating range, timestamps, and required movie titles, casts fields to typed values, and writes tab-separated clean outputs.
3. **Hive tables and analytics.** Hive external tables describe the cleaned ratings and movies. A separate external table exposes recommendation output. Hive's recommendation table registration and sample query were verified. An earlier Hive aggregation query failed with a generic error, so the analytical aggregate queries are not claimed as verified.
4. **ALS training.** Spark reads cleaned ratings, uses a seeded 80/20 random split, trains an explicit-feedback ALS model, evaluates rating predictions, and saves the model to `/movielens/models/als_v1`.
5. **Top-N recommendation generation.** Spark loads the saved model, requests 20 candidates per model user, removes any movie present in that user's complete ratings history, keeps up to ten candidates, joins movie metadata, and writes tab-separated output to `/movielens/recommendations/top10`.
6. **Offline evaluation.** Spark loads the saved model and scores rating prediction and ranking quality on a recreated seeded holdout split. A held-out rating of at least 4.0 is treated as relevant.

## Model configuration

The ALS job uses the following parameters:

| Parameter | Value |
| --- | ---: |
| Factor rank | 20 |
| Maximum iterations | 10 |
| Regularization parameter | 0.1 |
| Random seed | 42 |
| Nonnegative factors | Enabled |
| Cold-start prediction handling | Drop predictions without a known factor |
| Training/test split | 80% / 20%, seed 42 |

The recommendation and ranking evaluation jobs use a 20-item candidate pool to fit the available local memory. Recommendation output is capped at ten rows per user after previously rated items are removed. Consequently, some users can receive fewer than ten recommendations.

## Results

### Data and pipeline results

| Item | Result |
| --- | ---: |
| Clean ratings | 10,000,054 rows |
| Clean movies | 10,681 rows |
| Recommendation output | 698,742 rows |
| Recommendation part files | 4 |
| HDFS success marker | Present |
| Hive recommendation table | `movielens.top_recommendations` registered |
| Hive sample query | 20 rows returned |

The recommendation table columns are `user_id`, `recommendation_rank`, `movie_id`, `title`, `genres`, and `predicted_rating`. Sample output included ranked results for users 1 and 6. ALS dot-product scores are ranking scores; they are not probabilities, and values may exceed the original 0.5–5.0 rating range.

### Offline model evaluation

The successful evaluation process exited with code 0 and reported:

| Metric | Result | Interpretation |
| --- | ---: | --- |
| Test RMSE | 0.7983 | Root mean squared error on held-out rating predictions retained by the cold-start strategy. |
| Precision@10 | 0.0084 | On average, 0.84% of the ten recommendation slots matched a held-out rating of at least 4.0. |
| Recall@10 | 0.0132 | On average, 1.32% of each evaluated user's relevant held-out movies appeared in the top ten. |
| NDCG@10 | 0.0090 | Rank-sensitive normalized gain for held-out relevant movies in the top ten. |
| Users evaluated for ranking | 68,176 | Users with at least one held-out rating of 4.0 or higher. |

Precision, recall, and NDCG are macro-averaged over users who had at least one relevant test rating. For precision, the denominator is ten recommendation slots. Ranking begins with only 20 ALS candidates, removes movies seen in the training split, and then evaluates up to ten remaining items. This restricted candidate pool can reduce the measured metrics.

### RMSE consistency note

The original training run was reported as `test_rmse=0.8225`; the later evaluation run against the saved model reported `test_rmse=0.7983`. These values differ, so they should not be presented as the same run's result. The evaluation script recreates the 80/20 split with seed 42, but exact split/model correspondence was not independently verified across the two executions. Use 0.7983 as the metric emitted by the recorded evaluation run, and investigate split reproducibility before claiming it reproduces the original training metric.

## Technical environment

The verified local run used Spark 4.2.0 and Java 21.0.12.1+1. PySpark used the project virtual environment at `movie/spark/.venv`, where NumPy 2.5.4 was installed. Jobs accessed HDFS at `hdfs://localhost:9000`. Spark training and evaluation used local execution; recommendation generation succeeded with four local workers after the initial run exhausted the default Java heap.

## Reproduction commands

Run commands from the project root. The data, model, and recommendation output must already exist at the HDFS paths listed above. The recommendation path is write-once for the current script: it fails if the output directory already exists.

### Train ALS

```bash
PYSPARK_DRIVER_PYTHON="$PWD/movie/spark/.venv/bin/python" PYSPARK_PYTHON="$PWD/movie/spark/.venv/bin/python" spark-submit --master 'local[*]' --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 movie/spark/train_als.py
```

The script saves to `/movielens/models/als_v1`. Inspect that path before rerunning because it must not already exist.

### Generate recommendations

```bash
PYSPARK_DRIVER_PYTHON="$PWD/movie/spark/.venv/bin/python" PYSPARK_PYTHON="$PWD/movie/spark/.venv/bin/python" spark-submit --master 'local[4]' --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 movie/spark/recommend_top10.py
```

Verify HDFS output:

```bash
hadoop fs -ls -h /movielens/recommendations/top10
hadoop fs -cat '/movielens/recommendations/top10/part-*' | wc -l
```

### Evaluate the saved model

```bash
PYSPARK_DRIVER_PYTHON="$PWD/movie/spark/.venv/bin/python" PYSPARK_PYTHON="$PWD/movie/spark/.venv/bin/python" spark-submit --master 'local[4]' --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 movie/spark/evaluate_als.py
```

### Register recommendations in Hive

```bash
./installers/apache-hive-4.2.1-bin/bin/beeline -u 'jdbc:hive2://localhost:10000/default' -n adlee -f movie/hive/04_recommendations.hql
```

Then query in Beeline:

```sql
USE movielens;
SELECT * FROM top_recommendations LIMIT 20;
```

## Limitations and future work

- The current evaluation is an offline random interaction split. It does not simulate a strict time-based recommendation task or a live user study.
- The model uses ratings only. Movie tags and other content features are not incorporated.
- A 20-candidate pool was selected to control memory use. Larger candidate pools and a broader candidate-generation strategy should be evaluated before drawing strong conclusions about ranking quality.
- The ranking metrics are low under the chosen relevance definition and candidate setup. Compare against a popularity baseline and tune ALS rank, regularization, and iteration count before claiming recommendation effectiveness.
- Cold-start users or movies are not well served by collaborative filtering alone. A hybrid method or popularity fallback could address this.
- The RMSE discrepancy between the training log and saved-model evaluation should be resolved by persisting or materializing the exact split and recording the training run's metrics alongside the model.
- Hive recommendation table registration and sample reads succeeded; Hive aggregation-query behavior remains unverified in this environment.

## Conclusion

The project demonstrates a working big-data recommendation workflow across HDFS, Pig, Spark MLlib, and Hive. It validates and processes the MovieLens ratings, trains and saves an ALS model, generates hundreds of thousands of ranked recommendations, and exposes them through Hive. The recorded offline metrics provide an initial quality measurement and point to the next work: improve ranking quality, compare with a baseline, and make evaluation split provenance reproducible.
