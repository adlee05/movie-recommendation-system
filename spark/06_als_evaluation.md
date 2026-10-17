# Stage 6: Offline ALS evaluation

`evaluate_als.py` evaluates the saved model against the same seeded 80/20 random split used by `train_als.py`. It reports RMSE for held-out rating prediction and macro Precision@10, Recall@10, and NDCG@10 for ranking. A held-out rating of at least 4.0 is counted as relevant. Ranking uses the model's top 20 candidates per user, removes movies present in that user's training split, and evaluates the first 10 remaining candidates. Metrics average across users with at least one relevant test rating.

Run from the project root:

```text
PYSPARK_DRIVER_PYTHON="$PWD/movie/spark/.venv/bin/python" PYSPARK_PYTHON="$PWD/movie/spark/.venv/bin/python" spark-submit --master 'local[4]' --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 movie/spark/evaluate_als.py
```

The job loads `/movielens/models/als_v1` and writes metrics to stdout; it does not save or overwrite model or recommendation data. Expected output includes `test_rmse`, `precision_at_10`, `recall_at_10`, and `ndcg_at_10`. Because ranking starts with only 20 candidates before filtering training movies, the measured top 10 may contain fewer than ten recommendations for some users.

This evaluation is an offline holdout estimate, not a live-user experiment. The random interaction split can include users or movies with little training history; RMSE omits cold-start predictions according to the model's configured strategy.

## Recorded run

The run completed with Spark exit code 0 and reported:

| Metric | Result |
| --- | ---: |
| Test RMSE | 0.7983 |
| Precision@10 | 0.0084 |
| Recall@10 | 0.0132 |
| NDCG@10 | 0.0090 |
| Users with at least one relevant held-out rating | 68,176 |

The ranking scores are macro averages over those users. Precision@10 is the fraction of ten slots occupied by a held-out rating of at least 4 stars; recall is the fraction of each user's relevant held-out movies retrieved. The 20-candidate pool and filtering of training movies constrain the measured ranking results.
