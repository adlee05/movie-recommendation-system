# Stage 4: Spark ALS training and evaluation

This job reads the cleaned, tab-separated ratings from HDFS, makes a seeded 80/20 train/test split, fits explicit-feedback ALS, computes test RMSE, and saves the model to HDFS. Hive aggregation queries are not a dependency; this job reads `/movielens/clean/ratings` directly.

## Prerequisites

Run from the project root (`big-data`) in a normal terminal:

```text
command -v spark-submit
command -v java
hadoop fs -ls /movielens/clean/ratings
```

The installed Spark bundle's `spark-submit --version` could not be checked from the agent session because that session blocks Spark's local network-interface lookup. Run the version check in your terminal if needed. The Spark job must run with Python/PySpark available and able to access HDFS.

PySpark ML requires NumPy in the Python environment used by Spark. The project-local virtual environment at `movie/spark/.venv` contains NumPy 2.5.4, and its import was verified:

```text
movie/spark/.venv/bin/python -c 'import numpy; print(numpy.__version__)'
```

The venv keeps this dependency separate from system Python and does not change PATH or shell startup files.

The current bundled Hadoop config has `fs.defaultFS=hdfs://localhost:9000`; the command below passes that verified value to Spark for this run only and does not modify environment files.

## Run the model

```text
PYSPARK_DRIVER_PYTHON="$PWD/movie/spark/.venv/bin/python" PYSPARK_PYTHON="$PWD/movie/spark/.venv/bin/python" spark-submit --master 'local[*]' --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 movie/spark/train_als.py
```

`local[*]` runs Spark locally using the available CPU cores. Training 10 million ratings may take several minutes and needs sufficient memory. Successful output includes a numeric `test_rmse=...` and `saved_model=/movielens/models/als_v1`.

## Verify the saved model

```text
hadoop fs -ls -R /movielens/models/als_v1
```

The model directory should contain Spark metadata and factor data files. This run's HDFS listing confirmed `metadata`, `itemFactors`, and `userFactors` with `_SUCCESS` markers. The script prints its RMSE before saving; the exact metric wasn't included in the shared output. The model path must not already exist on reruns; inspect it before choosing whether to retain it, remove it, or save another run under a different path.

If Spark fails before starting, capture the first Java/Python exception and `spark-submit --version` output. If it starts but cannot read HDFS, verify the HDFS service and confirm the Hadoop endpoint passed in `spark.hadoop.fs.defaultFS`. If the Python worker reports a version mismatch, share the complete message before changing Python or Spark settings.
