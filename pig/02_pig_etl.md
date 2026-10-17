# Stage 2: Pig ETL and validation

`movielens_etl.pig` reads the confirmed raw HDFS files, parses their `::`-delimited rows, filters invalid IDs, rating ranges, timestamps, and empty movie titles, then writes normalized tab-separated data for Hive and Spark.

## Prerequisites

Run from the project root (`big-data`) in a normal terminal. Pig, Hadoop, and a working MapReduce/YARN execution environment must be available.

```text
command -v pig
command -v hadoop
hadoop fs -ls /movielens/raw
```

## Run Pig

```text
pig -x mapreduce movie/pig/movielens_etl.pig
```

Pig should finish both STORE operations successfully. The clean outputs are tab-separated:

- `/movielens/clean/ratings`: `user_id`, `movie_id`, `rating`, `rating_ts`
- `/movielens/clean/movies`: `movie_id`, `title`, `genres`

## Validate HDFS output

```text
hadoop fs -ls -h /movielens/clean/ratings /movielens/clean/movies
hadoop fs -cat '/movielens/clean/ratings/part-*' | wc -l
hadoop fs -cat '/movielens/clean/movies/part-*' | wc -l
```

Expected row counts for the official MovieLens 10M input are 10,000,054 ratings and 10,681 movies. Each output directory should contain one or more `part-*` files and a `_SUCCESS` marker.

If Pig reports that an output directory already exists, inspect it before rerunning; Pig does not overwrite existing STORE destinations. If the job fails to start, capture the Pig/YARN error and check that Hadoop's MapReduce framework and YARN services are available. Do not change Hadoop or Java settings based only on a generic Pig error.
