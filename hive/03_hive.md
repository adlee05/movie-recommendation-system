# Stage 3: Hive tables and analytics

The Hive external tables read Pig's tab-separated clean outputs directly from HDFS. External tables leave those files in HDFS when table metadata is dropped.

## Prerequisites

From the project root (`big-data`), confirm Hive and the cleaned HDFS paths are available:

```text
command -v hive
hadoop fs -ls /movielens/clean/ratings /movielens/clean/movies
```

Hive must use the same Hadoop configuration/default filesystem as the HDFS paths. The installed Hive CLI was confirmed to expose the `cli` service; the commands below invoke its default CLI service.

## Create the tables

```text
hive -f movie/hive/01_tables.hql
```

The script creates the `movielens` database and external `ratings` and `movies` tables. Check the table metadata in Hive:

```text
hive
```

At the `hive>` prompt, run:

```sql
USE movielens;
SHOW TABLES;
DESCRIBE ratings;
DESCRIBE movies;
SELECT COUNT(*) FROM ratings;
SELECT COUNT(*) FROM movies;
```

Expected counts are 10,000,054 ratings and 10,681 movies.

## Run the analytical queries

Exit the interactive CLI with `quit;`, then run this from the project root:

```text
hive -f movie/hive/02_analytics.hql
```

It reports overall counts and coverage, the top 20 movies averaging at least 100 ratings, and rating volume/average by year.

If Hive reports that a path cannot be found, check that `hadoop fs -ls` still works and that Hive is loading the same Hadoop configuration. If the CLI cannot start or the metastore fails, capture the Hive error before changing environment or configuration. These Hive files have not yet been executed.
