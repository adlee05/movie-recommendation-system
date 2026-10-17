# Stage 1: Ingest MovieLens 10M into HDFS

The extracted dataset is at `movie/dataset/ml-10M100K/`. Local line counts have been checked: 10,000,054 ratings, 10,681 movies, and 95,580 tags.

## Prerequisites

Run in a normal terminal from the project root (`big-data`):

```text
command -v hadoop
hadoop fs -ls /
```

The `hadoop` executable is currently discoverable in this workspace's `PATH`. The bundled `core-site.xml` sets the default filesystem to `hdfs://localhost:9000`. HDFS must be running and reachable before uploading. The HDFS check from this agent session was blocked by its socket restrictions, so connectivity from your terminal still needs verification.

## Upload

After `hadoop fs -ls /` succeeds, run these commands from the project root. They create `/movielens/raw` and copy each source file without replacing existing HDFS files.

```text
hadoop fs -mkdir -p /movielens/raw
hadoop fs -put movie/dataset/ml-10M100K/ratings.dat /movielens/raw/
hadoop fs -put movie/dataset/ml-10M100K/movies.dat /movielens/raw/
hadoop fs -put movie/dataset/ml-10M100K/tags.dat /movielens/raw/
```

## Verify

```text
hadoop fs -ls -h /movielens/raw
hadoop fs -cat /movielens/raw/ratings.dat | head -3
```

The listing should contain `ratings.dat`, `movies.dat`, and `tags.dat`. Rating rows use `UserID::MovieID::Rating::Timestamp` format.

If `-put` reports that a destination already exists, inspect it with `hadoop fs -ls /movielens/raw` before deciding whether to reuse or remove anything. If `hadoop fs -ls /` cannot connect, diagnose the HDFS service/configuration first. Do not format the NameNode. The bundled `core-site.xml` and `hdfs-site.xml` currently contain different `dfs.namenode.name.dir` values; do not change these as part of ingestion without first confirming which configuration the running HDFS service uses.
