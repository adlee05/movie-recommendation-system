CREATE DATABASE IF NOT EXISTS movielens;
USE movielens;

CREATE EXTERNAL TABLE IF NOT EXISTS ratings (
    user_id INT,
    movie_id INT,
    rating DOUBLE,
    rating_ts BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/movielens/clean/ratings';

CREATE EXTERNAL TABLE IF NOT EXISTS movies (
    movie_id INT,
    title STRING,
    genres STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/movielens/clean/movies';
