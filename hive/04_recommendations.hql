USE movielens;

CREATE EXTERNAL TABLE IF NOT EXISTS top_recommendations (
    user_id INT,
    recommendation_rank INT,
    movie_id INT,
    title STRING,
    genres STRING,
    predicted_rating DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/movielens/recommendations/top10';
