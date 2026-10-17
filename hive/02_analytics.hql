USE movielens;

-- Overall data volume and coverage.
SELECT
    COUNT(*) AS rating_rows,
    COUNT(DISTINCT user_id) AS distinct_users,
    COUNT(DISTINCT movie_id) AS distinct_rated_movies
FROM ratings;

-- Highly rated movies with enough votes to reduce small-sample effects.
SELECT
    m.movie_id,
    m.title,
    COUNT(*) AS rating_count,
    ROUND(AVG(r.rating), 3) AS average_rating
FROM ratings r
JOIN movies m
    ON r.movie_id = m.movie_id
GROUP BY m.movie_id, m.title
HAVING COUNT(*) >= 100
ORDER BY average_rating DESC, rating_count DESC
LIMIT 20;

-- Rating volume and average rating by calendar year.
SELECT
    FROM_UNIXTIME(rating_ts, 'yyyy') AS rating_year,
    COUNT(*) AS rating_count,
    ROUND(AVG(rating), 3) AS average_rating
FROM ratings
GROUP BY FROM_UNIXTIME(rating_ts, 'yyyy')
ORDER BY rating_year;
