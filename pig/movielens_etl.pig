-- MovieLens 10M: validate and normalize ratings and movie metadata.
-- Input is the official MovieLens 10M `::`-delimited format in HDFS.

ratings_raw = LOAD '/movielens/raw/ratings.dat'
    USING TextLoader()
    AS (line:chararray);

ratings_fields = FOREACH ratings_raw GENERATE
    FLATTEN(REGEX_EXTRACT_ALL(
        line,
        '^([0-9]+)::([0-9]+)::([0-9]+[.]?[0-9]*)::([0-9]+)$'
    )) AS (
        user_id:chararray,
        movie_id:chararray,
        rating:chararray,
        rating_ts:chararray
    );

ratings_valid = FILTER ratings_fields BY
    user_id IS NOT NULL
    AND movie_id IS NOT NULL
    AND rating IS NOT NULL
    AND rating_ts IS NOT NULL
    AND (int)user_id > 0
    AND (int)movie_id > 0
    AND (double)rating >= 0.5
    AND (double)rating <= 5.0
    AND (long)rating_ts > 0;

ratings_clean = FOREACH ratings_valid GENERATE
    (int)user_id AS user_id,
    (int)movie_id AS movie_id,
    (double)rating AS rating,
    (long)rating_ts AS rating_ts;

movies_raw = LOAD '/movielens/raw/movies.dat'
    USING TextLoader()
    AS (line:chararray);

movies_fields = FOREACH movies_raw GENERATE
    FLATTEN(REGEX_EXTRACT_ALL(
        line,
        '^([0-9]+)::(.*)::([^:]*)$'
    )) AS (
        movie_id:chararray,
        title:chararray,
        genres:chararray
    );

movies_valid = FILTER movies_fields BY
    movie_id IS NOT NULL
    AND title IS NOT NULL
    AND title != ''
    AND (int)movie_id > 0;

movies_clean = FOREACH movies_valid GENERATE
    (int)movie_id AS movie_id,
    title AS title,
    genres AS genres;

STORE ratings_clean INTO '/movielens/clean/ratings'
    USING PigStorage('\t');

STORE movies_clean INTO '/movielens/clean/movies'
    USING PigStorage('\t');
