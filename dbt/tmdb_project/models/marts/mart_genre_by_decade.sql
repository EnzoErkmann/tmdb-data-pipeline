with movies as (
        select * from {{ ref('stg_movies') }}
    ),

    genres as (
        select * from {{ ref('stg_genres') }}
    ),

    movie_genres as (
        select * from {{ ref('stg_movie_genres') }}
    ),

    joined as (
        select
            m.movie_id,
            m.title,
            cast(floor(extract(year from m.release_date) / 10) * 10 as int64) as decade,
            m.popularity,
            m.vote_average,
            g.genre_name
        from movies m
        join movie_genres mg on m.movie_id = mg.movie_id
        join genres g on mg.genre_id = g.genre_id
        where m.release_date is not null
    )

select
    decade,
    genre_name as top_genre,
    count(distinct movie_id) as total_movies,
    avg(popularity) as avg_popularity,
    avg(vote_average) as avg_rating
from joined
group by decade, genre_name
qualify row_number() over (partition by decade order by count(distinct movie_id) desc) = 1
order by decade desc
