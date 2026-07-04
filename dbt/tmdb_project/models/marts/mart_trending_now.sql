with trending as (
    select * from {{ ref('stg_trending') }}
),

popular as (
    select * from {{ ref('stg_popular') }}
),

genres as (
    select * from {{ ref('stg_genres') }}
),

movie_genres as (
    select * from {{ ref('stg_movie_genres') }}
),

trending_with_genres as (
    select
        t.movie_id,
        t.title,
        t.popularity,
        g.genre_name,
        'Trending' as list_type
    from trending t
    join movie_genres mg on t.movie_id = mg.movie_id
    join genres g on mg.genre_id = g.genre_id
),

popular_with_genres as (
    select
        p.movie_id,
        p.title,
        p.popularity,
        g.genre_name,
        'Popular' as list_type
    from popular p
    join movie_genres mg on p.movie_id = mg.movie_id
    join genres g on mg.genre_id = g.genre_id
),

combined as (
    select * from trending_with_genres
    union all
    select * from popular_with_genres
)

select * from combined
order by popularity desc
