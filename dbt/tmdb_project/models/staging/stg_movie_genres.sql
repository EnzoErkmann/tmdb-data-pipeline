with source as (
    select * from {{ source('bronze', 'raw_movies') }}
    ),

    unnested as (
    select
        cast(s.id as int64) as movie_id,
        cast(genre.id as int64) as genre_id
    from source s,
    unnest(genres) as genre
    )

select * from unnested
