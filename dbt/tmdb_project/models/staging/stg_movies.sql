with source as (
    select * from {{ source('bronze', 'raw_movies') }}
),

renamed as (
    select
        cast(id as int64) as movie_id,
        title,
        original_title,
        original_language,
        cast(release_date as date) as release_date,
        cast(budget as numeric) as budget,
        cast(revenue as numeric) as revenue,
        cast(runtime as int64) as runtime,
        cast(vote_average as numeric) as vote_average,
        cast(vote_count as int64) as vote_count,
        cast(popularity as numeric) as popularity,
        status
    from source
)

select * from renamed
