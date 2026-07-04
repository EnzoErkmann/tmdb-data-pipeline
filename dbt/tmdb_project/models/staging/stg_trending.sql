with source as (
    select * from {{ source('bronze', 'raw_trending') }}
),

renamed as (
    select
        cast(id as int64) as movie_id,
        title,
        cast(popularity as numeric) as popularity,
        cast(vote_average as numeric) as vote_average,
        cast(vote_count as int64) as vote_count,
        cast(release_date as date) as release_date
    from source
)

select * from renamed
