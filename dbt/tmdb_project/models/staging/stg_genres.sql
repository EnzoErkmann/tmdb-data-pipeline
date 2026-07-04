with source as (
    select * from {{ source('bronze', 'raw_genres') }}
),

renamed as (
    select
        cast(id as int64) as genre_id,
        name as genre_name
    from source
)

select * from renamed
