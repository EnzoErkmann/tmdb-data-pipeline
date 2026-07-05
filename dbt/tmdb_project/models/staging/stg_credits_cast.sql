with source as (
    select * from {{ source('bronze', 'raw_credits') }}
),

unnested_cast as (
    select
        cast(s.id as int64) as movie_id,
        cast(c.id as int64) as actor_id,
        c.name as actor_name,
        case 
            when cast(c.gender as int64) = 1 then 'Female'
            when cast(c.gender as int64) = 2 then 'Male'
            when cast(c.gender as int64) = 3 then 'Non-Binary'
            else 'Unknown'
        end as gender,
        c.character
    from source s,
    unnest(`cast`) as c
)

select * from unnested_cast
