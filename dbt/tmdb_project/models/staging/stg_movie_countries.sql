with source as (
    select * from {{ source('bronze', 'raw_movies') }}
),

unnested_countries as (
    select
        cast(id as int64) as movie_id,
        pc.iso_3166_1 as country_iso,
        pc.name as country_name
    from source,
    unnest(production_countries) as pc
)

select * from unnested_countries
