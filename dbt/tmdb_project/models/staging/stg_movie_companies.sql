with source as (
    select * from {{ source('bronze', 'raw_movies') }}
),

unnested_companies as (
    select
        cast(id as int64) as movie_id,
        cast(pc.id as int64) as company_id,
        pc.name as company_name,
        pc.origin_country
    from source,
    unnest(production_companies) as pc
)

select * from unnested_companies
