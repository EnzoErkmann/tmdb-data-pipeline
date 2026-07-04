with source as (
    select * from {{ source('bronze', 'raw_credits') }}
),

unnested_crew as (
    select
        cast(s.id as int64) as movie_id,
        cast(c.id as int64) as crew_id,
        c.name as crew_name,
        case 
            when cast(c.gender as int64) = 1 then 'Feminino'
            when cast(c.gender as int64) = 2 then 'Masculino'
            when cast(c.gender as int64) = 3 then 'Não-Binário'
            else 'Desconhecido'
        end as gender,
        c.job
    from source s,
    unnest(crew) as c
    where c.job = 'Director'
)

select * from unnested_crew
