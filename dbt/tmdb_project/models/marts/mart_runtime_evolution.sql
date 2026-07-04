with movies as (
    select * from {{ ref('stg_movies') }}
),

evolution as (
    select
        floor(extract(year from release_date) / 10) * 10 as decade,
        avg(runtime) as avg_runtime_minutes,
        count(movie_id) as total_movies
    from movies
    where release_date is not null and runtime > 0
    group by decade
)

select * from evolution
order by decade desc
