with movies as (
    select * from {{ ref('stg_movies') }}
),

countries as (
    select * from {{ ref('stg_movie_countries') }}
),

joined as (
    select
        extract(year from m.release_date) as release_year,
        c.country_name,
        count(m.movie_id) as total_movies,
        sum(case when m.original_language not in ('en', 'pt') then 1 else 0 end) as non_us_pt_movies
    from movies m
    join countries c on m.movie_id = c.movie_id
    where m.release_date is not null
    group by release_year, c.country_name
)

select * from joined
order by release_year desc, total_movies desc
