with movies as (
    select * from {{ ref('stg_movies') }}
)

select
    floor(extract(year from release_date) / 10) * 10 as decade,
    count(movie_id) as total_movies,
    avg(budget) as avg_budget,
    avg(revenue) as avg_revenue
from movies
where release_date is not null and budget > 0
group by decade
order by decade desc
