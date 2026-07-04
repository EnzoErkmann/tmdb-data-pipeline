with movies as (
    select * from {{ ref('stg_movies') }}
)

select
    extract(year from release_date) as release_year,
    count(movie_id) as total_movies,
    avg(budget) as avg_budget,
    avg(revenue) as avg_revenue
from movies
where release_date is not null and budget > 0
group by release_year
order by release_year desc
