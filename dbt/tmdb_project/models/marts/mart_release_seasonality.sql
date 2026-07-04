with movies as (
    select * from {{ ref('stg_movies') }}
),

seasonality as (
    select
        extract(month from release_date) as release_month,
        count(movie_id) as total_movies,
        avg(revenue) as avg_revenue,
        avg(popularity) as avg_popularity,
        avg(vote_average) as avg_rating
    from movies
    where release_date is not null
    group by release_month
)

select * from seasonality
order by release_month
