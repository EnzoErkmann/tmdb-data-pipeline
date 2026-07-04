with financials as (
    select * from {{ ref('mart_movie_financials') }}
),

movie_genres as (
    select * from {{ ref('stg_movie_genres') }}
),

genres as (
    select * from {{ ref('stg_genres') }}
),

joined as (
    select
        g.genre_name,
        count(f.movie_id) as total_movies,
        avg(f.budget) as avg_budget,
        avg(f.revenue) as avg_revenue,
        avg(f.profit) as avg_profit,
        avg(f.roi) as avg_roi
    from financials f
    join movie_genres mg on f.movie_id = mg.movie_id
    join genres g on mg.genre_id = g.genre_id
    group by g.genre_name
)

select * from joined
order by avg_roi desc
