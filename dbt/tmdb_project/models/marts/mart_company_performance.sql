with financials as (
    select * from {{ ref('mart_movie_financials') }}
),

movie_companies as (
    select * from {{ ref('stg_movie_companies') }}
),

joined as (
    select
        mc.company_name,
        count(f.movie_id) as total_movies,
        sum(f.revenue) as total_revenue,
        avg(f.roi) as avg_roi
    from financials f
    join movie_companies mc on f.movie_id = mc.movie_id
    group by mc.company_name
    having count(f.movie_id) >= 5
)

select * from joined
order by avg_roi desc
