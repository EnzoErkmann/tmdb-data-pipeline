with financials as (
    select * from {{ ref('mart_movie_financials') }}
),

movies as (
    select * from {{ ref('stg_movies') }}
),

directors as (
    select * from {{ ref('stg_credits_crew') }}
),

joined as (
    select
        d.crew_name as director_name,
        count(f.movie_id) as total_movies,
        avg(m.vote_average) as avg_rating,
        sum(f.revenue) as total_box_office,
        avg(f.roi) as avg_roi
    from directors d
    join financials f on d.movie_id = f.movie_id
    join movies m on d.movie_id = m.movie_id
    group by d.crew_name
    having count(f.movie_id) >= 3
)

select * from joined
order by avg_roi desc
