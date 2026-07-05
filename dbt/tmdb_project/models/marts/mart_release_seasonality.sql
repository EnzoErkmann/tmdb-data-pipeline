with movies as (
    select * from {{ ref('stg_movies') }}
),

seasonality as (
    select
        extract(month from release_date) as release_month,
        case extract(month from release_date)
            when 1 then 'January'
            when 2 then 'February'
            when 3 then 'March'
            when 4 then 'April'
            when 5 then 'May'
            when 6 then 'June'
            when 7 then 'July'
            when 8 then 'August'
            when 9 then 'September'
            when 10 then 'October'
            when 11 then 'November'
            when 12 then 'December'
        end as month_name_en,
        count(movie_id) as total_movies,
        avg(revenue) as avg_revenue,
        avg(popularity) as avg_popularity,
        avg(vote_average) as avg_rating
    from movies
    where release_date is not null
    group by release_month, month_name_en
)

select * from seasonality
order by release_month
