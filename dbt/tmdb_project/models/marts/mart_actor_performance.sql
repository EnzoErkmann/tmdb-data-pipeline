with movies as (
    select * from {{ ref('stg_movies') }}
),

cast_actors as (
    select * from {{ ref('stg_credits_cast') }}
),

joined as (
    select
        c.actor_name,
        c.gender,
        count(m.movie_id) as total_movies,
        count(case when m.vote_average >= 7.5 then m.movie_id end) as highly_rated_movies,
        count(case when m.popularity >= 50.0 and m.release_date >= date_sub(current_date(), interval 5 year) then m.movie_id end) as popular_recent_movies
    from movies m
    join cast_actors c on m.movie_id = c.movie_id
    where m.vote_count > 100
    group by c.actor_name, c.gender
    having count(m.movie_id) >= 3
)

select * from joined
order by popular_recent_movies desc, highly_rated_movies desc
