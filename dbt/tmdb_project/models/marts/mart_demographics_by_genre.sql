with cast_actors as (
    select * from {{ ref('stg_credits_cast') }}
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
        c.gender,
        count(c.actor_id) as total_actors
    from cast_actors c
    join movie_genres mg on c.movie_id = mg.movie_id
    join genres g on mg.genre_id = g.genre_id
    group by g.genre_name, c.gender
)

select * from joined
order by genre_name, total_actors desc
