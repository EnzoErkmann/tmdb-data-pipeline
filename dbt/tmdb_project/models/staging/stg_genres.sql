with source as (
    select * from {{ source('bronze', 'raw_genres') }}
    ),

    renamed as (
    select
        cast(id as int64) as genre_id,
        case name
            when 'Ação' then 'Action'
            when 'Aventura' then 'Adventure'
            when 'Animação' then 'Animation'
            when 'Comédia' then 'Comedy'
            when 'Crime' then 'Crime'
            when 'Documentário' then 'Documentary'
            when 'Drama' then 'Drama'
            when 'Família' then 'Family'
            when 'Fantasia' then 'Fantasy'
            when 'História' then 'History'
            when 'Terror' then 'Horror'
            when 'Música' then 'Music'
            when 'Mistério' then 'Mystery'
            when 'Romance' then 'Romance'
            when 'Ficção científica' then 'Science Fiction'
            when 'Cinema TV' then 'TV Movie'
            when 'Thriller' then 'Thriller'
            when 'Guerra' then 'War'
            when 'Faroeste' then 'Western'
            else name
        end as genre_name
    from source
    )

select * from renamed
