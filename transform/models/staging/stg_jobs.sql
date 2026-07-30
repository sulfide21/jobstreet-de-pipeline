with deduped as (
    select
    id            as job_id,
    title,
    company,
    salary        as salary_raw,
    location,
    work_type,
    teaser,
    bullet_points,
    listing_date,
    scrape_date,
    row_number() over (partition by id, scrape_date order by listing_date desc) as rn
from {{ source('raw', 'jobs') }}
)

select 
    job_id,
    title,
    company,
    salary_raw,
    case when salary_raw like '%per month%' then 
    cast(regexp_replace(regexp_extract_all(salary_raw, '[0-9.,]+')[1], '[.,]', '', 'g') as bigint) end as salary_min,
    case when salary_raw like '%per month%' then 
    cast(regexp_replace(regexp_extract_all(salary_raw, '[0-9.,]+')[2], '[.,]', '', 'g') as bigint) end as salary_max,
    (salary_min is null or  (
        salary_min >= 1000000
        and salary_max <= 1000000000
        and salary_max >= salary_min
    )) as is_salary_sane,         
    location,
    work_type,
    teaser,
    bullet_points,
    listing_date,
    scrape_date
from deduped 
where rn = 1



