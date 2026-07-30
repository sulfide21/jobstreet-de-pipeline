with jobs as (
    select
        job_id,
        scrape_date,
        lower(concat_ws(' ', title, teaser, bullet_points)) as job_text
    from {{ ref('stg_jobs') }}
),

day_totals as (
    select scrape_date, count(distinct job_id) as jobs_that_day
    from jobs
    group by scrape_date
),

matches as (
    select
        j.scrape_date,
        s.phase,
        s.skill,
        count(distinct j.job_id) as jobs_with_skill
    from jobs j
    cross join {{ ref('skills') }} s
    where regexp_matches(j.job_text, s.pattern)
    group by j.scrape_date, s.phase, s.skill
)

select
    m.scrape_date,
    m.phase,
    m.skill,
    m.jobs_with_skill,
    d.jobs_that_day,
    round(100.0 * m.jobs_with_skill / d.jobs_that_day, 1) as demand_pct
from matches m
join day_totals d on m.scrape_date = d.scrape_date
order by m.scrape_date, demand_pct desc
