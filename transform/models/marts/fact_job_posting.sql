{{ config(materialized='incremental') }}

select
    s.job_id,
    s.title,
    s.salary_raw,
    s.scrape_date,
    s.listing_date,
    c.company_id,
    l.location_id
from {{ ref('stg_jobs') }} s
left join {{ ref('dim_company') }} c on s.company = c.company_name
left join {{ ref('dim_location') }} l on s.location = l.location_label
{% if is_incremental() %}
where s.scrape_date > (select max(scrape_date) from {{ this }})
{% endif %}
