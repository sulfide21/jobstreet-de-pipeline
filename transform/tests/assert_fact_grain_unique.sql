select job_id, scrape_date, count(*)
from {{ ref('fact_job_posting') }}
group by job_id, scrape_date
having count(*) > 1
