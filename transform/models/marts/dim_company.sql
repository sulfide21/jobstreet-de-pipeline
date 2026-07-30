SELECT md5(company) as company_id,
company as company_name
from (
    select distinct company
    from {{ ref('stg_jobs') }}
    where company is not null and company <> ''
)