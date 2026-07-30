select * from {{ ref('stg_jobs') }}
where not is_salary_sane
