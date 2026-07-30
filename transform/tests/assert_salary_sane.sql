{{ config(error_if = '>10', warn_if = '>0') }}

select * from {{ ref('stg_jobs') }}
where salary_max < salary_min 
or salary_min < 1000000 
or salary_max > 1000000000