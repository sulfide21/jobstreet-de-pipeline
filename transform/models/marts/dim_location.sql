select
    md5(location) as location_id,
    location as location_label
from (
    select distinct location
    from {{ ref('stg_jobs') }}
    where location is not null and location <> ''
)
