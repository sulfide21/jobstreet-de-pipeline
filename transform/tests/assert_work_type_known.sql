select *
from {{ ref('stg_jobs') }}
where work_type not in ('Full time', 'Kontrak', 'Kasual', 
'Paruh waktu', 'Full time, Kontrak', 'Full time, Paruh waktu')