select *
from {{ ref('model_name') }}
where length({{ column_name }}) not in (7, 8)
or cast({{ column_name }} as integer) < 3000000
or cast({{ column_name }} as integer) > 99999999
