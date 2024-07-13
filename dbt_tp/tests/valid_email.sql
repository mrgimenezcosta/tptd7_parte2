select *
from {{ ref('model_name') }}
where {{ column_name }} !~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+(\.[A-Za-z]{2,})*$'