select *
from {{ ref('model_name') }}
where {{ column_name }} <= 0