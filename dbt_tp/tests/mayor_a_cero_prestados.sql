select *
from {{ ref('reservas_insatisfechas') }}
where cant_reservas_insatisfechas <= 0