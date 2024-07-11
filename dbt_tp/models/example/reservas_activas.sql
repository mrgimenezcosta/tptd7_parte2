-- models/reservas_activas.sql
{{ config(materialized='ephemeral') }}

select
    r.ISBN,
    count(*) as cantidad_reservas
from
    {{ source('biblioteca', 'reservas') }} as r
group by
    r.ISBN;
