-- models/reservas_insatisfechas.sql
{{ config(materialized='table') }}

with reservas_insatisfechas as (
    select
        lfd.isbn,
        ra.cantidad_reservas,
        (ra.cantidad_reservas - lfd.copias_disponibles) as cant_reservas_insatisfechas
    from
        {{ ref('copias_disponibles') }} as lfd
    join
        {{ ref('reservas_activas') }} as ra
    on
        lfd.isbn = ra.isbn
    where
        ra.cantidad_reservas > lfd.copias_disponibles --si fuera < se podria satisfacer seguro
)

select
    isbn,
    cant_reservas_insatisfechas
from
    reservas_insatisfechas
where
    cant_reservas_insatisfechas > 0
