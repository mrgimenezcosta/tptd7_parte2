-- models/reservas_insatisfechas.sql
{{ config(materialized='table') }}

with reservas_activas as (
    {{ ref('reservas_activas') }}
),

libros_fisicos_disponibles as (
    {{ ref('copias_disponibles') }}
),

reservas_insatisfechas as (
    select
        lfd.ISBN,
        ra.cantidad_reservas,
        (ra.cantidad_reservas - lfd.copias_disponibles) as reservas_insatisfechas
    from
        libros_fisicos_disponibles as lfd
    join
        reservas_activas as ra
    on
        lfd.ISBN = ra.ISBN
    where
        ra.cantidad_reservas > lfd.copias_disponibles --si fuera < se podria satisfacer seguro
)

select
    ISBN,
    cant_reservas_insatisfechas
from
    reservas_insatisfechas
where
    cant_reservas_insatisfechas > 0;
