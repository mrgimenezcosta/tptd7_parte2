-- models/libros_prestados.sql
{{ config(materialized='ephemeral') }}


select
    e.isbn,
    count(*) as cantidad_prestamos
from
    {{ source('biblioteca', 'prestamos') }} as p
join
    {{ source('biblioteca', 'ejemplares') }} as e on p.id_ejemplar = e.id_ejemplar
where
    p.fecha_devolucion is null
group by
    e.isbn


