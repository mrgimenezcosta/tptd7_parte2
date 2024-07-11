-- models/libros_prestados.sql
{{ config(materialized='ephemeral') }}

select
    p.ISBN,
    count(*) as cantidad_prestamos
from
    {{ source('biblioteca', 'prestamos') }} as p
where
    p.fecha_devolucion is null
group by
    p.ISBN;
