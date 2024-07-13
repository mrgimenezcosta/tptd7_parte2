-- models/libros_fisicos_disponibles.sql
{{ config(materialized='ephemeral') }}

select
    lf.isbn,
    lf.cant_copias - coalesce(lp.cantidad_prestamos, 0) as copias_disponibles
from
    {{ source('biblioteca', 'libros_fisicos') }} as lf
left join
    {{ ref('libros_prestados') }} as lp
on
    lf.isbn = lp.isbn
