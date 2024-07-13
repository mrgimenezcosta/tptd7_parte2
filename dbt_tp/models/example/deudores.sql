{{ config(materialized='table') }}

with prestamos_vencidos as (
    select
        p.dni,
        p.id_ejemplar,
        l.isbn,
        l.titulo,
        u.email,
        case
            when p.fecha_devolucion is null and p.fecha_vencimiento < current_date then current_date - p.fecha_vencimiento 
            else null
        end as dias_deuda
    from prestamos p
    join ejemplares e on p.id_ejemplar = e.id_ejemplar
    join libros_fisicos lf on e.isbn = lf.isbn
    join libros l on lf.isbn = l.isbn -- el titulo lo tiene la tabla 'libros'
    join usuarios u on p.dni = u.dni
    where p.fecha_vencimiento < current_date and p.fecha_devolucion is null -- eq. dias_deudas is not null
)

select
    dni,
    titulo as libro_prestado,
    email as correo_usuario,
    dias_deuda
from prestamos_vencidos
where dias_deuda is not null --chequeo repetitivo de where fecha_vencimiento < current_date() en base
