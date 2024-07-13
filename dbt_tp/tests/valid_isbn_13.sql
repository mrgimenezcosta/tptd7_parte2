select *
from {{ ref('libros_prestados') }}
where length(isbn) != 13
or isbn not like '978%'
