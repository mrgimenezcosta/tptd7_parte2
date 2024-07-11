{% test mayor_a_cero(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} <= 0
{% endtest %}

{% test mayor_o_igual_cero(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} < 0
{% endtest %}

{% test mayor_o_igual_uno(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} < 1
{% endtest %}

{% test valid_isbn_13(model, column_name) %}
    select *
    from {{ model }}
    where length({{ column_name }}) != 13
    or {{ column_name }} not like '978%'
{% endtest %}

{% test valid_dni(model, column_name) %}
    select *
    from {{ model }}
    where length({{ column_name }}) not in (7, 8)
    or cast({{ column_name }} as integer) < 3000000
    or cast({{ column_name }} as integer) > 99999999
{% endtest %}

{% test valid_email(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} !~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+(\.[A-Za-z]{2,})*$
{% endtest %}
