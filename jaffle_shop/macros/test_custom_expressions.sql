{% test check_greater_than_or_equal(model, column_name, value=0) %}
    select *
    from {{ model }}
    where {{ column_name }} < {{ value }}
{% endtest %}

{% test check_between(model, column_name, min_val=0, max_val=100) %}
    select *
    from {{ model }}
    where {{ column_name }} < {{ min_val }} or {{ column_name }} > {{ max_val }}
{% endtest %}