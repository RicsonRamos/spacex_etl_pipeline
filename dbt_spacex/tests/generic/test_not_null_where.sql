{% test not_null_where(model, column, where_condition) %}

SELECT *
FROM {{ model }}
WHERE {{ column }} IS NULL
  AND ({{ where_condition }})

{% endtest %}