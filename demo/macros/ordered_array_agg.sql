{% macro ordered_array_agg(value, order_by) %}
  {{ adapter.dispatch('ordered_array_agg', 'rowlineage_demo')(value, order_by) }}
{% endmacro %}

{% macro default__ordered_array_agg(value, order_by) %}
  array_agg({{ value }} order by {{ order_by }})
{% endmacro %}

{% macro clickhouse__ordered_array_agg(value, order_by) %}
  arrayMap(x -> x.2, arraySort(x -> x.1, groupArray(({{ order_by }}, {{ value }}))))
{% endmacro %}
