{% macro get_min_orderdate() %}

 {% set get_min_orderdate_query %}
 select min(orderdate)
 from {{ref("fct_orders")}}
 {% endset %}

    {% set results = run_query(get_min_orderdate_query) %}

    {% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0][0] %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}

    {{return(results_list)}}
{% endmacro %}

{% macro get_max_orderdate() %}

    {% set get_max_orderdate_query %}

    select max(orderdate)
    from {{ref("fct_orders")}}

    {% endset %}

    {% set results = run_query(get_max_orderdate_query) %}

    {% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0][0] %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}

    {{return(results_list)}}

{% endmacro %}