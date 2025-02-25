{{config(materialized='view',schema='report_dev')}}
{% set v_linenumber=[1,2,3] %}

select orderid,

{% for linenumber in v_linenumber %}

sum(case when lineno ={{linenumber}} then linesalesamount end) as lineno(linenumber)_sales

{% endfor %}

from 
{{ref("fct_orders")}}

group by {{orderid}}