{{config(materialized='incremental', unique_key=['orderid','Lineno'])}}

select
od.*,
o.orderdate from
{{source('qwt_raw','raw_orders')}} as o inner join
{{source('qwt_raw','raw_orderdetails')}} as od on o.orderid = od.orderid

{% if is_incremental() %}

where orderdate > (select max(orderdate) from {{this}})

{% endif %}