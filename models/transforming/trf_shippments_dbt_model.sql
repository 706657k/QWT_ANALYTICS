{{config(materialized='table',schema='transforming_dev')}}

select 
ss.orderid,
ss.LineNo,
lkp.companyname,
ss.shipmentdate,
ss.status
from {{ref("shipments_snapshot")}} as ss inner join {{ref('lkp_shippers')}} as lkp
on ss.ShipperID = lkp.ShipperID
where ss.dbt_valid_to is NULL