{{config(materialized='table',schema='transforming_dev')}}
select 
prd.productid,
prd.productname,
cat.categoryname,
sup.CompanyName as  suppliercompany,
sup.contactname as suppliercontact,
sup.city as suppliercity,
sup.Country as suppliercountry,
prd.quantityperunit,
prd.unitcost,
prd.unitprice,
prd.unitsinstock,
prd.unitsonorder,
to_decimal(prd.unitprice-prd.unitcost,9,2) as profit,
IFF(prd.unitsonorder > prd.unitsinstock,'Not available','Available') as productavailabilty
from 
{{ref("stg_products")}} as prd left join {{ref('trf_suppliers')}} as sup
on prd.SupplierID = sup.SupplierID
left join {{ref("lkp_categories")}} as cat on cat.categoryid = prd.categoryid 