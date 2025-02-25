{{config(materialized='table')}}

select 
OrderID,
LineNo,
ShipperID,
CustomerID,
ProductID,
EmployeeID,
split_part(shipmentdate,' ',1)::date as shipmentdate,
status
from 
{{source("qwt_raw",'raw_shipments')}}