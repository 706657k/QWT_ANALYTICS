{{config(materialized='view',schema='report_dev')}}

select * from {{ref("trf_customers")}}