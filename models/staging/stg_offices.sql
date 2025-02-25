{{config(materialized='table',schema='staging_dev')}}

select * from {{source('qwt_raw','raw_offices')}}