{{config(materialized = 'view', schema = env_var('DBT_TRANSFORMSCHEMA', 'report_dev'))}}

select * from {{ref("trf_products")}}