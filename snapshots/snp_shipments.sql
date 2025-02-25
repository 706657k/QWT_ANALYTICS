{% snapshot shipments_snapshot%}

{{

config(
    target_database = 'QWT_ANALYTICS_DEV',
    target_schema = 'SNAPSHOT_DEV',

    unique_key="orderid||'-'||LineNo",
    strategy='timestamp',
    updated_at='shipmentdate'
)

}}

select * from {{ref("stg_shipments")}}

{% endsnapshot %}