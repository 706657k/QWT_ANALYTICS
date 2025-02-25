import snowflake.snowpark.functions as F 
import pandas as pd
import holidays

def isholiday(orderdate):
    french_holidays=holidays.france()
    is_holiday= (orderdate in french_holidays)
    return is_holiday

def avgorder(order,value):
    value/order
    
def model(dbt,session):
    dbt.config(
        materialized="table",
        schema='report_dev',
        packages=['holidays']
    )

    orders_df =dbt.ref("fct_orders")
    customers_df =dbt.ref("rpt_customers")  

    orders_aggregate_df = ( orders_df.group_by('customerid')
                          .agg
                          (
                            F.min(F.col('orderdate')).alias('first_order_date'),
                            F.max(F.col('orderdate')).alias('recent_order_date'),
                            F.count(F.col('orderid')).alias('number_of_orders'),
                            F.countDistinct(F.col('productid')).alias('total_products'),
                            F.sum(F.col('quantity')).alias("total_quantity"),
                            F.sum(F.col('LineSalesAmount')).alias("total_sales"),
                            F.avg(F.col('margin')).alias("avg_margin")
                          )

    )
    final_orders_aggregate_df=(customers_df
                                .join(orders_aggregate_df,customers_df.customerid==orders_aggregate_df.customerid,'inner')
                                .select
                                (
                                    customers_df.companyname,
                                    customers_df.contactname,
                                    orders_aggregate_df.first_order_date,
                                    orders_aggregate_df.recent_order_date,
                                    orders_aggregate_df.number_of_orders,
                                    orders_aggregate_df.total_products,
                                    orders_aggregate_df.total_quantity,
                                    orders_aggregate_df.total_sales,
                                    orders_aggregate_df.avg_margin
                                )
    )

    final_orders_aggregate_df =final_orders_aggregate_df.withColumn("avgordervalue",avgorder(final_orders_aggregate_df.["number_of_orders"],final_orders_aggregate_df.["total_sales"]))

    final_orders_aggregate_df =final_orders_aggregate_df.filter(F.col)
    return final_orders_aggregate_df  