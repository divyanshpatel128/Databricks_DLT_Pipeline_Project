import dlt

from pyspark.sql.functions import *

# streaming view

@dlt.view(
    name="sales_silver_view"
)
def sales_silver_view():
    df_sales=spark.readStream.table("sales_bronze")
    df_sales=df_sales.withColumn('price_per_unit',round(col("total_amount")/col("quantity"),2))
    df_sales=df_sales.withColumn('processed_date',current_timestamp())
    df_sales=df_sales.withColumn('sales_id',col('sales_id').cast('int'))\
                    .withColumn('customer_id',col('customer_id').cast('int'))\
                    .withColumn('product_id',col('product_id').cast('int'))\
                    .withColumn('store_id',col('store_id').cast('int'))\
                    .withColumn('quantity',col('quantity').cast('int'))\
                    .withColumn('total_amount',col('total_amount').cast('double'))\
                    .withColumn('discount',col('discount').cast('double'))\
                    .withColumn('price_per_unit',col('price_per_unit').cast('double'))

    return df_sales

# Create target streaming table
dlt.create_streaming_table(
    name="sales_silver"
)

# Apply CDC (Upsert)
dlt.create_auto_cdc_flow(
    source="sales_silver_view",
    target="sales_silver",
    keys=["sales_id"],
    sequence_by=col("processed_date"),
    stored_as_scd_type=1
)

