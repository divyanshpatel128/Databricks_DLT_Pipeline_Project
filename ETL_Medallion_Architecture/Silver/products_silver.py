import dlt

from pyspark.sql.functions import *

# streaming view

@dlt.view(
    name="products_silver_view"
)
def sales_silver_view():
    df_products=spark.readStream.table("products_bronze")
    df_products=df_products.withColumn('processed_date',current_timestamp())
    df_products=df_products.withColumn('product_id',col('product_id').cast('int'))\
                            .withColumn('price',col('price').cast('double'))

    return df_products

# Create target streaming table
dlt.create_streaming_table(
    name="products_silver"
)

# Apply CDC (Upsert)
dlt.create_auto_cdc_flow(
    source="products_silver_view",
    target="products_silver",
    keys=["product_id"],
    sequence_by=col("processed_date"),
    stored_as_scd_type=1
)

