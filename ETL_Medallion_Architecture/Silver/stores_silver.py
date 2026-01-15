import dlt

from pyspark.sql.functions import *

# streaming view

@dlt.view(
    name="stores_silver_view"
)
def sales_silver_view():
    df_stores=spark.readStream.table("stores_bronze")
    df_stores=df_stores.withColumn('store_name',regexp_replace('store_name','_',''))
    df_stores=df_stores.withColumn('processed_date',current_timestamp())
    df_stores=df_stores.withColumn('store_id',col('store_id').cast('int'))

    return df_stores

# Create target streaming table
dlt.create_streaming_table(
    name="stores_silver"
)

# Apply CDC (Upsert)
dlt.create_auto_cdc_flow(
    source="stores_silver_view",
    target="stores_silver",
    keys=["store_id"],
    sequence_by=col("processed_date"),
    stored_as_scd_type=1
)

