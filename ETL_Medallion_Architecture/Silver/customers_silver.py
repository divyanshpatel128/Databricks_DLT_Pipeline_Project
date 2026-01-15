import dlt

from pyspark.sql.functions import *

# streaming view

@dlt.view(
    name="customers_silver_view"
)
def customers_silver_view():
    df_customers=spark.readStream.table("customers_bronze")
    df_customers=df_customers.withColumn('name',upper(col("name")))
    df_customers=df_customers.withColumn('domain',split(col("email"),'@')[1])
    df_customers=df_customers.withColumn('processed_date',current_timestamp())
    df_customers=df_customers.withColumn('customer_id',col('customer_id').cast('int'))\
                            .withColumn('signup_date',col('signup_date').cast('date'))

    return df_customers

# Create target streaming table
dlt.create_streaming_table(
    name="customers_silver"
)

# Apply CDC (Upsert)
dlt.create_auto_cdc_flow(
    source="customers_silver_view",
    target="customers_silver",
    keys=["customer_id"],
    sequence_by=col("processed_date"),
    stored_as_scd_type=1
)

