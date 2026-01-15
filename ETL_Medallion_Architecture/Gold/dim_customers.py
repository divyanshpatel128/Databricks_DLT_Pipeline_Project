import dlt

from pyspark.sql.functions import *

# gold streaming view on top of silver view

@dlt.view(
    name='customers_gold_view'
)
def customers_gold_view():
  return spark.readStream.table('customers_silver_view')

dlt.create_streaming_table(
  name='dim_customers'
)


# Apply CDC (Upsert)
dlt.create_auto_cdc_flow(
    source="customers_gold_view",
    target="dim_customers",
    keys=["customer_id"],
    sequence_by=col("processed_date"),
    stored_as_scd_type=2,
    except_column_list=["processed_date"])
