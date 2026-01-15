import dlt

from pyspark.sql.functions import *

# gold streaming view on top of silver view

@dlt.view(
    name='stores_gold_view'
)
def stores_gold_view():
  return spark.readStream.table('stores_silver_view')

dlt.create_streaming_table(
  name='dim_stores'
)
# Apply CDC (Upsert)
dlt.create_auto_cdc_flow(
    source="stores_gold_view",
    target="dim_stores",
    keys=["store_id"],
    sequence_by=col("processed_date"),
    stored_as_scd_type=2,
    except_column_list=["processed_date"])