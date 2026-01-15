import dlt

from pyspark.sql.functions import *

# gold streaming view on top of silver view

@dlt.view(
    name='sales_gold_view'
)
def sales_gold_view():
  return spark.readStream.table('sales_silver_view')

dlt.create_streaming_table(
  name='fact_sales'
)
# Apply CDC (Upsert)
dlt.create_auto_cdc_flow(
    source="sales_gold_view",
    target="fact_sales",
    keys=["sales_id"],
    sequence_by=col("processed_date"),
    stored_as_scd_type=1,
    except_column_list=["processed_date"])