import dlt

from pyspark.sql.functions import *

# gold streaming view on top of silver view

@dlt.view(
    name='products_gold_view'
)
def poducts_gold_view():
  return spark.readStream.table('products_silver_view')

dlt.create_streaming_table(
  name='dim_products'
)
# Apply CDC (Upsert)
dlt.create_auto_cdc_flow(
    source="products_gold_view",
    target="dim_products",
    keys=["product_id"],
    sequence_by=col("processed_date"),
    stored_as_scd_type=2,
    except_column_list=["processed_date"])