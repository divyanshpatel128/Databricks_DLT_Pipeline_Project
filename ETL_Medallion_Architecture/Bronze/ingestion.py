import dlt

# ingesting sales data from volume

@dlt.table(
    name="sales_bronze"
)
def sales_bronze():
    df= spark.readStream.format("cloudFiles")\
      .option("cloudFiles.format","csv")\
          .load("/Volumes/databricks_bootcamp/bronze/bronze_volume/sales/")
    return df

# ingesting stores data from volume

@dlt.table(
    name="stores_bronze"
)
def sales_bronze():
    df= spark.readStream.format("cloudFiles")\
      .option("cloudFiles.format","csv")\
          .load("/Volumes/databricks_bootcamp/bronze/bronze_volume/stores/")
    return df

# ingesting products data from volume

@dlt.table(
    name="products_bronze"
)
def sales_bronze():
    df= spark.readStream.format("cloudFiles")\
      .option("cloudFiles.format","csv")\
          .load("/Volumes/databricks_bootcamp/bronze/bronze_volume/products/")
    return df

# ingesting customers data from volume

@dlt.table(
    name="customers_bronze"
)
def sales_bronze():
    df= spark.readStream.format("cloudFiles")\
      .option("cloudFiles.format","csv")\
          .load("/Volumes/databricks_bootcamp/bronze/bronze_volume/customers/")
    return df




























