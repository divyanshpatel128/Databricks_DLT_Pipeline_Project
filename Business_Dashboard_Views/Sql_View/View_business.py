# Databricks notebook source
# MAGIC %md
# MAGIC ### perform data masking to prevent information of customers

# COMMAND ----------

# DBTITLE 1,Untitled
# MAGIC %sql
# MAGIC create or replace function databricks_bootcamp.gold.data_masking(p_gmail string)
# MAGIC returns string
# MAGIC return case when is_account_group_member("developer") then p_gmail else "******" end

# COMMAND ----------

# MAGIC %sql
# MAGIC Alter table databricks_bootcamp.gold.dim_customers
# MAGIC alter column email set mask  databricks_bootcamp.gold.data_masking

# COMMAND ----------

# MAGIC %md
# MAGIC ### create a sql view 

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view databricks_bootcamp.gold.business_view
# MAGIC as 
# MAGIC select 
# MAGIC       s.sales_id,s.total_amount,round(s.total_amount-s.discount,2) as net_sales ,s.quantity,s.price_per_unit,
# MAGIC       c.name,c.email,c.location,c.domain,c.signup_date,
# MAGIC       p.product_name,p.category,
# MAGIC       st.store_name,st.region
# MAGIC from 
# MAGIC       databricks_bootcamp.gold.fact_sales s
# MAGIC left join
# MAGIC       databricks_bootcamp.gold.dim_stores st on s.store_id = st.store_id and st.`__END_AT` is null
# MAGIC left  join 
# MAGIC       databricks_bootcamp.gold.dim_products p on s.product_id = p.product_id and p.`__END_AT` is null
# MAGIC left join 
# MAGIC       databricks_bootcamp.gold.dim_customers c on s.customer_id = c.customer_id and c.`__END_AT` is null;
# MAGIC
# MAGIC select * from databricks_bootcamp.gold.business_view
# MAGIC limit 5