# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Backout Script - MED834ENRL_OLAP Schema

# COMMAND ----------

# DBTITLE 1,Drop FACT_ENROLLMENT
# Drop fact table first (has foreign key dependencies on dimension tables)
spark.sql('DROP TABLE IF EXISTS FACT_ENROLLMENT')
print('Successfully Dropped FACT_ENROLLMENT')

# COMMAND ----------

# DBTITLE 1,Drop DIM_TRANSACTION_SOURCE
spark.sql('DROP TABLE IF EXISTS DIM_TRANSACTION_SOURCE')
print('Successfully Dropped DIM_TRANSACTION_SOURCE')

# COMMAND ----------

# DBTITLE 1,Drop DIM_DATE
spark.sql('DROP TABLE IF EXISTS DIM_DATE')
print('Successfully Dropped DIM_DATE')

# COMMAND ----------

# DBTITLE 1,Drop DIM_PLAN
spark.sql('DROP TABLE IF EXISTS DIM_PLAN')
print('Successfully Dropped DIM_PLAN')

# COMMAND ----------

# DBTITLE 1,Drop DIM_GROUP
spark.sql('DROP TABLE IF EXISTS DIM_GROUP')
print('Successfully Dropped DIM_GROUP')

# COMMAND ----------

# DBTITLE 1,Drop DIM_MEMBER
spark.sql('DROP TABLE IF EXISTS DIM_MEMBER')
print('Successfully Dropped DIM_MEMBER')

# COMMAND ----------

# DBTITLE 1,Verify All Tables Dropped
# Verify all tables are dropped
tables_to_check = [
    'fact_enrollment',
    'dim_transaction_source', 
    'dim_date',
    'dim_plan',
    'dim_group',
    'dim_member'
]

print('\nVerifying table drops...')
for table in tables_to_check:
    try:
        spark.table(table)
        print(f'⚠️  {table} still exists')
    except Exception as e:
        print(f'✓ {table} successfully dropped (or never existed)')

print('\n✅ Backout completed')

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Check Current Schema and Tables
# Check current database/schema context
print(f"Current database: {spark.sql('SELECT current_database()').collect()[0][0]}")
print(f"Current catalog: {spark.sql('SELECT current_catalog()').collect()[0][0]}")

# Show all tables
print("\nAll tables:")
display(spark.sql("SHOW TABLES"))

# COMMAND ----------

# DBTITLE 1,Find Where Tables Actually Are
# Try to describe one of the tables to see its full path
try:
    result = spark.sql("DESCRIBE EXTENDED dim_member")
    location_row = result.filter(result.col_name == "Location").collect()
    catalog_row = result.filter(result.col_name == "Catalog").collect()
    
    if location_row:
        print(f"Table Location: {location_row[0].data_type}")
    if catalog_row:
        print(f"Table Catalog: {catalog_row[0].data_type}")
    
    # Show the full table details
    print("\nFull table info:")
    display(spark.sql("DESCRIBE EXTENDED dim_member").filter("col_name IN ('Catalog', 'Database', 'Table', 'Location')"))
except Exception as e:
    print(f"Error: {e}")