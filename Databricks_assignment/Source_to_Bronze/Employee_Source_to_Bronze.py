# Databricks notebook source
# DBTITLE 1,running util
# MAGIC %run /Users/dhinakaran.t@diggibyte.com/Databricks_assignment/Source_to_Bronze/Util
# MAGIC

# COMMAND ----------
dbutils.widgets.text("data_type","","data_type")
dbutils.widgets.text("table_name","","table_name")
dbutils.widgets.text("database_name","","database_name")
data_type = dbutils.widgets.get("data_type")
data_type = dbutils.widgets.get("table_name")
data_type = dbutils.widgets.get("database_name")

# COMMAND -----------
%run /Users/dhinakaran.t@diggibyte.com/Databricks_assignment/Source_to_Bronze/Util

# DBTITLE 1,Reading All CSV Files
source_path = f"dbfs:/FileStore/assignments/assignment1/resources/{data_type}.csv"
bronze_path = f"dbfs:/FileStore/assignments/assignment1/source_to_bronze1/{data_type1}.csv"
reading_csv_files=read_csv_data(source_path)
reading_csv_files.display()



# COMMAND ----------

# DBTITLE 1,Writing all CSV Files
write_csv_file(reading_csv_files,bronze_path)

def overwrite_silver_path(database_name, table_name, df, silver_path):
    
    df.mode("overwrite").option("path",silver_path).saveAsTable(f'{database_name}.{table_name}')


# COMMAND ----------

write_delta_table(employee_df, "Employee_info", "dim_employee", "EmployeeID", "/silver/Employee_info/dim_employee")


# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/assignments/assignment1/source_to_bronze'))

# COMMAND ----------

