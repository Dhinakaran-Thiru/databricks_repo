# Databricks notebook source
# DBTITLE 1,running util
# MAGIC %run /Users/dhinakaran.t@diggibyte.com/Databricks_assignment/Source_to_Bronze/Util
# MAGIC

# COMMAND ----------

# DBTITLE 1,Reading All CSV Files
employee_path = "dbfs:/FileStore/assignments/assignment1/resources/Employee_Q1.csv"
department_path = "dbfs:/FileStore/assignments/assignment1/resources/Department_Q1.csv"
country_path= "dbfs:/FileStore/assignments/assignment1/resources/Country_Q1.csv"

country_df=read_csv_data(country_path)
country_df.display()
department_df=read_csv_data(department_path)
department_df.display()
employee_df=read_csv_data(employee_path)
employee_df.display()

# COMMAND ----------

# DBTITLE 1,Writing all CSV Files
write_csv_file(country_df,'dbfs:/FileStore/assignments/assignment1/source_to_bronze/country_df.csv')
write_csv_file(department_df,'dbfs:/FileStore/assignments/assignment1/source_to_bronze/department_df.csv')
write_csv_file(employee_df,'dbfs:/FileStore/assignments/assignment1/source_to_bronze/employee_df.csv')





# COMMAND ----------

# DBTITLE 1,Spark SQL Commands to create Database
spark.sql("CREATE DATABASE IF NOT EXISTS Employee_info")


# COMMAND ----------

write_delta_table(employee_df, "Employee_info", "dim_employee", "EmployeeID", "/silver/Employee_info/dim_employee")


# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/assignments/assignment1/source_to_bronze'))

# COMMAND ----------

