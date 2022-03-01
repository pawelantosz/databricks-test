# Databricks notebook source
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *

#storage-databricks-aws-test S3 Bucket DB name

# COMMAND ----------

tm1_cost = sqlContext.sql("SELECT * FROM tm1_cost")
tm1_drivers = sqlContext.sql("SELECT * FROM tm1_drivers_1_csv")
manual_instruction = sqlContext.sql("SELECT * FROM manual_instruction_1_csv")

# COMMAND ----------

#TM1 Cost Data Transformation
tm1_cost = tm1_cost.withColumn('asc_cost_cetner', regexp_replace('asc_cost_cetner', ' - Cost Center', ''))
tm1_cost = tm1_cost.withColumn('period', regexp_replace('period', '22', ''))
tm1_cost = tm1_cost.withColumn('period', regexp_replace('period', '-', ''))

tm1_cost = tm1_cost.drop('account')
tm1_cost = tm1_cost.drop('company')

tm1_cost = tm1_cost.withColumnRenamed("asc_cost_cetner","asc_cost_center")

tm1_cost = tm1_cost.groupBy('asc_cost_center','period').sum()
tm1_cost.show(5)


# COMMAND ----------

#Drivers Data Transformation

#tm1_drivers = tm1_drivers.groupBy('driver_ou','driver_country','driver_region','driver_lob','driver_bu','target_cost_center').sum()
#from pyspark.sql import functions as F
tm1_drivers = tm1_drivers.withColumn("driver_geography",F.concat_ws('/',tm1_drivers.driver_country,tm1_drivers.driver_region))

#tm1_drivers.show(1,truncate=False)
tm1_drivers

# COMMAND ----------

#Instruction Data Transformation
from pyspark.sql import functions as F
manual_instruction = manual_instruction.withColumn('source_geography',
                        F.when((F.col("allocation_geography") == 'Country'), F.col("source_country"))\
                        .when((F.col("allocation_geography") == 'Regional'), F.col("source_region"))\
                        .otherwise("Global")
                                                  )
#manual_instruction.filter(manual_instruction.allocation_geography == "Global").show()

# COMMAND ----------

#Creating Hive tables for SQL queries
#manual_instruction.createOrReplaceTempView("manual_instruction_hive")
# spark.sql("SELECT * FROM manual_instruction_hive where allocation_geography == 'Regional'").show()

# COMMAND ----------

#master = tm1_drivers.crossJoin(manual_instruction)
#master.show()

master2 = master.where((master.driver_geography == master.source_geography) | (master.source_geography == 'Global'))
master2.count()
