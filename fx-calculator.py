# Databricks notebook source
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *



# COMMAND ----------

fx_rates = sqlContext.sql("SELECT * FROM fx_rates")
large_data = sqlContext.sql("SELECT * FROM large_data")

# COMMAND ----------

large_data = large_data.withColumn('jan_01', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('jan_02', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('jan_03', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('jan_04', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('jan_05', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('jan_06', rand()*rand()*rand()*1000000)

large_data = large_data.withColumn('feb_01', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('feb_02', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('feb_03', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('feb_04', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('feb_05', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('feb_06', rand()*rand()*rand()*1000000)

large_data = large_data.withColumn('mar_01', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('mar_02', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('mar_03', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('mar_04', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('mar_05', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('mar_06', rand()*rand()*rand()*1000000)

large_data = large_data.withColumn('apr_01', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('apr_02', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('apr_03', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('apr_04', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('apr_05', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('apr_06', rand()*rand()*rand()*1000000)

large_data = large_data.withColumn('may_01', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('may_02', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('may_03', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('may_04', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('may_05', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('may_06', rand()*rand()*rand()*1000000)

large_data = large_data.withColumn('jun_01', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('jun_02', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('jun_03', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('jun_04', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('jun_05', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('jun_06', rand()*rand()*rand()*1000000)

large_data = large_data.withColumn('jul_01', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('jul_02', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('jul_03', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('jul_04', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('jul_05', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('jul_06', rand()*rand()*rand()*1000000)

large_data = large_data.withColumn('aug_01', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('aug_02', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('aug_03', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('aug_04', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('aug_05', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('aug_06', rand()*rand()*rand()*1000000)

large_data = large_data.withColumn('sep_01', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('sep_02', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('sep_03', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('sep_04', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('sep_05', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('sep_06', rand()*rand()*rand()*1000000)

large_data = large_data.withColumn('oct_01', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('oct_02', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('oct_03', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('oct_04', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('oct_05', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('oct_06', rand()*rand()*rand()*1000000)

large_data = large_data.withColumn('nov_01', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('nov_02', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('nov_03', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('nov_04', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('nov_05', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('nov_06', rand()*rand()*rand()*1000000)

large_data = large_data.withColumn('dec_01', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('dec_02', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('dec_03', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('dec_04', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('dec_05', rand()*rand()*rand()*1000000)
large_data = large_data.withColumn('dec_06', rand()*rand()*rand()*1000000)

# COMMAND ----------

join_table = large_data.join(fx_rates,large_data['CCY'] == fx_rates['CCY'],how = 'left')

# COMMAND ----------

join.select(join.colRegex("`(feb_)+?.+`")).show()

# COMMAND ----------

from pyspark.sql import functions as F

jan =['jan_01','jan_02','jan_03','jan_04','jan_05','jan_06']
feb =['feb_01','feb_02','feb_03','feb_04','feb_05','feb_06']
mar =['mar_01','mar_02','mar_03','mar_04','mar_05','mar_06']
apr =['apr_01','apr_02','apr_03','apr_04','apr_05','apr_06']
may =['may_01','may_02','may_03','may_04','may_05','may_06']
jun =['jun_01','jun_02','jun_03','jun_04','jun_05','jun_06']
jul =['jul_01','jul_02','jul_03','jul_04','jul_05','jul_06']
aug =['aug_01','aug_02','aug_03','aug_04','aug_05','aug_06']
sep =['sep_01','sep_02','sep_03','sep_04','sep_05','sep_06']
oct =['oct_01','oct_02','oct_03','oct_04','oct_05','oct_06']
nov =['nov_01','nov_02','nov_03','nov_04','nov_05','nov_06']
dec =['dec_01','dec_02','dec_03','dec_04','dec_05','dec_06']


join_table = join_table.withColumn("jan", F.struct(*[(F.col(x)*F.col('Jan')) for x in jan]))
join_table = join_table.withColumn("feb", F.struct(*[(F.col(x)*F.col('Feb')) for x in feb]))
join_table = join_table.withColumn("mar", F.struct(*[(F.col(x)*F.col('Mar')) for x in mar]))
join_table = join_table.withColumn("apr", F.struct(*[(F.col(x)*F.col('Apr')) for x in apr]))
join_table = join_table.withColumn("may", F.struct(*[(F.col(x)*F.col('May')) for x in may]))
join_table = join_table.withColumn("jun", F.struct(*[(F.col(x)*F.col('Jun')) for x in jun]))
join_table = join_table.withColumn("jul", F.struct(*[(F.col(x)*F.col('Jul')) for x in jul]))
join_table = join_table.withColumn("aug", F.struct(*[(F.col(x)*F.col('Aug')) for x in aug]))
join_table = join_table.withColumn("sep", F.struct(*[(F.col(x)*F.col('Sep')) for x in sep]))
join_table = join_table.withColumn("oct", F.struct(*[(F.col(x)*F.col('Oct')) for x in oct]))
join_table = join_table.withColumn("nov", F.struct(*[(F.col(x)*F.col('Nov')) for x in nov]))
join_table = join_table.withColumn("dec", F.struct(*[(F.col(x)*F.col('Dec')) for x in dec]))


from pyspark.sql.functions import *

# COMMAND ----------

join_table.select(join_table.colRegex("`(jan_)+?.+`")).show(5)

# COMMAND ----------

#join_table.select(join_table.colRegex("`(jan_)+?.+`")).show(5)
#join_table.select("feb.*").show(5)


# COMMAND ----------

join_table.select("Jan").show(5)
