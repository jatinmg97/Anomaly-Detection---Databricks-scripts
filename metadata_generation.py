# Databricks notebook source
file_location = "dbfs:/mnt/ford/Source_Test/EDW_2015_2020.csv"

#file_location="/FileStore/tables/final_csv"
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#df=df.toPandas()
#df.dtypes
#df.to_csv('main.csv')


# COMMAND ----------

file_location = "dbfs:/mnt/ford/Source_Test/2016"

#file_location="/FileStore/tables/2015.csv"
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df2 = spark.read.format(file_type) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------

file_location = "dbfs:/mnt/ford/Source_Test/2017"

#file_location="/FileStore/tables/2015.csv"
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df3 = spark.read.format(file_type) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------

file_location = "dbfs:/mnt/ford/Source_Test/2018"

#file_location="/FileStore/tables/2015.csv"
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df4 = spark.read.format(file_type) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------

file_location = "dbfs:/mnt/ford/Source_Test/2019"

#file_location="/FileStore/tables/2015.csv"
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df5 = spark.read.format(file_type) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------

file_location = "dbfs:/mnt/ford/Source_Test/2020"

#file_location="/FileStore/tables/2015.csv"
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df6 = spark.read.format(file_type) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------

#combine 2015,2016,2017,2018,2019,2020
df_join=df.union(df2)
df_join2=df_join.union(df3)
df_join3=df_join2.union(df4)
df_join4=df_join3.union(df5)
final=df_join4.union(df6)

# COMMAND ----------

#final.write.format("csv").save("dbfs:/mnt/ford/final")

# COMMAND ----------

def get_metadata(file_location):
  file_type = "csv"
  #CSV options
  infer_schema = "True"
  first_row_is_header = "True"
  delimiter = ","
  #The applied options are for CSV files. For other file types, these will be ignored.
  df = spark.read.format(file_type) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)
  
  from pyspark.sql.functions import col, countDistinct
  uniqcount=df.agg(*(countDistinct(col(c)).alias(c) for c in df.columns)) #get unique count of each column
  uniq=uniqcount.toPandas()
  uniq=uniq.to_dict(orient='rows')
  uniq=uniq[0]
  categorical_col=[]
  statistical_col=[]
  #get stat and cat columns based on unique count
  for k,v in uniq.items():
    if v<100:
      categorical_col.append(k)
    else:
      statistical_col.append(k) 
  #get dates column
  dates_col = [s for s in statistical_col if "_DT"  in s] + [s for s in statistical_col if "_TS"  in s] +[s for s in statistical_col if "_NB"  in s] 
  
  #remove the common date columns from stats 
  statistical_col=list(set(statistical_col)^set(dates_col))
  #categorical_col=list(set(categorical_col)^set(dates_col))
  print("stats col")
  print(statistical_col)
  print("cat col")
  print(categorical_col)

  ######## Older method to get stats and categorical columns  ###########
  #df=df.toPandas()
  #df_dt = df.filter(regex='_DT')
  #df_nb = df.filter(regex='_NB')
  #df_ts = df.filter(regex='_TS')
  #removal=list(df_dt.columns)+list(df_nb.columns)+list(df_ts.columns)
  #df=df.drop(removal,axis=1)
  #save unique values in n
  #get_distinct_categorical_columns=list(df.filter(regex='_CD'))+list(df.filter(regex='_CT'))+list(df.filter(regex='_IN'))
  #df_categorical=df[get_distinct_categorical_columns]
  #df_categorical = df_categorical.astype(object)
  #df_dict = dict(zip([i for i in df_categorical.columns] , [list(df_categorical[i].unique()) for i in df_categorical.columns]))
  #selected = [s for s in df.columns if '_CT' in s]+[s for s in df.columns if '_CD' in s]+[s for s in df.columns if '_IN' in s]
  #selected = [s for s in df.columns if '_PC' in s]+[s for s in df.columns if '_AM' in s]
  #df_stats=df[['CNCSN_STD_RTE_PC','STD_DISC_RTE_PC','CNCSN_PBT_AM','RTE_CNCSN_AM']]
  #uniq=dict(df.nunique(dropna = False))
  #metadata['count']=uniq
  #########################################
  
  
  #get distinct values for all categorical column
  df_categorical=df.select(categorical_col)
  df_categorical=df_categorical.select([col(c).cast("string") for c in df_categorical.columns])
  import pyspark.sql.functions as f
  distinct=df_categorical.select(*[f.collect_set(c).alias(c) for c in df_categorical.columns])
  distinct=distinct.toPandas()
  dis=distinct.to_dict(orient="rows")
  dis=dis[0]
  metadata={}
  metadata['distinct']=dis
  col=df_categorical.columns
  for i in col:
    metadata['distinct'][i]=metadata['distinct'][i].tolist()
  import pandas as pd
  
  #Get summary statistics for the statistical columns 
  df_stats=df.select(statistical_col)
  df_stats=df_stats.toPandas()
  df_stats=df_stats.replace("NULL", 0)
  df_stats = df_stats.astype(float)
  out=df_stats.describe()
  dfd=out.to_dict()
  main={}
  dfd_list = []
  count = 0
  for key, value in dfd.items():
      main={}
      print(key)
      main["column_name"] =key 
      main.update(value)
      dfd_list.append(main)
  metadata['statistics']=dfd
  import json
  #with open('/dbfs/mnt/ford/metadata.json', 'w') as fp:
  #    json.dump(metadata, fp)
  
  
  return metadata




  
  
  
  

# COMMAND ----------

#final.write.saveAsTable("Final_csv")
#final.write.csv('FileStore/tables/final')

# COMMAND ----------

metadata_2015=get_metadata("dbfs:/mnt/ford/Source_Test/2015")

# COMMAND ----------

metadata_2015

# COMMAND ----------

ZEDV214_AUTO_LOAN=get_metadata("dbfs:/mnt/ford/Source_Test/ZEDV214_AUTO_LOAN.csv")

# COMMAND ----------

ZEDV214_AUTO_LOAN

# COMMAND ----------

file_location = "dbfs:/mnt/ford/Source_Test/2016"
metadata_2016=get_metadata(file_location)


# COMMAND ----------

file_location = "dbfs:/mnt/ford/Source_Test/2017"
metadata_2017=get_metadata(file_location)

# COMMAND ----------

file_location = "dbfs:/mnt/ford/Source_Test/2018"
metadata_2018=get_metadata(file_location)

# COMMAND ----------

file_location = "dbfs:/mnt/ford/Source_Test/2019"
metadata_2019=get_metadata(file_location)

# COMMAND ----------

file_location = "dbfs:/mnt/ford/Source_Test/2020"
metadata_2020=get_metadata(file_location)

# COMMAND ----------

import json
with open('/dbfs/mnt/ford/metadata.json', 'w') as fp:
    json.dump(metadata, fp)
