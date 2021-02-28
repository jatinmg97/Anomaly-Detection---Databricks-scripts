# Databricks notebook source
#creating anomalies in dataset and saving it 
file_location = "dbfs:/mnt/ford/Source_Test/2020"
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
df=df.limit(10000)
df=df.toPandas()
df["CO_BUS_CD"][0]=3
df["DEL_CD"][1]="Z"
df["FIN_LABEL_CD"][10]=9999
df.to_csv("/dbfs/mnt/ford/2020_anomalies.csv",index=False)

# COMMAND ----------

import pandas as pd
df=pd.read_csv("/dbfs/mnt/ford/2020_anomalies.csv")
df.nunique()

# COMMAND ----------

#function to get outliers
def get_outliers(file_location):
  #load the metadata from the blob storage
  import json
  with open('/dbfs/mnt/ford/metadata.json') as json_file:
      metadata = json.load(json_file)
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
  uniqcount=df.agg(*(countDistinct(col(c)).alias(c) for c in df.columns))
  uniq=uniqcount.toPandas()
  uniq=uniq.to_dict(orient='rows')
  uniq=uniq[0]
  categorical_col=[]
  statistical_col=[]
  for k,v in uniq.items():
    if v<100:
      categorical_col.append(k)
    else:
      statistical_col.append(k)              
  dates_col = [s for s in statistical_col if "_DT"  in s] + [s for s in statistical_col if "_TS"  in s] +[s for s in statistical_col if "_NB"  in s]
  statistical_col=list(set(statistical_col)^set(dates_col))
  print("stats col")
  print(statistical_col)
  print("cat col")
  print(categorical_col)
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
  df_categorical=df.select(categorical_col)
  import pyspark.sql.functions as f
  df_categorical=df_categorical.select([col(c).cast("string") for c in df_categorical.columns])
  distinct=df_categorical.select(*[f.collect_set(c).alias(c) for c in df_categorical.columns])
  distinct=distinct.toPandas()
  dis=distinct.to_dict(orient="rows")
  dis=dis[0]
  new_metadata={}
  new_metadata['distinct']=dis
  col=df_categorical.columns
  for i in col:
    new_metadata['distinct'][i]=new_metadata['distinct'][i].tolist()
  import pandas as pd
  #selected = [s for s in df.columns if '_PC' in s]+[s for s in df.columns if '_AM' in s]
  df_stats=df.select(statistical_col)
  df_stats=df_stats.toPandas()
  #df_stats=df[['CNCSN_STD_RTE_PC','STD_DISC_RTE_PC','CNCSN_PBT_AM','RTE_CNCSN_AM']]
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
  new_metadata['statistics']=dfd
  out={}
  #compare the new and reference metadata to get the new values
  for (k,v),(k2,v2) in zip(sorted(metadata['distinct'].items()), sorted(new_metadata['distinct'].items())):
        print("historic data","incoming data")
        print(k,k2)
        print(v,v2)
        main_list = [item for item in v2 if item not in v]
        print("New DataPoint")
        print(main_list)
        out[k]=main_list
        #out.append['main_list']
  out= {k:v for k,v in out.items() if v}
  import pandas as pd
  outliers=pd.DataFrame()
  df=df.toPandas()
  for k,v in out.items():
      df2=df[df[k].isin(v)]
      df2['outlier_column']=k
      df2['Review']="Pending"
      outliers=outliers.append(df2)
    
  #outliers.to_csv('/dbfs/mnt/ford/anomalies.csv',index=False)
  return outliers


# COMMAND ----------

outliers=get_outliers("dbfs:/mnt/ford/2020_anomalies.csv")

# COMMAND ----------

outliers

# COMMAND ----------

# save it in blob storage as anomalies.csv
outliers.to_csv('/dbfs/mnt/ford/anomalies.csv',index=False)

# COMMAND ----------


