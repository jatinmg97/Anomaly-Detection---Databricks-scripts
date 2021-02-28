# Databricks notebook source
import pandas as pd
df=pd.read_csv("/dbfs/mnt/ford/anomalies.csv")

# COMMAND ----------

df['Review'][0]=False
df['Review'][1]=True
df

# COMMAND ----------

import json
with open('/dbfs/mnt/ford/metadata.json') as json_file:
    metadata = json.load(json_file)

# COMMAND ----------

errors=pd.DataFrame(columns=df.columns)
errors

# COMMAND ----------


for index, row in df.iterrows():
    
    if row['Review']==True:
        errors=errors.append(row)
        #errors=errors.iloc[:,-1]
        df.drop(index, inplace=True) 
    elif row['Review']==False:  
      a=row['outlier_column']
      print(a)
      lst=metadata['distinct'][a]
      print(lst)
      print(row[a])
      b=row[a]
      lst.append(b)
      print(lst)
      #metadata['distinct'][a]=lst 
      metadata['distinct'].update({a:lst})
      df.drop(index,inplace=True)
    else:
      pass
      
      

# COMMAND ----------

df

# COMMAND ----------

errors

# COMMAND ----------

metadata['distinct']

# COMMAND ----------

errors.to_csv("/dbfs/mnt/ford/errors.csv",index=False)

# COMMAND ----------


