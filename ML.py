# Databricks notebook source
# DBTITLE 1,Data Loading and Pre-Processing
import pandas as pd

# COMMAND ----------

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

# COMMAND ----------



# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df=df.withColumn('OFFR_DT', col('OFFR_DT').cast('timestamp'))

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
df=df.where((col("OFFR_DT") >= lit("2019-01-01")))


# COMMAND ----------

df=df.where((col("OFFR_DT") < lit("2020-01-01")))

# COMMAND ----------

display(df)

# COMMAND ----------

data=df.toPandas()

# COMMAND ----------

data

# COMMAND ----------

data.to_csv("/dbfs/mnt/ford/Source_Test/2019_new",index=False)

# COMMAND ----------

#data=pd.read_csv("/dbfs/mnt/ford/Source_Test/2019_2020")

# COMMAND ----------

data

# COMMAND ----------



# COMMAND ----------

#loading the data
#data=pd.read_csv("/dbfs/mnt/ford/Source_Test/2019")

# COMMAND ----------

data.isna().sum()

# COMMAND ----------

data['ORIG_DECIS_TS'].value_counts()

# COMMAND ----------


#dropping the date columns
data.drop(["EDW_OFFR_NB","EFF_DT","EXPIR_DT","LSTUPDT_DT","DDLP_ACCT_NB","OFFR_DT","OFFR_ENTRY_TS"],axis=1,inplace=True)

# COMMAND ----------


#impute the missing values with 0
#data[['CNCSN_STD_RTE_PC','CNCSN_PBT_AM']]=data[['CNCSN_STD_RTE_PC','CNCSN_PBT_AM']].fillna(0)

# COMMAND ----------

#impute the missing values with NA
#data[['ORIG_BRANCH_CD','SRVC_BRANCH_CD']]=data[['ORIG_BRANCH_CD','SRVC_BRANCH_CD']].fillna("NA")

# COMMAND ----------

data['PURC_DT'].value_counts()

# COMMAND ----------

data['ORIG_DECIS_TS'].value_counts()

# COMMAND ----------

data[['ORIG_DECIS_TS']]=data[['ORIG_DECIS_TS']].replace("NULL",0)

# COMMAND ----------

data[['PURC_DT']]=data[['PURC_DT']].replace("NULL",0)

# COMMAND ----------

data['PURC_DT'].value_counts()

# COMMAND ----------

data.loc[data['PURC_DT']!=0, 'PURC_DT'] = 1

# COMMAND ----------

data.loc[data['ORIG_DECIS_TS']!=0, 'ORIG_DECIS_TS'] = 1

# COMMAND ----------

data['ORIG_DECIS_TS'].unique(),data['ORIG_DECIS_TS'].value_counts()

# COMMAND ----------

data['PURC_DT'].unique(),data['PURC_DT'].value_counts()

# COMMAND ----------

#separate the numerical columns
data_int=data[['CNCSN_STD_RTE_PC','CNCSN_PBT_AM','RTE_CNCSN_AM','STD_DISC_RTE_PC']]
data.drop(['CNCSN_STD_RTE_PC','CNCSN_PBT_AM','RTE_CNCSN_AM','STD_DISC_RTE_PC'],inplace=True,axis=1)

# COMMAND ----------

# data pre-processing steps
from sklearn.preprocessing import RobustScaler
imp_mean = RobustScaler()
imp_mean.fit(data_int)
data_int_transformed= pd.DataFrame(imp_mean.transform(
    data_int), columns=data_int.columns,index=data_int.index)

# COMMAND ----------

data_int

# COMMAND ----------

data

# COMMAND ----------

#label encoding step for categorical column
from sklearn import preprocessing
le = preprocessing.LabelEncoder()
df_encoded = data.apply(le.fit_transform)
print(df_encoded)

# COMMAND ----------

#display the label encoding mapping
for col in data.columns:
  le.fit(data[col])
  le_name_mapping = dict(zip(le.classes_, le.transform(le.classes_)))
  print(col)
  print(le_name_mapping)

# COMMAND ----------

!pip install category_encoders
from category_encoders.target_encoder import TargetEncoder
from category_encoders.count import CountEncoder

# COMMAND ----------

data

# COMMAND ----------

#encoding based on frequency
ce = CountEncoder(cols = data.columns)

df_encoded = ce.fit_transform(data)
print(df_encoded)

# COMMAND ----------

# DBTITLE 1,Unsupervised Algorithm - Isolation Forest
#Fit the Isolation Forest
from sklearn.ensemble import IsolationForest
clf = IsolationForest(random_state=0,verbose=1,max_samples=float(0.01),contamination=0.01,n_estimators=200).fit(df_encoded)


# COMMAND ----------

data['class']=sk_predictions['predicted_class']

# COMMAND ----------

# DBTITLE 1,Tuned Model
from sklearn.preprocessing import RobustScaler
imp_mean = RobustScaler()
imp_mean.fit(df_encoded)
df_encoded= pd.DataFrame(imp_mean.transform(
    df_encoded), columns=df_encoded.columns,index=df_encoded.index)

# COMMAND ----------

df_encoded

# COMMAND ----------

#Fit the Isolation Forest
from sklearn.ensemble import IsolationForest
clf=IsolationForest(random_state=0,verbose=1,n_estimators=1000,n_jobs=-1,max_samples=float(0.005),contamination=0.04,max_features=float(0.5)).fit(df_encoded)

# COMMAND ----------

#Fit the Isolation Forest
from sklearn.ensemble import IsolationForest
clf=IsolationForest(random_state=0,verbose=1,n_estimators=500,contamination=0.04,max_features=float(0.5)).fit(df_encoded)

# COMMAND ----------

#Fit the Isolation Forest
from sklearn.ensemble import IsolationForest

# COMMAND ----------


clf=IsolationForest(random_state=0,verbose=1,n_estimators=1,warm_start=True,contamination=0.04,max_features=float(0.5)).fit(df_encoded)

# COMMAND ----------

#RULE1
ind1=data[(data['ORIG_DECIS_TS']==0) & data['PURC_DT']==1].index.values.tolist()  
df_enc=pd.DataFrame(df_encoded)
df_enc=df_enc.iloc[ind1,:]
df_enc

# COMMAND ----------

data[(data['ORIG_DECIS_TS']==0) & data['PURC_DT']==1]

# COMMAND ----------

#RULE2
ind2=data[(data['APPL_STAT_CD']!="D") & data['PURC_DT']==1].index.values.tolist()  
df_enc=pd.DataFrame(df_encoded)
df_enc2=df_enc.iloc[ind2,:]
df_enc2

# COMMAND ----------

data[(data['APPL_STAT_CD']!="D") & data['PURC_DT']==1]

# COMMAND ----------

pred=clf.predict(df_encoded)

# COMMAND ----------

sk_predictions = pd.DataFrame({
    "predicted_class": list(map(lambda x: 1*(x == -1), pred))
    #"predict": -iso_score
}) 
data['class']=sk_predictions['predicted_class']

# COMMAND ----------

data.to_csv("/dbfs/mnt/ford/Source_Test/pred.csv",index=False)

# COMMAND ----------

#make predictions
pred2=clf.predict(df_enc.iloc[0:100,:])


# COMMAND ----------

sk_predictions = pd.DataFrame({
    "predicted_class": list(map(lambda x: 1*(x == -1), pred2))
    #"predict": -iso_score
}) 

# COMMAND ----------

# DBTITLE 1,ORIG_DECIS_TS is  null and PURC_DT is  not null and  OFFR_DT >= '2019-01-01'
#count of the anomalies
sk_predictions['predicted_class'].value_counts()

# COMMAND ----------

# DBTITLE 1,APPL_STAT_CD not in ('D') and PURC_dt is  not null and  OFFR_DT >= '2019-01-01
#make predictions
pred2=clf.predict(df_enc2)

# COMMAND ----------

sk_predictions = pd.DataFrame({
    "predicted_class": list(map(lambda x: 1*(x == -1), pred2))
    #"predict": -iso_score
}) 


# COMMAND ----------

#count of the anomalies
sk_predictions['predicted_class'].value_counts()

# COMMAND ----------

sk_predictions.hist()

# COMMAND ----------

data['class']=sk_predictions['predicted_class']

# COMMAND ----------

#1 rule
data[(data['ORIG_DECIS_TS']==0) & data['PURC_DT']==1]['class'].value_counts()

# COMMAND ----------

#2 rule
data[(data['APPL_STAT_CD']!="D") & data['PURC_DT']==1]['class'].value_counts()

# COMMAND ----------

# DBTITLE 1,Column Wise Analysis 


# COMMAND ----------

!pip install seaborn
import seaborn as sns
import warnings
import matplotlib.pyplot as plt
from matplotlib import pyplot
warnings.filterwarnings('ignore')

# COMMAND ----------

########################### Uni Variate Analysis #############################

# COMMAND ----------

# Orange = Anomalies
# Blue = Valid points

# COMMAND ----------

import seaborn as sns
sns.distplot((main[main['class']==0])["STD_DISC_RTE_PC"]),sns.distplot((main[main['class']==1])["STD_DISC_RTE_PC"])

# COMMAND ----------

import seaborn as sns
sns.catplot(x="class", y="CNCSN_PBT_AM", data=main)

# COMMAND ----------

import seaborn as sns
sns.catplot(x="class", y="CNCSN_STD_RTE_PC", data=main)

# COMMAND ----------

################################## Bi-variate Analysis  ####################################

# COMMAND ----------

#create a temporary dataframe to plot the bi variate analysis
temp=main.copy()
temp=temp.astype('str')

# COMMAND ----------

sns.catplot(x="APPL_TYPE_CD", y="OFFR_STAT_CD",hue="class", data=temp)

# COMMAND ----------

sns.catplot(x="OFFR_STAT_CD", y="FIN_LABEL_CD",hue="class", data=temp)

# COMMAND ----------

import seaborn as sns
sns.catplot(x="OFFR_SRC_CD", y="FIN_LABEL_CD",hue="class", data=temp)

# COMMAND ----------

sns.catplot(x="OFFR_SRC_CD", y="REWRT_TRNSFR_CD",hue="class", data=temp)

# COMMAND ----------

sns.catplot(x="ECA_APRV_IN", y="STD_DISC_RTE_PC",hue="class", data=main)

# COMMAND ----------

main

# COMMAND ----------

main2=main.sample(300000)
!pip install plotly
import plotly.express as px

fig = px.scatter_3d(main2, x='REWRT_TRNSF_CD', y='OFFR_SRC_CD', z='ECA_APRV_IN',
              color='class')
fig.show()

# COMMAND ----------

# DBTITLE 1,Extracting Rules including only CD columns
data

# COMMAND ----------

Y_train=data['class']

# COMMAND ----------

data2=data.drop("class",axis=1)

# COMMAND ----------

#encoding based on target mean
ce = TargetEncoder(cols = data2.columns)

df_encoded = ce.fit_transform(data2,Y_train)
print(df_encoded)

# COMMAND ----------

df_encoded.iloc[1432],data2.iloc[1432]

# COMMAND ----------

rule1=df_encoded[df_encoded.index.isin(ind1)]

# COMMAND ----------

rule1

# COMMAND ----------

rule2=df_encoded[df_encoded.index.isin(ind2)]

# COMMAND ----------

rule2

# COMMAND ----------

mapp={}
for col in data2:
  a=list(data2[col].unique())
  b=list(df_encoded[col].unique())
  mapp[col]=[]
  le_name_mapping={}
  res = {} 
  for key in a: 
      for value in b: 
          res[key] = value 
          b.remove(value) 
          break  
  
  mapp[col].append(res)

# COMMAND ----------

mapp

# COMMAND ----------

from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn import tree
clf = DecisionTreeClassifier(max_depth=None,max_leaf_nodes=700,random_state = 0)
# Step 3: Train the model on the data
clf.fit(df_encoded, Y_train)

# COMMAND ----------

#data=pd.read_csv('/dbfs/mnt/ford/Source_Test/pred.csv')

# COMMAND ----------

#get the predictions 
pred=clf.predict(df_encoded)
pred

# COMMAND ----------

#calculate evaluation metric of the model
from sklearn.metrics import f1_score
f1_score(data['class'],pred)

# COMMAND ----------

#display the label encoding mapping
#mapp={}
#for col in data.columns:
#  le.fit(data[col])
#  le_name_mapping = dict(zip(le.classes_, le.transform(le.classes_)))
#  print(col)
#  print(le_name_mapping)
#  mapp[col]=[]
#  mapp[col].append(le_name_mapping)

# COMMAND ----------

n_nodes = clf.tree_.node_count
children_left = clf.tree_.children_left
children_right = clf.tree_.children_right
feature = clf.tree_.feature
threshold = clf.tree_.threshold

def find_path(node_numb, path, x):
        path.append(node_numb)
        if node_numb == x:
            return True
        left = False
        right = False
        if (children_left[node_numb] !=-1):
            left = find_path(children_left[node_numb], path, x)
        if (children_right[node_numb] !=-1):
            right = find_path(children_right[node_numb], path, x)
        if left or right :
            return True
        path.remove(node_numb)
        return False


def get_rule(path, column_names):
    mask = ''
    for index, node in enumerate(path):
        #We check if we are not in the leaf
        if index!=len(path)-1:
            # Do we go under or over the threshold ?
            if (children_left[node] == path[index+1]):
                mask += "(df['{}']<= {}) \t ".format(column_names[feature[node]], threshold[node])
            else:
                mask += "(df['{}']> {}) \t ".format(column_names[feature[node]], threshold[node])
    # We insert the & at the right places
    mask = mask.replace("\t", "&", mask.count("\t") - 1)
    mask = mask.replace("\t", "")
    return mask
  
def get_rule2(path, column_names):
    mask = ''
    for index, node in enumerate(path):
        #We check if we are not in the leaf
        if index!=len(path)-1:
            # Do we go under or over the threshold ?
            if (children_left[node] == path[index+1]):
                
                le_name_mapping = mapp[column_names[feature[node]]][0]
                #print(le_name_mapping)
                names=[k for k,v in le_name_mapping.items() if v <=threshold[node]]
                mask += "({} is in {}) \t ".format(column_names[feature[node]], names)
                #mask += "(df['{}']<= {}) \t ".format(column_names[feature[node]], threshold[node])
            else:
                #le.fit(data[column_names[feature[node]]])
                le_name_mapping = mapp[column_names[feature[node]]][0]
                #print(le_name_mapping)
                names=[k for k,v in le_name_mapping.items() if v >=threshold[node]]
                mask += "({} is in {}) \t ".format(column_names[feature[node]], names)
    # We insert the & at the right places
    mask = mask.replace("\t", "&", mask.count("\t") - 1)
    mask = mask.replace("\t", "")
    return mask

# COMMAND ----------

import numpy as np
# Leaves
leave_id = clf.apply(df_encoded)

paths ={}
for leaf in np.unique(leave_id):
    path_leaf = []
    find_path(0, path_leaf, leaf)
    paths[leaf] = np.unique(np.sort(path_leaf))

rules_columns = {}
for key in paths:
    rules_columns[key] = get_rule2(paths[key], df_encoded.columns)

# COMMAND ----------

rules[7575]

# COMMAND ----------

import numpy as np
# Leaves
leave_id = clf.apply(df_encoded)

paths ={}
for leaf in np.unique(leave_id):
    path_leaf = []
    find_path(0, path_leaf, leaf)
    paths[leaf] = np.unique(np.sort(path_leaf))

rules = {}
for key in paths:
    rules[key] = get_rule(paths[key], df_encoded.columns)

# COMMAND ----------

# DBTITLE 1,Rules excluding percent/amount columns
rules

# COMMAND ----------


rules.keys()

# COMMAND ----------


df=df_encoded.copy()

# COMMAND ----------

data

# COMMAND ----------

number_anomalies=[]
rule_no=[]
for k,v in rules.items():
  try:
    rul=df_encoded[eval(rules[k])]
    ind=rul.index.values.tolist()
    rul_df=data[data.index.isin(ind)]
    no=len(rul_df[rul_df['class']==0])
    no2=len(rul_df[rul_df['class']==1])
    if no==0:
      number_anomalies.append(no2)
      rule_no.append(k)
  except: 
    pass

# COMMAND ----------

number_anomalies

# COMMAND ----------

#sort the rules in asceding order of the number of anomalies
Z = [x for _,x in sorted(zip(number_anomalies,rule_no))]
print(Z)

# COMMAND ----------

#total number of rules for anomalies
len(Z)

# COMMAND ----------

rules[112]

# COMMAND ----------

rule_keys=list(rules.keys())

# COMMAND ----------

#################################
anomalies=[]
key=[]
for i in rule_keys:
  rul=rule1[eval(rules[i])]  
  
  no_ano=len(rul)
  if no_ano>0:
    anomalies.append(no_ano)
    key.append(i)
  
  

  
  
  
  
  


# COMMAND ----------

anomalies,key

# COMMAND ----------

rules_columns[564]

# COMMAND ----------

#################################
anomalies=[]
key=[]
for i in rule_keys:
  rul=rule2[eval(rules[i])]  
  
  no_ano=len(rul)
  if no_ano>0:
    anomalies.append(no_ano)
    key.append(i)
  

# COMMAND ----------

anomalies,key

# COMMAND ----------

rules_columns[1334]

# COMMAND ----------



# COMMAND ----------

#get all the records in the data based on a specific rule
rul=df_encoded[eval(rules[115])]
ind=rul.index.values.tolist()
rul_df=data[data.index.isin(ind)]
rul_df

# COMMAND ----------

#get the number of anomalies for that rule
rul_df['class'].value_counts()

# COMMAND ----------

rules_columns[26]

# COMMAND ----------

rules_columns[8]

# COMMAND ----------

rules_columns[37]

# COMMAND ----------

rules_columns[7]

# COMMAND ----------

rules_columns[576]

# COMMAND ----------

rules_columns[1089]

# COMMAND ----------

rules_columns[579]

# COMMAND ----------

rules_columns[93]

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


