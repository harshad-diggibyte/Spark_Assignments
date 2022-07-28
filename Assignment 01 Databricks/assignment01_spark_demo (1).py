# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('assignment_01').getOrCreate()
spark

# COMMAND ----------

user_data =[(101,"abc.123@gmail.com","hindi","mumbai"),
(102,"jhon@gmail.com","english","usa"),
(103,"madan.44@gmail.com","marathi","nagpur"),
(104,"local.88@outlook.com","tamil","chennai"),
(105,"sahil.55@gmail.com","english","usa"),
(106,"adi@gmail.com","hindi","nagpur"),
(107,"jason@gmail.com","marathi","mumbai"),
(108,"sohan@gmail.com","kannad","usa"),
(109,"case@outlook.com","tamil","mumbai"),
(110,"fury@gmail.com","hindi","nagpur")]

user_schema = ["user_id","emailid","nativelanguage","location" ]

userDF= spark.createDataFrame(data=user_data,schema=user_schema)

display(userDF)

# COMMAND ----------

transaction_data =[(3300101,1000001,101,700,"mouse"),
(3300102,1000002,102,900,"keyboard"),
(3300103,1000003,103,34000,"tv"),
(3300104,1000004,101,35000,"fridge"),
(3300105,1000005,105,55000,"sofa"),
(3300106,1000006,106,100,"bed"),
(3300107,1000007,105,66000,"laptop"),
(3300108,1000008,108,20000,"phone"),
(3300109,1000009,101,500,"speaker"),
(3300110,1000010,102,1000,"chair")]

transaction_schema = ["transaction_id","product_id","userid","price","product_description" ]

transactionDF= spark.createDataFrame(data=transaction_data,schema=transaction_schema)

display(transactionDF)

# COMMAND ----------

display(userDF)
display(transactionDF)

# COMMAND ----------

df_join = userDF.join(transactionDF,userDF.user_id == transactionDF.userid,"full")
display(df_join)

# COMMAND ----------

#a)Count of unique locations where each product is sold.
df_join.groupby('location').count().show()


# COMMAND ----------

#b)	Find out products bought by each user.
df_join.groupby(['user_id','product_description']).count().show()

# COMMAND ----------

productcount=df_join.groupby(['user_id','product_description']).count()
display(productcount.na.drop())

# COMMAND ----------

#c)Total spending done by each user on each product.c)	Total spending done by each user on each product.
df_join.groupby(['user_id','product_description','price']).avg('price').show()

# COMMAND ----------

spendingbyuser=df_join.groupby(['user_id','product_description','price']).avg('price')
display(spendingbyuser.na.drop())

# COMMAND ----------

df_join = userDF.join(transactionDF,userDF.user_id == transactionDF.userid,"outer")
display(df_join)

# COMMAND ----------


