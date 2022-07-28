from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

"""
This is utility  class. consist various logic 
"""

"""
getSparkSession : method return spark application 
arguments
appName: name of spark app 
return
sparkSession: new spark session 
"""


def getSparkSession(appName):
    spark = SparkSession \
        .builder \
        .appName(appName) \
        .master("local[2]") \
        .getOrCreate()

    return spark


"""
getMaxValue : method returns max record  
arguments
appName: name of spark app 
return
sparkSession: new spark session 
"""


def getMaxValue(df, colname):
    result = df.select(F.max(colname).alias("max")).collect()[0].asDict()
    return result["max"]
