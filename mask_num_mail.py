from pyspark import *
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("mask").getOrCreate()
def mask_email(col_email):
    usr = col_email.split("@")[0]
    charList = list(usr)
    mask_mail = ''
    for i,ch in enumerate (charList):
        if (i == 0) or (i==len(charList)-1):
            mask_mail += ch
        else:
            mask_mail += '*'
    return mask_mail+'@'+col_email.split("@")[1]

def mask_mobile(col_mobile):
    charList = list(col_mobile)
    mask_num = ''
    for i,ch in enumerate(col_mobile):
        if(i==0) or (i ==1) or (i==len(charList)-2) or (i==len(charList)-1):
            mask_num += ch
        else:
            mask_num += '*'
    return mask_num




#declaring UDF
mask_email_udf = udf(mask_email,StringType())
mask_mobile_udf = udf(mask_mobile,StringType())


df = spark.read.format("csv").option("header",True).load("C:\\Users\\Kamal Nayan\\Desktop\\Data Engineering\\dataset-master\\mask_data.csv")
df = df.withColumn("email",mask_email_udf(df["email"]))\
       .withColumn("mobile", mask_mobile_udf(df["mobile"]))
df.show(10)
# print(mask_email("kamalnayan242@gmail.com"))
