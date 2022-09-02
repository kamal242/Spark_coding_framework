import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import  IntegerType
import data_pipeline
class Ingest:

    def __init__(self,spark):
        self.spark = spark

    def ingest_data(self):
        print("Ingesting")
        # my_list = [1,2,3]
        # df = self.spark.createDataFrame(my_list,IntegerType())
        customer_df = self.spark.read.csv("C:\\Users\\Kamal Nayan\\Desktop\\Data Engineering\\Dataset\\retailstore.csv",header=True)
        # customer_df.show()
        customer_df.describe().show()
        customer_df.groupBy("Gender").agg({"Salary":"avg","age":"max"}).show()


