
import ingest
import transform
import persist
import pyspark
from pyspark.sql import SparkSession

class Pipeline:

    def run_pipeline(self):
        print("Running Pipeline")
        ingest_process = ingest.Ingest(self.spark)
        ingest_process.ingest_data()
        tranform_process = transform.Transform()
        tranform_process.transform_data()
        persist_process = persist.Persist()
        persist_process.persist_data()
        return
    def create_spark_session(self):
        self.spark = SparkSession.builder\
                    .appName("spark with logging and class")\
                    .enableHiveSupport().getOrCreate()

if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.run_pipeline()

