from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

data_path = 'jobs.csv'


class SparkTask:
    def __init__(self, spark_session):
        self.job_counts_dict = None
        self.sc = spark_session.sparkContext
        self.spark = spark_session

    def group_sort(self, input_path):
        df = self.spark.read.option("header","true").csv(input_path)

        job_counts = (df.groupBy("job").agg(count("*").alias("count")).orderBy(col("count"),
        col("job")).collect())

        self.job_counts_dict = {row["job"]: row["count"] for row in job_counts}

        return self.job_counts_dict

if __name__ == "__main__":
    spark = SparkSession.builder.appName("JobCount").getOrCreate()
    task = SparkTask(spark)
    result = task.group_sort(data_path)
    print(result)

