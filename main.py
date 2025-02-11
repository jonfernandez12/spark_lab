from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
import os


def main():
    print("Hello from spark-lab!")
    os.environ["JAVA_HOME"] = '/Library/Java/JavaVirtualMachines/jdk-18.0.2.1.jdk/Contents/Home/'
    conf = SparkConf() \
        .setAppName("MyApp") \
        .setMaster("local") \
        .set("spark.driver.extraClassPath","~/Users/jonfernandez/Documents/spark_lab/")

    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession(sc)


if __name__ == "__main__":
    main()
