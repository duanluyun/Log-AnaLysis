"""
利用spark对数据进行二次清洗
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import AccessConvertUtl

if __name__=="__main__":
    spark=SparkSession.builder.appName('SparkStatCleanJob').master('local[2]').getOrCreate()
    accessRDD=spark.sparkContext.textFile('file:///home/sam/Documents/spark/data/logoutput/access2.log')
    # RDD==>DF
    schema=StructType(AccessConvertUtl.fileds)
    accessDF=spark.createDataFrame(accessRDD.map(lambda line:AccessConvertUtl.parselog(line)),schema)
    # accessDF.printSchema()
    # accessDF.show(20000)
    accessDF.coalesce(1).write.partitionBy("day").format("parquet").mode('Overwrite').save("file:///home/sam/Documents/spark/data/clean")
    spark.stop()

