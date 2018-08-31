from pyspark.sql import SparkSession



def videoAccessTopNStat(spark,accessDF):
    accessDF.createOrReplaceTempView("access_logs")
    accessDF=spark.sql("select day,cmsId,count(1) as times  from access_logs where day='20170511' and cmsType='video'"
                       +"group by day,cmsId order by times desc")
    accessDF.show()
    accessDF.write.format("org.elasticsearch.spark.sql").option("es.nodes","192.168.1.4:9200").mode("overwrite").save("TopN/day")




if __name__=='__main__':
    spark = SparkSession.builder.appName('TopNStatJob')\
        .config('spark.sql.sources.partitionColumnTypeInference.enabled','false')\
        .master("local[2]")\
        .getOrCreate()
    accessDF=spark.read.format('parquet').load('file:///home/sam/Documents/spark/data/clean')
    videoAccessTopNStat(spark,accessDF)
    spark.stop()