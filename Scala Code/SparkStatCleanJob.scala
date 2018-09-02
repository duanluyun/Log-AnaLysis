package com.sam

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("SparkStatCleanJob")
      .master("local[2]").getOrCreate()

    val accessRDD=spark.sparkContext.textFile("file:///home/sam/Documents/spark/data/data2/access.log")

    val accessDF=spark.createDataFrame(accessRDD.map(x=>AccessConvertUtil.parseLog(x)),AccessConvertUtil.struct)

    accessDF.coalesce(1).write.format("parquet").partitionBy("day")
      .mode(SaveMode.Overwrite).save("/home/sam/Documents/spark/data/data2/clean")

    spark.stop()
  }

}
