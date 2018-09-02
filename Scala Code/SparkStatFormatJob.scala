package com.sam

import org.apache.spark.sql.SparkSession

object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().appName("SparkStatFormatJob")
      .master("local[2]").getOrCreate()

    val access=spark.sparkContext.textFile("file:///home/sam/Documents/spark/data/data2/10000_access.log")

    access.map(line=>{
      val splits=line.split(" ")
      val ip=splits(0)
      val time=splits(3)+" "+splits(4)
      val url=splits(11).replace("\"","")
      val traffic=splits(9)

      DateUtils.parse(time)+"\t"+url+"\t"+traffic+"\t"+ip

    }).saveAsTextFile("file:///home/sam/Documents/spark/data/data2/output")
    spark.stop()
  }
}
