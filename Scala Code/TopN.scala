package com.sam



import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * 统计清洗完成的数据
  */
object TopN {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("TopN")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
      .master("local[2]").getOrCreate()

    val accessDF=spark.read.format("parquet").load("/home/sam/Documents/spark/data/data2/clean")

    val day="20170511"

    StatDAO.DeleteData(day)

   videoAccessTopNStat(spark,accessDF,day)

   cityAccessTopNStat(spark,accessDF,day)

     videoTrafficsTopNStat(spark,accessDF,day)

     spark.stop()

  }


  def videoAccessTopNStat(spark:SparkSession,accessDF:DataFrame,day:String):Unit={
    import spark.implicits._
   val videoAccessTopNDF= accessDF.filter($"day"===day && $"cmsType"==="video")
      .groupBy("day","cmsId")
     .agg(count("cmsId").as("times")).orderBy($"times".desc)


    try{

      videoAccessTopNDF.foreachPartition(partitionOfRecords=>{
        val list=new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(info=>{

          val day=info.getAs[String]("day")
          val cmsId=info.getAs[Long]("cmsId")
          val times=info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day,cmsId,times))

        })

        StatDAO.insertDayVideoAccessTopN(list)

      })

    }catch{
      case e:Exception=>e.printStackTrace()
    }

  }


  def cityAccessTopNStat(spark:SparkSession,accessDF:DataFrame,day:String)={

    import spark.implicits._

    val cityAccessTopNDF=accessDF.filter($"day"===day && $"cmsType"==="video")
      .groupBy("day","city","cmsId")
      .agg(count($"cmsId").as("times"))

    val top3DF=cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
        .orderBy(cityAccessTopNDF("times").desc)
    ).as("times_rank")

    ).filter("times_rank<=3")



    try{
      top3DF.foreachPartition(partionsOfRecords=>{

        val list=new ListBuffer[DayCityVideoAccessStat]

        partionsOfRecords.foreach(info=>{

          val day=info.getAs[String]("day")
          val cmsId=info.getAs[Long]("cmsId")
          val city=info.getAs[String]("city")
          val times=info.getAs[Long]("times")
          val timesRank=info.getAs[Int]("times_rank")

          list.append(DayCityVideoAccessStat(day,cmsId,city,times,timesRank))
        })
        StatDAO.insertDayCityVideoAccessTopN(list)

      })

    }catch{
      case e:Exception=>e.printStackTrace()
    }

    }


 def videoTrafficsTopNStat(spark: SparkSession, accessDF: DataFrame,day:String): Unit ={
   import spark.implicits._
   val trafficsDF=accessDF.filter($"day"===day &&$"cmsType"==="video")
     .groupBy("day","cmsId").agg(sum("traffic").as("traffics"))
     .orderBy($"traffics".desc)

   try{
     trafficsDF.foreachPartition(partionsOfRecords=>{
       val list=new ListBuffer[DayVideoTrafficsStat]

       partionsOfRecords.foreach(info=>{
         val day=info.getAs[String]("day")
         val cmsId=info.getAs[Long]("cmsId")
         val traffics=info.getAs[Long]("traffics")
         list.append(DayVideoTrafficsStat(day,cmsId,traffics))

       })
       StatDAO.insertDayVideoTrafficsAccessTopN(list)
     })

   }catch{
     case e:Exception=>e.printStackTrace()
   }
 }



}
