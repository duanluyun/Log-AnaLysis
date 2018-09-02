package com.sam


import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat



/**
  * 日期解析工具
  * [10/Nov/2016:00:01:02 +0800]
  */
object DateUtils {


  val DDMMMYYYYHHMM_TIME_FORMAT=FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)

  val Target_FORMAT=FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def parse(time:String):String={
    Target_FORMAT.format(new Date(getTiem(time)))

  }

  def getTiem(time:String):Long={
    try {
      DDMMMYYYYHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.indexOf("]"))).getTime
    }catch{
      case e:Exception=>{
        0l
      }
    }
  }



}
