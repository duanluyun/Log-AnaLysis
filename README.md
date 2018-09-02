# HandWritten_Digit_Recoganition
Logs analysis is an ideal use case for Spark. It's a very large, common data source and contains a rich set of information.Logs data can be used for monitoring your servers, improving business and customer intelligence, building recommendation systems, preventing fraud, and much more.This project will show you to use Apache Spark on your organization's production logs and fully harness the power of that data. 



## Requirements:
1.Spark==2.3.0

2.Scala==2.11.8

3.Python==3.5

4.zeppelin==0.7.1

5.maven==3.3.9

8.MySQl




##Data Cleaning

###1. Conversion between the two forms of date and time
####Scala
```scala
import java.util.{Date, Locale}
import org.apache.commons.lang3.time.FastDateFormat
/**
  * 日期解析工具
  * 原日志时间信息：[10/Nov/2016:00:01:02 +0800] 要求的时间格式为：yyyy-MM-dd HH:mm:ss
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
```

####Python




###2.Find location from Ip

![Source Image](https://github.com/duanluyun/HandWritten_Digit_Recoganition/raw/master/Image/1.png)


