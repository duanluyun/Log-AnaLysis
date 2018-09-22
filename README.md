# Spark logs Analyzer
Logs analysis is an ideal use case for Spark. It's a very large, common data source and contains a rich set of information.Logs data can be used for monitoring your servers, improving business and customer intelligence, building recommendation systems, preventing fraud, and much more.This project will show you to use Apache Spark on your organization's production logs and fully harness the power of that data. 



## Requirements:
1.Spark==2.3.0

2.Scala==2.11.8

3.Python==3.5

4.zeppelin==0.7.1

5.maven==3.3.9

8.MySQl




## Data Cleaning

### 1. Conversion between the two forms of date and time
#### Scala
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

![Scala SparkStatFromatJob](https://github.com/duanluyun/Log-AnaLysis/blob/master/images/DeepinScreenshot_select-area_20180902210627.png)


#### Python

```python
from datetime import datetime

def getTimeStr(time):
    try:
        time_str=time[time.index('[')+1:time.index(' ')]
        return time_str
    except:
        return '0'


def reformat(time):
    time_str=getTimeStr(time)
    time=datetime.strptime(time_str,'%d/%b/%Y:%H:%M:%S')
    time=time.strftime('%Y-%m-%d %H:%M:%S')
    return time

```

![Python SparkStatFromatJob](https://github.com/duanluyun/Log-AnaLysis/blob/master/images/DeepinScreenshot_select-area_20180902210756.png)


### 2.Find location from Ip

#### Scala

##### 1. DownLoad Ipdata base from github

![Source Image](https://github.com/duanluyun/Log-AnaLysis/blob/master/images/DeepinScreenshot_select-area_20180831234656.png)

##### 2. Import Maven dependency
![Import dependency](https://github.com/duanluyun/Log-AnaLysis/blob/master/images/DeepinScreenshot_select-area_20180902204416.png)

```Scala
import com.ggstar.util.ip.IpHelper

/**
  * Ip解析工具类
  */
object IpUtils {

  def getCity(ip:String)={
    IpHelper.findRegionByIp(ip)
  }

}
```
![Convert RDD To DataFrame](https://github.com/duanluyun/Log-AnaLysis/blob/master/images/DeepinScreenshot_select-area_20180902211455.png)

![AccessConvertUtil](https://github.com/duanluyun/Log-AnaLysis/blob/master/images/DeepinScreenshot_select-area_20180902211635.png)
#### Python
```python
import geoip2.database



def getIp(ip):

    reader = geoip2.database.Reader('/home/sam/Documents/spark/data/GeoLite2-City_20180807/GeoLite2-City.mmdb')
    response=reader.city(ip)
    city="{}".format(response.subdivisions.most_specific.names["zh-CN"])
    return city
    # 有多种语言，我们这里主要输出英文和中文
    #print("你查询的IP的地理位置是:")
    #print("地区：{}({})".format(response.continent.names["es"],response.continent.names["zh-CN"]))

    #print("国家：{}({}) ，简称:{}".format(response.country.name,response.country.names["zh-CN"],response.country.iso_code))

    #print("洲／省：{}({})".format(response.subdivisions.most_specific.name,response.subdivisions.most_specific.names["zh-CN"]))

    # print("城市：{}({})".format(response.city.name, response.city.names["zh-CN"]))
    #
    # print("经度：{}，纬度{}".format(response.location.longitude,response.location.latitude))
    #
    # print("时区：{}".format(response.location.time_zone))
    #
    # print("邮编:{}".format(response.postal.code))


```
![Convert RDD To DataFrame](https://github.com/duanluyun/Log-AnaLysis/blob/master/images/DeepinScreenshot_select-area_20180902212429.png)

![AccessConvertUtil](https://github.com/duanluyun/Log-AnaLysis/blob/master/images/DeepinScreenshot_select-area_20180902212359.png)

## Write To Database

### 1.MysqlUtils to get and relese Connection
```scala
package com.sam

import java.sql.{Connection, DriverManager, PreparedStatement}

object MysqlUtils {

  def getConnection()={
    DriverManager.getConnection("jdbc:mysql://localhost:3306/logproject?user=root&password=dly920329")
  }

  def release(connection:Connection,pstmt:PreparedStatement )={
    try{
      if (pstmt!=null){
        pstmt.close()
      }

    }catch{
      case e:Exception=>{e.printStackTrace()}
    }finally {
      if (connection!=null){
        connection.close()
      }
    }

  }
 }
```
### 2.Create case class
```scala
case class DayCityVideoAccessStat(day:String, cmsId:Long, city:String, times:Long, timesRank:Int)

case class DayVideoAccessStat(day:String,cmsId:Long,times:Long)

case class DayVideoTrafficsStat (day:String,cms_id:Long,traffics:Long)
```
### 3.DAO
```scala
package com.sam

import java.sql.{PreparedStatement,Connection}

import scala.collection.mutable.ListBuffer

object StatDAO {

  def insertDayVideoAccessTopN(list:ListBuffer[DayVideoAccessStat])={

    var connection:Connection=null
      var pstmt:PreparedStatement=null

    try{

      connection=MysqlUtils.getConnection()

      connection.setAutoCommit(false)
      val sql="insert into day_video_access_topn_stat(day,cms_id,times) values(?,?,?)"

      pstmt=connection.prepareStatement(sql)

      for(ele<-list){
        pstmt.setString(1,ele.day)
        pstmt.setLong(2,ele.cmsId)
        pstmt.setLong(3,ele.times)

        pstmt.addBatch()
      }

      pstmt.executeBatch()

      connection.commit()

    }catch{

      case e:Exception=>e.printStackTrace()

    }finally{
      MysqlUtils.release(connection,pstmt)

    }

  }


  def insertDayCityVideoAccessTopN(list:ListBuffer[DayCityVideoAccessStat])={

    var connection:Connection=null
    var pstmt:PreparedStatement=null

    try{

      connection=MysqlUtils.getConnection()

      connection.setAutoCommit(false)
      val sql="insert into  day_video_city_access_topn_stat(day,cms_id,city,times,times_rank) values(?,?,?,?,?)"

      pstmt=connection.prepareStatement(sql)

      for(ele<-list){
        pstmt.setString(1,ele.day)
        pstmt.setLong(2,ele.cmsId)
        pstmt.setString(3,ele.city)
        pstmt.setLong(4,ele.times)
        pstmt.setInt(5,ele.timesRank)

        pstmt.addBatch()
      }

      pstmt.executeBatch()

      connection.commit()

    }catch{

      case e:Exception=>e.printStackTrace()

    }finally{
      MysqlUtils.release(connection,pstmt)

    }

  }


  def insertDayVideoTrafficsAccessTopN(list:ListBuffer[DayVideoTrafficsStat]): Unit ={

    var connection:Connection=null
    var pstmt:PreparedStatement=null
    try{
      connection=MysqlUtils.getConnection()
      connection.setAutoCommit(false)
      val sql="insert into day_video_traffics_topn_stat(day,cms_id,traffics) values(?,?,?)"
      pstmt=connection.prepareStatement(sql)

      for(ele<-list){
        pstmt.setString(1,ele.day)
        pstmt.setLong(2,ele.cms_id)
        pstmt.setLong(3,ele.traffics)
        pstmt.addBatch()

      }

      pstmt.executeBatch()
      connection.commit()

    }catch{
      case e:Exception=>e.printStackTrace()

    }finally{

      MysqlUtils.release(connection,pstmt)

    }

  }

  def DeleteData(day:String): Unit ={
    val tables=Array("day_video_access_topn_stat"," day_video_city_access_topn_stat","day_video_traffics_topn_stat")

    var connection:Connection=null
    var pstmt:PreparedStatement=null

    try{
      connection=MysqlUtils.getConnection()
      for(table<-tables){

        val deleteSQL=s"delete from $table where day = ?"
        pstmt=connection.prepareStatement(deleteSQL)
        pstmt.setString(1,day)
        pstmt.executeUpdate()

      }

    }catch{
      case e:Exception=>e.printStackTrace()
    }finally{
      MysqlUtils.release(connection,pstmt)
    }

  }

}


```
### 4.Data visualization
![Data visualization](https://github.com/duanluyun/Log-AnaLysis/blob/master/images/DeepinScreenshot_select-area_20180902105123.png)
![Data visualization](https://github.com/duanluyun/Log-AnaLysis/blob/master/images/DeepinScreenshot_select-area_20180902105141.png)
![Data visualization](https://github.com/duanluyun/Log-AnaLysis/blob/master/images/DeepinScreenshot_select-area_20180902110337.png)

### 5.Spark on Yarn
#### 1.Create Tables
```SQL
create table day_video_access_topn_stat (
day varchar(8) not null,
cms_id bigint(10) not null,
times bigint(10) not null,
primary key (day, cms_id)
);


create table day_video_city_access_topn_stat (
day varchar(8) not null,
cms_id bigint(10) not null,
city varchar(20) not null,
times bigint(10) not null,
times_rank int not null,
primary key (day, cms_id, city)
);

create table day_video_traffics_topn_stat (
day varchar(8) not null,
cms_id bigint(10) not null,
traffics bigint(20) not null,
primary key (day, cms_id)
);
```
