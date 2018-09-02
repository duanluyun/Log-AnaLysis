# HandWritten_Digit_Recoganition
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

#### Python

```python
from datetime import datetime

def getTimeStr(time):
    try:
        time_str=time[time.index('[')+1:time.index(' ')]
        return time_str
    except:
        return '0l'


def reformat(time):
    time_str=getTimeStr(time)
    time=datetime.strptime(time_str,'%d/%b/%Y:%H:%M:%S')
    time=time.strftime('%Y-%m-%d %H:%M:%S')
    return time

```




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


