package com.sam

import com.ggstar.util.ip.IpHelper

/**
  * Ip解析工具类
  */
object IpUtils {


  def getCity(ip:String)={
    IpHelper.findRegionByIp(ip)
  }






}
