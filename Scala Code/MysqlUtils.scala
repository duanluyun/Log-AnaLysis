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
