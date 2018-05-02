package com.eastday.utils

import java.sql.{PreparedStatement, DriverManager, Connection}
import java.util

import com.eastday.conf.ConfigurationManager
import com.eastday.constract.Constract

import com.eastday.domain.LogRecord

/**
 * Created by admin on 2018/4/11.
 * 4/23.迁移值java目录下
 */
  class JDBCHelper private{

  private val datasource :util.LinkedList[Connection]  = new util.LinkedList[Connection]()

  private var i =0
  for( i <- 1 to ConfigurationManager.getInteger(Constract.JDBC_DATASOURCE_SIZE)){
    val url =ConfigurationManager.getString(Constract.JDBC_URL)
    val user= ConfigurationManager.getString(Constract.JDBC_USER)
    val passwd =ConfigurationManager.getString(Constract.JDBC_PASSWD)
    val conn :Connection =DriverManager.getConnection(url,user,passwd)
    datasource.add(conn)
  }

  private def getConn () :Connection ={
    while (datasource.size()==0 ){
      try{
        Thread.sleep(10)
      }catch {
        case e :Exception => e.printStackTrace()
      }
    }
    datasource.poll()
  }

  def executeUpdate(sql :String ): Int ={
    var conn :Connection =null
    var stmt :PreparedStatement =null
    var rnt =0
    try{
      conn =getConn()
      stmt =conn.prepareStatement(sql)
      rnt =stmt.executeUpdate(sql)
    }catch{
      case e :Exception => e.printStackTrace()
    }finally{
      if(conn !=null){
        datasource.push(conn)
      }
    }
    rnt
  }

  def executeDel(sql :String ,params :Array[Any]): Int ={
    var conn :Connection =null
    var stmt :PreparedStatement =null
    var rnt =0
    try{
      conn =getConn()
      conn.setAutoCommit(false)
      stmt =conn.prepareStatement(sql)
      var i  =0
      for( i <- 0 to params.length){

          stmt.setObject(i+1,params(i))
      }
      rnt =stmt.executeUpdate(sql)
    }catch{
      case e :Exception=> e.printStackTrace()
    }finally{
      if(conn !=null){
        datasource.push(conn)
      }
    }
    rnt
  }
}

object JDBCHelper{
  private var jdbcHelper :JDBCHelper =null
  try {
    val driver = ConfigurationManager.getString(Constract.JDBC_DRIVER)
    Class.forName(driver)
  }catch {
    case e :Exception => e.printStackTrace()
  }
  def apply() =getInstance()
  def getInstance() :JDBCHelper ={
    if(jdbcHelper == null ){
      jdbcHelper =new JDBCHelper
    }
    jdbcHelper
  }
  def main(args: Array[String]) {
    val dao ="2222"
    println("da")
  }
}
