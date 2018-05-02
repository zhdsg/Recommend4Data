package com.eastday.conf

import java.io.InputStream
import java.util.Properties


/**
 * Created by admin on 2018/4/3.
 */
object ConfigurationManager {

  val prop =new Properties();
  val in :InputStream =ConfigurationManager.getClass.getClassLoader.getResourceAsStream("my.properties");
  prop.load(in)

  def getString(value:String): String ={
    prop.getProperty(value)
  }

  def getInteger(value:String) :Int={
    prop.getProperty(value).toInt
  }
  def getBoolean(value:String) :Boolean ={
    prop.getProperty(value).toBoolean
  }
  def getLong(value:String) :Long={
    prop.getProperty(value).toLong
  }
}
