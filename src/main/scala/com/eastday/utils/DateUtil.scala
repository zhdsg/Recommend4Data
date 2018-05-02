package com.eastday.utils

import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

/**
 * Created by admin on 2018/4/3.
 */
object DateUtil {

  val DT_FORMAT =new SimpleDateFormat("yyyyMMdd")
  val TIME_FORMAT =new SimpleDateFormat("yyyy-MM-dd")
  val CURRENT_DATE_FORMAT=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def getFormatTime(date:String):String= {
    val yyyy =date.substring(0,4)
    val mm =date.substring(4,6)
    val dd =date.substring(6,8)
    val hh =date.substring(8,10)
    val ms =date.substring(10,12)
    val ss =date.substring(12,14)
    s"${yyyy}-${mm}-${dd} ${hh}:${ms}:${ss}"
  }
  def getYesterdayDate(date :Date):String={
    val  cal :Calendar =Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DAY_OF_YEAR,-1)

    DT_FORMAT.format(cal.getTime)
  }
  def getYesterdayDate(date :String):String={
    getYesterdayDate(str2Date(date))
  }

  /**
   *
   * @param date
   * @param x
   * @return
   */
  def getXdaysAgo(date:String,x:Int): String ={
    val date1 =DT_FORMAT.parse(date)
    val  cal :Calendar =Calendar.getInstance()
    cal.setTime(date1)
    cal.add(Calendar.DAY_OF_YEAR,0-x)
    DT_FORMAT.format(cal.getTime)
  }
  /**
   * 规整格式yyyyMMdd
   * @return
   */
  def getTodayDate(date :Date) :String ={
    DT_FORMAT.format(date)
  }
  /**
   * 规整格式yyyyMMdd
   * @return
   */
  def getTodayDate(date :String) :String ={
    getTodayDate(str2Date(date))
  }

  /**
   * 字符串转时间
   * @param date
   * @return
   */
  def str2Date(date :String):Date={
    CURRENT_DATE_FORMAT.parse(date)
  }

  /**
   * 日期转字符串
   * @param date
   * @return
   */
  def date2Str(date :Date):String={
    CURRENT_DATE_FORMAT.format(date)
  }

  /**
   * 获取date的零时零分零秒
   * @param date
   * @return
   */
  def getZeroTime(date :String):Date={
    CURRENT_DATE_FORMAT.parse(date.substring(0,10)+" 00:00:00")
  }
  def getZeroTime(date :Date):Date={
    getZeroTime(date2Str(date))
  }
  /**
   * 将分钟取整，10的倍数,秒数取00
   * 如2018-04-09 11:11:11 -> 2018-04-09 11:10:00
   * @param date
   * @return  字符串
   */
  def trimDate(date :String):String ={
    val day =date.split(" ")(0)
    val time =date.split(" ")(1)
    val hour =time.split(":")(0)
    val mm =s"${time.split(":")(1).substring(0,1)}0"
    s"${day} ${hour}:${mm}:00"
  }

  /**
   *将分钟取整，10的倍数,秒数取00
   * 如2018-04-09 11:11:11 -> 2018-04-09 11:10:00
   * @param date1
   * @return Date
   */
  def trimDate(date1 :Date):String ={
    val date =date2Str(date1)
    trimDate(date)
  }
  def main(args: Array[String]) {
    println(null.equals(null))
  }
}
