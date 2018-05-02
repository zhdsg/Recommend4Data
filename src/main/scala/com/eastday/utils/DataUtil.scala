package com.eastday.utils

/**
 * Created by admin on 2018/4/11.
 */
object DataUtil {
  def someOrNone(option :Option[Long] , noneValue:Long) :Long={
    option match {
      case Some(a) => a
      case None  => noneValue
    }
  }
}
