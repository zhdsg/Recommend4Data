package com.eastday.domain

/**
 * Created by admin on 2018/4/10.
 */
case class LogRecord(dt:Int,logTime:Long,qId:String,PV:Long
                      ,UV:Long,IP:Long,incrPV:Long,incrUV:Long,incrIP:Long)

case class APPLogRecord(dt:Int,logTime:Long,apptypeId:String ,qId:String,PV:Long
                        ,UV:Long,IP:Long,incrPV:Long,incrUV:Long,incrIP:Long)

