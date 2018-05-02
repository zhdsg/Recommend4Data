package com.eastday.spark

import com.eastday.conf.ConfigurationManager
import com.eastday.constract.Constract
import com.eastday.utils.{DateUtil, ETLUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by admin on 2018/4/28.
 */
object GetDataSet4Recommend {
  def main(args: Array[String]) {

    if(args.length==0){
      println("please input params........")
      System.exit(-1)
    }

    //log.info(s"start time is ${startTime}")
    //    val dateStr :StringBuffer=new StringBuffer("")
    //    dateStr.append(DateUtil.getFormatTime(args(0)))
    //spark 具体句柄创建
    val conf: SparkConf = new SparkConf()
      .setAppName(Constract.SPARK_APP_NAME_RECOMMEND1)
      .set(Constract.SPARK_SHUFFLE_CONSOLIDATEFILES
        , ConfigurationManager.getString(Constract.SPARK_SHUFFLE_CONSOLIDATEFILES))
      .set(Constract.SPARK_SHUFFLE_FILE_BUFFER
        , ConfigurationManager.getString(Constract.SPARK_SHUFFLE_FILE_BUFFER))
      .set(Constract.SPARK_REDUCER_MAXSIZEINFLIGHT
        , ConfigurationManager.getString(Constract.SPARK_REDUCER_MAXSIZEINFLIGHT))
      .set(Constract.SPARK_SHUFFLE_IO_MAXRETRIES
        , ConfigurationManager.getString(Constract.SPARK_SHUFFLE_IO_MAXRETRIES))
      .set(Constract.SPARK_DEFAULT_PARALLELISM
        , ConfigurationManager.getString(Constract.SPARK_DEFAULT_PARALLELISM))
    //val sc: SparkContext = SparkContext.getOrCreate(conf)
    val sc = new SparkContext(conf)
    //设置读取时长
    //  sc.hadoopConfiguration.set(Constract.DFS_CLIENT_SOCKET_TIMEOUT
    //    ,ConfigurationManager.getString(Constract.DFS_CLIENT_SOCKET_TIMEOUT))

    val dt =args(0)


    try{

      val dataRDD =getDataSet(sc,dt,dt)
      val dataSourceRDD =dataRDD.filter(row=>{
        val url =row._3
        val url_click =row._2
        if(url ==null || url_click==null){
          false
        }else{
          (!url.equals(url_click))&&url.length>2
        }
      }).persist().repartition(400)

      val showActiveStep1:RDD[(String,String,String,Long)] =filterAndJoin(dataSourceRDD)
      dataSourceRDD.unpersist()
      showActiveStep1.persist()

      //(uid#dateline#url_1#url_2,0)
      val showActiveStep2Tmp =filterAndJoin2(showActiveStep1)
      val showActiveStep2=showActiveStep1.map(row=>
        (s"${row._1}#${row._4}#${row._2}#${row._3}",0))
        .leftOuterJoin(showActiveStep2Tmp).filter(row=>{
          row._2._2 match {
            case Some(row) =>false
            case None => true
          }
        })
      showActiveStep1.unpersist()
      showActiveStep2.persist()




      //***************************************
     // println(showActiveStep2Tmp.count()+"    show_active_increment_step2_temp_temp")
//      for(rdd <- showActiveStep2Tmp.take(10) ){
//        println(s"${rdd}  show_active_increment_step2_temp_temp")
//      }

      //println(showActiveStep2.count()+"   show_active_increment_step2_temp")
//      for(rdd <- showActiveStep2.take(10) ){
//        println(s"${rdd}   show_active_increment_step2_temp")
//      }
      //***************************************

     // println(getDataSet(sc,DateUtil.getXdaysAgo(dt,7),dt).count()+"   7days datas")

    }catch{
      case e :Exception =>{
        e.printStackTrace()
        sc.stop()
        System.exit(-1)
      }
    }finally{
      sc.stop()
    }


  }


  /**
   *从show_active_increment_h5_new取数据并筛选，中间无action算子
   * //数据格式为(uid ,url_click, url, dateline)
   * @param sc
   * @param minDt
   * @param maxDt
   * @return
   */
 def  getDataSet(sc:SparkContext,minDt:String,maxDt:String
                             ):RDD[(String,String,String,String)]={

//    val data1 = Nil
//    var dataRDD: RDD[(String)] = sc.parallelize(data1)
    var dataRDD  =sc.textFile(
      s"hdfs://Ucluster/user/hive/warehouse/bprdb.db/show_active_increment_h5_new/dt=${minDt}")
//    if(minDt < maxDt) {
//      var dt:Long =0
//      for ( dt <- minDt.toLong to  maxDt.toLong) {
//        val dataRDD_tmp = sc.textFile(
//          s"hdfs://Ucluster/user/hive/warehouse/bprdb.db/show_active_increment_h5_new/dt=${dt}")
//        dataRDD = dataRDD.union(dataRDD_tmp)
//
//      }
//    }else if (minDt==maxDt){
//
//    }
    val resultData =dataRDD.map(row =>{
      val splits =row.split("\u0001")
      if(splits.length==4){
        //uid ,url_click url dateline
        (splits(0),splits(1),splits(2),splits(3))
      }else{
        (row,null,null,null)
      }
    })
    resultData

  }

  /**
   *
   * 去除已点击之后再展现
   * @param dataSourceRDD
   * @return
   */
  def filterAndJoin(dataSourceRDD:RDD[(String,String,String,String)]):RDD[(String,String,String,Long)] ={
    val leftOfData:RDD[(String,(String,Long))] =dataSourceRDD.map(row=>{
      val uid =row._1
      val url_click=row._2
      val url =row._3
      val dateline =row._4.toLong
      (s"${uid}#${url}",(url_click,dateline))
    })
    //RDD[(uid#url_click,dateline)]
    val rightOfData:RDD[(String,Long)] =dataSourceRDD.map(row=>{
      val uid =row._1
      val url_click=row._2
      val url =row._3
      val dateline =row._4.toLong
      (s"${uid}#${url_click}",dateline)
    }).reduceByKey(
      (v1,v2) =>{
        if(v1 > v2 ){
          v2
        }else{
          v1
        }
      }
    )

  val resultRDD =leftOfData.leftOuterJoin(rightOfData).filter(row=>{
    val clickShowTime:Long =row._2._2 match {
      case Some(dateline_t2) => dateline_t2
      case None => "4486530534".toLong
    }
    val dateline =row._2._1._2
    if(clickShowTime >= dateline) {
      true
    }else{
      false
    }
  }).map(row=>{
    val splits =row._1.split("#")
    (splits(0),row._2._1._1,splits(1),row._2._1._2)
  })
    resultRDD // 返回(uid,url_click,url,dateline)
  }

  /**
   * 去除同批展现里均被点击的url之间的偏序样本
   * @param showActiveStep1
   * @return
   */
  def filterAndJoin2(showActiveStep1 :RDD[(String,String,String,Long)]):RDD[(String,Int)] ={

    def distinctData(rdd : RDD[(String,String,String,Long)]):RDD[(String,String)] ={
      rdd.map(row => (s"${row._1}#${row._2}#${row._4}",0))
        .reduceByKey(_ + _)
        .map(row =>{
          val splits =row._1.split("#")
          val uid =splits(0)
          val url_click= splits(1)
          val dateLine =splits(2)
          (s"${uid}#${dateLine}",url_click)
        })
    }
    val publicRDD =distinctData(showActiveStep1)
    publicRDD.persist()
    val distinctRDD1:RDD[(String,String)]=publicRDD
    val distinctRDD2:RDD[(String,String)] =publicRDD
    val joinRDD =distinctRDD1.join(distinctRDD2).filter(
      row =>{
        val click_v1 =row._2._1
        val click_v2 =row._2._2
        !click_v1.equals(click_v2)
      }
    ).map(row=>{
      val splits =row._1.split("#")
      val uid =splits(0)
      val dateLine = splits(1)
      val url_1 =row._2._1
      val url_2=row._2._2
      (s"${uid}#${dateLine}#${url_1}#${url_2}",0)
    }).reduceByKey(_ + _)
    joinRDD
  }
}
