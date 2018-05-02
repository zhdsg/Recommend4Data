package com.eastday.spark

import java.net.URI

import com.eastday.conf.ConfigurationManager
import com.eastday.constract.Constract
import com.eastday.utils.{ETLUtil, DateUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}



import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import com.eastday.format.RddOutputFormat
/**
 * Created by admin on 2018/4/25.
 */
object Recommend4Data {
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
      .setAppName(Constract.SPARK_APP_NAME_RECOMMEND)
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

    try {
      //一亿  bprsample_show_active_active_realtime_slim
      var showActiveFilterRDD: RDD[(String, Long, String)] = selectDataFromActive(sc, dt)
      //持久化
      showActiveFilterRDD = showActiveFilterRDD.persist()
      //43197370  like http
      val showActiveFilterRDD1st = showActiveFilterRDD.filter(row => {
        val urlFrom = row._3
        if (urlFrom.indexOf("http") == 0) {
          true
        } else {
          false
        }
      }).coalesce(40)
      //79968857    not like http
      val showActiveFilterRDD2nd = showActiveFilterRDD.filter(row => {
        val urlFrom = row._3
        if (urlFrom.indexOf("http") == 0) {
          false
        } else {
          true
        }
      }).coalesce(80)
      //    println(showActiveActiveRDD.count())
      //    println(showActiveFilterRDD1st.count()+"  like 'http' ")
      //    println(showActiveFilterRDD2nd.count()+" unlike 'http'")
      //    for(rdd <- showActiveFilterRDD.take(100)){
      //      println(rdd)
      //    }


      //详情页  bprsample_show_active_detailpage_show_slim
      var showActiveDetailFilterRDD: RDD[(String, (Int, Int, Long))] = selectDataFromDetail(sc, dt)
      showActiveDetailFilterRDD = showActiveDetailFilterRDD.persist()
      //println(showActiveDetailFilterRDD.count() +" bprsample_show_active_detailpage_show_slim ")

      // log.info(s"stop time is ${System.currentTimeMillis()},time_interval is${System.currentTimeMillis()-startTime} ")
      //   show_active_increment_detail_info_today  只去大时间的
      val aggrShowActiveAndDetailRDD: RDD[(String, (Long, (Int, Int, Long)))] = joinFromActiveAndDetail(showActiveFilterRDD1st, showActiveDetailFilterRDD)
      // println(aggrShowActiveAndDetailRDD.count()+"   show_active_increment_detail_info_today")
      val showActiveDetailMapRDD: RDD[(String, (Int, String, Long))] = showActiveDetailFilterRDD.mapPartitions(
        iter => iter.map {
          row => {
            val splits = row._1.split("#")
            val uid = splits(0)
            var url = "null"
            if(splits.length==2){
              url =splits(1)
            }
            val id = row._2 ._1
            val groupID = row._2._2
            val dateLine = row._2._3
            (s"${uid}#${groupID}", (id, url, dateLine))
          }
        }
      )

      val aggrShowActiveAndDetailMapRDD = aggrShowActiveAndDetailRDD.mapPartitions(
        iter => iter.map {
          row => {
            val splits = row._1.split("#")
            val uid = splits(0)
            var url = "null"
            if(splits.length==2){
              url =splits(1)
            }
            val id = row._2._2._1
            val groupID = row._2._2._2
            (s"${uid}#${groupID}", (id, url))
          }
        }
      )
      //    println(aggrShowActiveAndDetailMapRDD.count()+"    1")

      val showActiveAndClickRDD: RDD[(String, ((Int, String, Long), (Int, String)))] = showActiveDetailMapRDD.join(aggrShowActiveAndDetailMapRDD)
      showActiveDetailFilterRDD.unpersist()
      val showActiveAndClickFilterRDD = showActiveAndClickRDD.filter(
        row => {
          val id1 = row._2._1._1
          val id2 = row._2._2._1
          if (id1 != id2 && id1 >= id2 - 6 && id1 <= id2 + 2) {
            true
          } else {
            false
          }
        }
      )
      //
      //    for(rdd <- showActiveAndClickFilterRDD.take(100)){
      //      println("show_active_increment_h5_new_part2  : "+rdd)
      //    }
     // println(showActiveAndClickFilterRDD.count() + "  show_active_increment_h5_new_part1")
      //397156375
      val DataFromDetailH5FilterRDD: RDD[(String, (Int, Int, Int, Long))] = selectDataFromDetailH5(sc, dt)
      //println(DataFromDetailH5FilterRDD.count()+"   H5")
      //259696571
      val DataFromDetailAPPFilterRDD: RDD[(String, (Int, Int, Int, Long))] = selectDataFromDetailAPP(sc, dt)
      //println(DataFromDetailAPPFilterRDD.count()+"   APP")

      val unionAllDataFromDetailRDD: RDD[(String, (Int, Int, Int, Long))] = DataFromDetailH5FilterRDD.union(DataFromDetailAPPFilterRDD)

      unionAllDataFromDetailRDD.persist()
      // println(unionAllDataFromDetailRDD.count()+"  ")
      val joinFor1st: RDD[(String, (Long, (Int, Int, Int, Long)))] = showActiveFilterRDD2nd.map(row => (row._1 + "#" + row._3, row._2))
        .join(unionAllDataFromDetailRDD)
        .filter(
          row => {
            val dateline_t2 = row._2._1
            val dateline_t1 = row._2._2._4
            if (dateline_t2 > dateline_t1) {
              true
            } else {
              false
            }
          }
        ).coalesce(400).reduceByKey(
        (v1, v2) => {
          val dateline_v1 = v1._2._4
          val dateline_v2 = v2._2._4
          if (dateline_v1 >= dateline_v2) {
            v1
          } else {
            v2
          }
        }
      )
      showActiveFilterRDD.unpersist()
      // println(joinFor1st.count()+"   show_active_increment_list_info_today")

      val joinFor1stFilterRDD = joinFor1st.map(
        row => {
          val splits = row._1.split("#")
          val uid = splits(0)
          val url = splits(1)
          var types = "null"
          if(splits.length==3){
             types = splits(2)
          }
          val id = row._2._2._1
          val groupID = row._2._2._2
          val pageNUM = row._2._2._3
          val dateline = row._2._2._4
          (s"${uid}#${groupID}#${types}", (id, url, pageNUM, dateline))
        }
      )
      val joinFor2nd = unionAllDataFromDetailRDD.map(
        row => {
          val splits = row._1.split("#")
          val uid = splits(0)
          val url = splits(1)
          var types = "null"
          if(splits.length==3){
            types = splits(2)
          }
          val id = row._2._1
          val groupID = row._2._2
          val pageNUM = row._2._3
          val dateline = row._2._4
          (s"${uid}#${groupID}#${types}", (id, url, pageNUM, dateline))
        }
      ).join(joinFor1stFilterRDD)
      unionAllDataFromDetailRDD.unpersist()
      val joinFor2ndFilter = joinFor2nd.filter(
        row => {
          val id_t1 = row._2._1._1.toInt
          val id_t2 = row._2._2._1.toInt
          val pageNUM_t1 = row._2._1._3.toInt
          val pageNUM_t2 = row._2._2._3.toInt
          if (pageNUM_t1 > 0 && pageNUM_t2 > 0 || pageNUM_t2 < 0 && pageNUM_t1 == pageNUM_t2) {
            if (id_t1 != id_t2 && id_t1 <= id_t2 + 2 && id_t1 >= id_t2 - 6) {
              true
            } else {
              false
            }
          } else {
            false
          }
        }
      )

//      for( rdd <-joinFor2nd.filter(row=>row._1.split("#")(2).equals("null")).take(100)){
//        println(rdd)
//      }


      val blackListRDD = selectDataFromBlackList(sc)

      val resultRDD = joinFor2ndFilter.map(
        row => {
          (row._1.split("#")(0), (row._2._2._2, row._2._1._2, row._2._1._4))
        }
      ).union(showActiveAndClickFilterRDD.map(
        row => {
          (row._1.split("#")(0), (row._2._2._2, row._2._1._2, row._2._1._3))
        }
      )).leftOuterJoin(blackListRDD).filter(row => {
        row._2._2 match {
          case Some(ime) => false
          case None => true
        }

      })
      val outPath = s"hdfs://Ucluster/user/hive/warehouse/bprdb.db/tmp_show_active_increment_h5_new/dt=${dt}/"
      val fs = FileSystem.get(new Configuration())
      if (fs.exists(new Path(outPath))) {
        val result = fs.delete(new Path(outPath))
        println(result)
      }

      resultRDD.map(row => (s"${row._1}\u0001${row._2._1._1}\u0001${row._2._1._2}\u0001${row._2._1._3}".toString)).coalesce(150)
        .saveAsTextFile(outPath)
    }catch{
      case e :Exception => {
        e.printStackTrace()
        sc.stop()
        System.exit(-1)
      }
    }finally{
      sc.stop()
    }
    //println(resultRDD.count())

//    unionAllDataFromDetailRDD.filter(row=>{
//      row._1.split("#")(0).equals("863116031212418")&&row._2._2==4 &&row._1.split("#")(2).equals("toutiao")
//    })
//      .collect().foreach( println(_))
//
//    joinFor1stFilterRDD.filter(row=>{
//      row._1.equals("863116031212418#4#toutiao")
//    })
//      .collect().foreach( println(_))
   //println(joinFor2ndFilter.count()+"  show_active_increment_h5_new_part2")
//    //   println(showActiveAndClickFilterRDD.count()) //   191625028  数据不对  ，后续检查！！！！
//    joinFor2ndFilter.filter(row=>row._1.equals("863116031212418#4#toutiao"))
//      .collect().foreach( println(_))


    //println(showActiveAndDetailRDD.count())
//    print(showActiveDetailFilterRDD.count())
//
//    for(rdd <- aggrShowActiveAndDetailRDD.take(100)){
//      println(rdd)
//    }

  }

  /**
   * 从bprsample_show_active_active_realtime_slim取数据并筛选，中间无action算子
   * @param sc
   * @param dt
   * @return
   */
  def  selectDataFromActive(sc:SparkContext,dt:String
                             ):RDD[(String,Long,String)]={


    val showActiveActiveRDD  = sc.textFile(
      s"hdfs://Ucluster/user/hive/warehouse/bprdb.db/bprsample_show_active_active_realtime_slim/dt=${dt}")
    val showActiveFilterRDD:RDD[(String,Long,String)]=showActiveActiveRDD.mapPartitions(
      iter=>iter.map {
        line => {
          val fields = line.split("\t")
          val uid = fields(2)
          val  urlFrom =fields(4)
          val urlTo = fields(5)
          val dateLine = fields(0).toLong
          (uid, urlTo, dateLine,urlFrom)
        }
      }
    ).filter(
      row =>{
        val clientIme =row._1
        if(clientIme==null || clientIme.equals("")||clientIme.toUpperCase().equals("NULL")){
          false
        }else{
          true
        }
      }
    ).map(row=>
      (s"${row._1}#${ETLUtil.regexpExtract(row._2,"(.*?)([a-zA-Z]{0,1}[0-9]{15,})(.html[\\\\?]{0,1}.*)",2)}",row._3,row._4)
    )

    showActiveFilterRDD
  }

  /**
   * 从bprsample_show_active_detailpage_show_slim取数据并筛选，中间无action算子
   * @param sc
   * @param dt
   * @return
   */
  def selectDataFromDetail(sc:SparkContext,dt:String
                            ):RDD[(String,(Int,Int,Long))]={
    val  showActiveDetailRDD =sc.textFile(
      s"hdfs://Ucluster/user/hive/warehouse/bprdb.db/bprsample_show_active_detailpage_show_slim/dt=${dt}")
    val showActiveDetailFilterRDD:RDD[(String,(Int,Int,Long))] =showActiveDetailRDD.mapPartitions(
      iter=>iter.map {
        row => {
          val fields = row.split("\t")
          val uid = fields(5)
          val url = fields(3)
          val id = fields(9).toInt
          val groupId = fields(8).toInt
          val dateLine = fields(0).toLong
          (s"${uid}#${url}", (id, groupId, dateLine))
        }
      }
    )
    showActiveDetailFilterRDD
  }

  /**
   * bprsample_show_active_show_realtime_slim，中间无action算子
   * @param sc
   * @param dt
   * @return
   */
  def selectDataFromDetailH5(sc:SparkContext,dt:String
                              ):RDD[(String,(Int,Int,Int,Long))] ={
    val  DataFromDetailH5RDD =sc.textFile(
      s"hdfs://Ucluster/user/hive/warehouse/bprdb.db/bprsample_show_active_show_realtime_slim/dt=${dt}")
    val DataFromDetailH5FilterRDD:RDD[(String,(Int,Int,Int,Long))] =DataFromDetailH5RDD.mapPartitions(
      iter=>iter.map {
        row => {
          val fields = row.split("\t")
          val uid = fields(2)
          val url = fields(7)
          var types =fields(4)
          if(types == null ||types.equals("")){
            types="null"
          }
          val pagenum = fields(5).toInt
          val groupId = fields(8).toInt
          val id =fields(9).toInt

          val dateLine = fields(0).toLong
          (s"${uid}#${url}#${types}", (id, groupId,pagenum, dateLine))
        }
      }
    )
    DataFromDetailH5FilterRDD
  }

  /**
   * bprsample_show_active_appshow_realtime_slim
   * @param sc
   * @return
   */
  def selectDataFromBlackList(sc:SparkContext
                              ):RDD[(String,String)]={
    val  DataFromBlackList =sc.textFile(
      s"hdfs://Ucluster/user/hive/warehouse/chenpei_db.db/accounts_black_list")
    val DataFromBlackListFilterRDD:RDD[(String,String)] =DataFromBlackList.filter(
      row=>{
          val fields = row.split("\t")
        if(fields.length==2){
          true
        }else{
          false
        }
      }
    ).mapPartitions(
      iter=>iter.map {
        row => {
          val fields = row.split("\t")
            val ime =fields(1)
            (ime,ime)

        }
      }
    )
    DataFromBlackListFilterRDD
  }

  def selectDataFromDetailAPP(sc:SparkContext,dt:String
                               ):RDD[(String,(Int,Int,Int,Long))]={
    val  DataFromDetailAPPRDD =sc.textFile(
      s"hdfs://Ucluster/user/hive/warehouse/bprdb.db/bprsample_show_active_appshow_realtime_slim/dt=${dt}")
    val DataFromDetailAPPFilterRDD:RDD[(String,(Int,Int,Int,Long))] =DataFromDetailAPPRDD.mapPartitions(
      iter=>iter.map {
        row => {
          val fields = row.split("\t")
          val uid = fields(4)
          val url = fields(8)
          var types =fields(5)
          if(types == null ||types.equals("")){
            types="null"
          }
          val pagenum = fields(6).toInt
          val groupId = fields(9).toInt
          val id =fields(10).toInt
          val dateLine = fields(0).toLong
          (s"${uid}#${url}#${types}", (id, groupId,pagenum, dateLine))
        }
      }
    )
    DataFromDetailAPPFilterRDD
  }
  /**
   * 筛选  join 后 时间对比
   * @param showActiveFilterRDD1st
   * @param showActiveDetailFilterRDD
   * @return
   */
  def  joinFromActiveAndDetail(showActiveFilterRDD1st:RDD[(String,Long,String)],
                               showActiveDetailFilterRDD:RDD[(String,(Int,Int,Long))]
                                ):RDD[(String,(Long,(Int,Int,Long)))]={

    val showActiveAndDetailRDD: RDD[(String,(Long,(Int,Int,Long)))]=
      showActiveFilterRDD1st.mapPartitions(
        iter =>iter.map{
          row =>{
            (row._1,row._2)
          }
        }
      ).join(showActiveDetailFilterRDD).filter(
        row=>{
          val dateLine1 =row._2._1
          val dateLine2=row._2._2._3
          if(dateLine1>dateLine2){
            true
          }else{
            false
          }
        }
      )
    // log.info(s"stop time is ${System.currentTimeMillis()},time_interval is${System.currentTimeMillis()-startTime} ")
    //   show_active_increment_detail_info_today  只去大时间的
    val aggrShowActiveAndDetailRDD :RDD[(String,(Long,(Int,Int,Long)))]= showActiveAndDetailRDD.reduceByKey(
      (v1,v2)=>{
        val dateline_v1 =v1._2._3
        val dateline_v2=v2._2._3
        if(dateline_v1>=dateline_v2){
          v1
        }else{
          v2
        }
      }
    )
    aggrShowActiveAndDetailRDD
  }


}
