package com.eastday.spark



import com.eastday.conf.ConfigurationManager
import com.eastday.constract.Constract
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by admin on 2018/4/20.
 */
object DEMO {
  def main (args: Array[String]) {
    // val logger :Logger = Logger.getLogger(UvAndIP2AnaylzeSpark2.getClass)


    //spark 具体句柄创建
    val conf: SparkConf = new SparkConf()
      .setAppName(Constract.SPARK_APP_NAME)
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
    sc.hadoopConfiguration.set(Constract.DFS_CLIENT_SOCKET_TIMEOUT
      ,ConfigurationManager.getString(Constract.DFS_CLIENT_SOCKET_TIMEOUT))
    // sc.setCheckpointDir("/user/zhanghao/data/checkpoint")
    //    sc.setCheckpointDir("E:\\tool\\checkpoint")
//    val spark: SparkSession = SparkSession.builder()
//      .config(conf)
//      .enableHiveSupport()
//      .getOrCreate()
//    import spark.implicits._
//     .set("spark.shuffle.manager","hash")
    //.set("spark.shuffle.sort.bypassMergeThreshold","550")
    //.set("spark.shuffle.memoryFraction","0.2")
    //.set("spark.storage.memoryFraction","0.5")

    //      .setMaster("local")
    //      .set("spark.testing.memory","471859200")
    //      .set("spark.sql.warehouse.dir","file:////F://workspace//UV2DongFang")
    //      .set("spark.driver.memory","5g")


    //val dateTime =new Date().getTime
    try {
      val datas =sc.textFile("hdfs://Ucluster/user/hive/warehouse/bprdb.db/bprsample_show_active_active_realtime_slim/dt=varenddate")
      for(data <- datas.take(10)){
        println(data)
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        sc.stop()
        //spark.stop()
        System.exit(-1)

      }
    } finally {
      sc.stop()
      //spark.stop()
    }
  }
}
