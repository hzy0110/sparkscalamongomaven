package com.hzy.mongo

import org.apache.spark.sql.SparkSession
import com.mongodb.spark.MongoSpark

/**
  * Created by hzy on 2018/4/1.
  */
object mongo2conn {
  def main(args: Array[String]): Unit = {
    var url = "mongodb://dmsdbo:dms2017@ods18:27017,ods17:27017,ods16:27017/DMS.Ofr_test"


    //  local,yarn 可用
    //    System.setProperty("SPARK_YARN_MODE", "yarn");
    val sparkConn = SparkSession.builder()
      .master("yarn")
//      .config("deploy-mode", "client")
      .config("spark.mongodb.input.uri", url)
      .config("spark.mongodb.output.uri", url)
      .config("spark.executor.memory", "512M")
      .config("spark.driver.host", "10.21.83.193")
//      .config("spark.yarn.jars", "hdfs://nameservice1/spark-libs/spark-core_2.11-2.2.0.jar")
      .config("spark.yarn.archive", "hdfs://nameservice1/spark-libs")
      .getOrCreate()

    val rdd = MongoSpark.load(sparkConn)

    println("rdd.count=" + rdd.count())


  }
}
