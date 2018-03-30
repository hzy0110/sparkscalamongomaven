package com.hzy.mongo

import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hzy on 2018/3/30.
  */
object mongoconn {
  def main(args: Array[String]): Unit = {

    var url = "mongodb://dmsdbo:dms2017@ods18:27017,ods17:27017,ods16:27017/DMS.ofr_test"

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Mingdao-Score")
      //同时还支持mongo驱动的readPreference配置, 可以只从secondary读取数据
      .set("spark.mongodb.input.uri", url)
      .set("spark.mongodb.output.uri", url)

    val sc = new SparkContext(conf)

    // 创建rdd
    val originRDD = MongoSpark.load(sc)

    println("originRDD.count()="+originRDD.count())
  }

}
