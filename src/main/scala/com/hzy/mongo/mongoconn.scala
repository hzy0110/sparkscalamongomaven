package com.hzy.mongo

import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.{BsonDateTime, BsonDocument, Document}

/**
  * Created by hzy on 2018/3/30.
  */
object mongoconn {
  def main(args: Array[String]): Unit = {

    var url = "mongodb://dmsdbo:dms2017@ods18:27017,ods17:27017,ods16:27017/DMS.Ofr_test"

    val conf = new SparkConf()
            .setMaster("local")
//              .setMaster("spark://ods18:7077").set("spark.ui.port","18080")
//      .setMaster("yarn-client")
      .setAppName("SparkMongo")
      .set("spark.yarn.jar", "hdfs://nameservice1/spark-libs/spark-assembly_2.10-1.6.0-cdh5.9.0.jar")
//      .set("spark.yarn.archive", "hdfs://nameservice1/spark-libs")
      .set("spark.executor.memory", "256M")
      //同时还支持mongo驱动的readPreference配置, 可以只从secondary读取数据
      .set("spark.mongodb.input.uri", url)
      .set("spark.mongodb.output.uri", url)
//      在内外或者 VPN 下设备 IP 获取是错误的，所以需要手动设定
      .set("spark.driver.host", "10.21.83.193")
      .setJars(List(
//        "hdfs://nameservice1/spark-libs/spark-assembly_2.10-1.6.0-cdh5.9.0.jar",
        "hdfs://nameservice1/spark-libs/mongo-spark-connector_2.10-2.2.0.jar"))


    val sc = new SparkContext(conf)

    // 创建rdd
    val originRDD = MongoSpark.load(sc)

    //    val dateQuery = new BsonDocument()
    //    val matchQuery = new Document("$match", BsonDocument.parse("{\"STATE\":\"A\"}"))


    println("originRDD.count()=" + originRDD.count())
    //    println("dateQuery.count()="+matchQuery.size())


  }
}
