package com.hzy.mongo

import com.mongodb.spark.MongoSpark
import com.mongodb.spark._
import org.apache.spark.sql._
import com.mongodb.spark.config._
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

/**
  * Created by hzy on 2018/4/1.
  */
object QueryExercise {
  def main(args: Array[String]): Unit = {
    var url = "mongodb://dmsdbo:dms2017@ods18:27017,ods17:27017,ods16:27017/DMS.Par_ods_CustInf"

    val sparkSession = SparkSession.builder()
      .appName("SparkMongoSS")
      .master("local")
      //      .master("yarn")
      .config("spark.mongodb.input.uri", url)
      .config("spark.mongodb.output.uri", url)
      .config("spark.executor.memory", "512M")
      .config("spark.driver.host", "10.21.83.193")
      //      .config("spark.yarn.jars", "hdfs://nameservice1/spark-libs/spark-core_2.11-2.2.0.jar")
      .config("spark.yarn.archive", "hdfs://nameservice1/spark-libs")
      .getOrCreate()
    val sparkContext = sparkSession.sparkContext

    val rdd = MongoSpark.load(sparkContext)
    val df = MongoSpark.load(sparkSession)
//    RDD (resilientdistributed dataset)
//    指的是一个只读的，可分区的分布式数据集，这个数据集的全部或部分可以缓存在内存中，在多次计算间重用。
//    RDD内部可以有许多分区(partitions)，每个分区又拥有大量的记录(records)
//    rdd
//      withPipeline
//      Seq
//      Map
//    DataFrame是一种以RDD为基础的分布式数据集
//    DataFrame与RDD的主要区别在于，前者带有schema元信息
//    即DataFrame所表示的二维表数据集的每一列都带有名称和类型
//    df
//      filter
//      createOrReplaceTempView
//    sparkSession
//      sql
//    show,count,first

  }
}
