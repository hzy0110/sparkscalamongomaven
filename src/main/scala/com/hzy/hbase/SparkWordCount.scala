package com.hzy.hbase
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._
/**
  * Created by hzy on 2017/1/12.
  */
object SparkWordCount {

  def main(args: Array[String]) {

    ///运行参数：HDFS中的文件，默认前缀是/user/hadoop/
/*    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }*/

    //定义Spark运行时的配置信息
    /*
      Configuration for a Spark application. Used to set various Spark parameters as key-value pairs.
      Most of the time, you would create a SparkConf object with `new SparkConf()`, which will load
      values from any `spark.*` Java system properties set in your application as well. In this case,
      parameters you set directly on the `SparkConf` object take priority over system properties.
     */
    val conf = new SparkConf()
    conf.setAppName("SparkWordCount")
    //conf.setMaster("spark://ods18:7077")
    conf.setMaster("yarn-client")
    //conf.setMaster("spark://192.168.3.185:17077")
    conf.set("spark.executor.memory","128M")
    conf.set("spark.yarn.appMasterEnv.CLASSPATH",
      "$CLASSPATH:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/*")
    conf.setJars(List("hdfs://ods18/zhunian/sparkscalamaven-1.0-SNAPSHOT.jar"))
//    conf.set("spark.ui.port‌​","7077");
    //定义Spark的上下文

    /*
       Main entry point for Spark functionality. A SparkContext represents the connection to a Spark
       cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
       Only one SparkContext may be active per JVM.  You must `stop()` the active SparkContext before
       creating a new one.  This limitation may eventually be removed; see SPARK-2243 for more details.

       @param config a Spark Config object describing the application configuration. Any settings in this config overrides the default configs as well as system properties.
    */

    val sc = new SparkContext(conf)

    ///从HDFS中获取文本(没有实际的进行读取)，构造MappedRDD
    val rdd = sc.textFile("hdfs:/zhunian/zhunian_simple.txt")

    //此行如果报value reduceByKey is not a member of org.apache.spark.rdd.RDD[(String, Int)]，则需要import org.apache.spark.SparkContext._
    rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1)).saveAsTextFile("SortedWordCountRDDInSparkApplication")

    sc.stop
  }

}
