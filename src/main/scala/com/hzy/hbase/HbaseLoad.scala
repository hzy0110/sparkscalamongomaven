package com.hzy.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkContext, SparkConf}
/**
 * @author ${user.name}
 */
object HbaseLoad {

  def main(args:Array[String]) {
    //Spark环境初始化
    val conf = new SparkConf().setAppName("ReadFromHBase")//.set("spark.cores.max","2").setMaster("spark://10.10.13.178:18080")
    //Standalone 模式
//    conf.setMaster("spark://ods18:7077")//.set("spark.ui.port‌​","7077");
    conf.setMaster("yarn-client")//.set("spark.ui.port‌​","7077");
    conf.set("spark.executor.memory","256M")
    conf.set("spark.yarn.appMasterEnv.CLASSPATH",
      "$CLASSPATH:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/*")
    conf.setJars(List("hdfs://ods17/zhunian/sparkscalamaven-1.0-SNAPSHOT.jar"))

    val sparkContext = new SparkContext(conf)

    LogManager.getRootLogger.setLevel(Level.WARN)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)



    //通过zookeeper获取HBase连接
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "ods18")

    //hbaseConf.addResource(new org.apache.hadoop.fs.Path(s"file://$hbaseConf")) //读取hbase配置文件


    //设置读取表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "serv_msg")
    //设置读取列组
    hbaseConf.set(TableInputFormat.SCAN_COLUMNS, "cf1")
    //应用newAPIHadoopRDD读取HBase，返回NewHadoopRDD
    val hbaseRDD = sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])


    //读取结果集RDD，返回一个MapPartitionsRDD
    val resRDD = hbaseRDD.map(tuple => tuple._2)

    println("resRDD.count()"+resRDD.count())

    //打印读取数据内容
    resRDD.map(r => (Bytes.toString(r.getRow),
      //Bytes.toString(r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("serv_id"))),
      //Bytes.toString(r.getValue(Bytes.toBytes("daily"), Bytes.toBytes("v2"))),
      Bytes.toString(r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("serv_id"))))).foreach(println(_))


    println("------------")
  }
}
