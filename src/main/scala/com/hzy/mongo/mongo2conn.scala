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
object mongo2conn {
  def main(args: Array[String]): Unit = {
    var url = "mongodb://dmsdbo:dms2017@ods18:27017,ods17:27017,ods16:27017/DMS.Par_ods_CustInf"


    //  local,yarn 可用
    //    System.setProperty("SPARK_YARN_MODE", "yarn");
    val ss = SparkSession.builder()
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

    val df = MongoSpark.load(ss)

//  简单查询
//    println("rdd.count=" + df.count())
//    println("rdd.first=" + df.first())
//  打印表结构字段
//    df.printSchema()
//  查询
    df.filter(df("AreaId") < 32000).show()


    val sc = ss.sparkContext
//    sc.loadFromMongoDB()
    val rdd = MongoSpark.load(sc)

    val time1=System.currentTimeMillis()

//    字段不存在会报错，使用withPipeline 可以避免
//    val filteredRdd = rdd.filter(doc => doc.getLong("AreaId") > 5)
//    println(filteredRdd.count)
//    println(filteredRdd.first.toJson)

    val time2=System.currentTimeMillis()
    println("timediff1="+(time2-time1))

    // aggregation 对空值可以处理，filter会报错
    val aggregatedRdd = rdd.withPipeline(Seq(Document.parse("{ $match: { AreaId : { $gt : 5 } } }")))
    println(aggregatedRdd.count)
    println(aggregatedRdd.first.toJson)

    val time3=System.currentTimeMillis()
    println("timediff2="+(time3-time2))

//    设置读取参数collection是集合名称，readPreference是读取主从的配置
    val readConfig = ReadConfig(Map("collection" -> "Par_ods_CustInf", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    val customRdd = MongoSpark.load(sc, readConfig)
    println("customRdd.count=" + customRdd.count)
//    println(customRdd.first.toJson)

//    存储模型
//    SaveMode.ErrorIfExists 在存储DataFrame到数据源时，如果数据已经存在了则抛出异常
//
//    SaveMode.Append
//    SaveMode.Overwrite
//    SaveMode.Ignore
//    val saveDoc = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
//    MongoSpark.save(saveDoc)


//    显式指定schema,使用 SparkSession
    val explicitDF = MongoSpark.load[mongo2conn.Character](ss)
    explicitDF.printSchema()
    //必须注册为临时表才能供下面查询使用
    explicitDF.createOrReplaceTempView("CustTmpSql")

    val custTmpSqlDF = ss.sql("SELECT custName, areaId FROM CustTmpSql WHERE AreaId <= 32000")
    custTmpSqlDF.show()


//    使用case class将DataFrame转化为Dataset
//    val dataset = explicitDF.as[mongo2conn.Character]

//    RDD也可以转化为DataFrame和Dataset
    val dfInferredSchema = rdd.toDF()
    val dfExplicitSchema = rdd.toDF[Character]()
    val ds = rdd.toDS[Character]()

    //    rdd.filter(rdd("AreaId") < 40000).show()

//    val documents = sparkConn.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
//    MongoSpark.save(documents)



//
//    val centenarians = sparkConn.sql("SELECT AreaId FROM DMS.Par_ods_CustInf WHERE AreaId >= 100")
//    centenarians.show()

  }

//  字段名称要一致，大小写敏感
  case class Character(CustName: String, AreaId: Long)
}
