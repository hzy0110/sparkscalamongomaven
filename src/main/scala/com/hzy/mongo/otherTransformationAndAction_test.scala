package com.hzy.mongo
import org.apache.spark.{SparkContext, SparkConf}
/**
  * distinct：去重
  * union：合并
  * foreachPartition：遍历
  * jion：内连接
  * leftOuterJion：左外连接
  * rightouterJion：右外连接
  * fullouterJion：全外连接
  */

object otherTransformationAndAction_test {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TransformationAndAction_test").setMaster("local")
    val sc = new SparkContext(conf)
    //=======================distinct========================================//
    //去重distinct:源码就是：map+reduceByKey+map
    //SparkContext.parallelize(Array,partitonNum=1)：第二个参数默认是1，可以不用设置
    val repeatRDD = sc.parallelize(Array("java", "scala", "js", "java", "html", "js"))
    val distinctRDD = repeatRDD.map((_, 1)).reduceByKey(_ + _).map(_._1)
    distinctRDD.foreach(println)
    repeatRDD.distinct().foreach(println)
    //=======================foreachPartiton====================================//
    //foreachPartiton和foreach（）数据库连接对比案例代码
    distinctRDD.foreach(x => {
      println("创建数据库连接")
      println("拼接sql")
      println("执行sql")
    })
    // foreachPartition（Iterator）实现：传入的参数是迭代器，将一个Partition的数据加载到内存中，然后再遍历
    // foreach（）遍历的基本单位是每一条记录，foreachPartition（）遍历的单位是partition,类似的////还有map（）和mapPartition（）
    distinctRDD.foreachPartition(x => {
      println("创建数据库连接")
      while (x.hasNext) {
        println("拼接sql" + x.next())
      }
      println("执行sql")
    })

    //===========================jion=======================================//
    //jion:内连接
    val scoreRDD = sc.parallelize(List(
      (1, 100),
      (2, 90),
      (3, 84),
      (4, 69)
    ), 2)
    //本地集合获取得到RDD：
    val subjectRDD = sc.makeRDD(Array(
      // (1,"math"),
      (2, "english"),
      (3, "chinese"),
      (4, "physics")
    ), 3)
    //测试jion后的分区数：jionedRDD的分区是由父RDD中最多的分区数来决定的
    println("Jion后的partition=" + subjectRDD.partitions.length)
    //jion左边的为主表:主表的数据做为标准连接
    val jionRDD = subjectRDD.join(scoreRDD)
    jionRDD.foreach(
      x => {
        val id = x._1
        val subject = x._2._1
        val score = x._2._2
        println("id:" + id + " subject:" + subject + " score:" + score)
      }
    )
    //leftOuterJoin左边的为主表:主表的数据做为标准连接,没连接上的为none
    //rightOuterJoin右边的为主表:主表的数据做为标准连接，没连接上的为none
    //fullouterJion全部连接，没连接上的为none
    val leftOuterJoinRDD = subjectRDD.leftOuterJoin(scoreRDD)

    leftOuterJoinRDD.foreach(
      x => {
        val id = x._1
        val subject = x._2._1
        val score = x._2._2
        println("id:" + id + " subject:" + subject + " score:" + score)
      }
    )
    //===========================union=======================================//
    //union：union后的分区数是union RDD的累加和
    val unionRDD1 = sc.parallelize(1 to 10, 1)
    val unionRDD2 = sc.parallelize(1 until 20, 2)
    val unionRDD = unionRDD1.union(unionRDD2)
    println("union后的分区数=" + unionRDD.partitions.length)
    unionRDD.foreach(println)
  }
}
