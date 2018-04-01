package com.hzy.mongo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hzy on 2018/4/1.
  */
object ActionDemo {

  //  1.reduce(func):通过函数func先聚集各分区的数据集，再聚集分区之间的数据，func接收两个参数，返回一个新值，新值再做为参数继续传递给函数func，直到最后一个元素
  //  2.collect():以数据的形式返回数据集中的所有元素给Driver程序，为防止Driver程序内存溢出，一般要控制返回的数据集大小
  //  3.count()：返回数据集元素个数
  //  4.first():返回数据集的第一个元素
  //  5.take(n):以数组的形式返回数据集上的前n个元素
  //  6.top(n):按默认或者指定的排序规则返回前n个元素，默认按降序输出
  //  7.takeOrdered(n,[ordering]): 按自然顺序或者指定的排序规则返回前n个元素

  def action1_7(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("reduce")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 10, 2)
    val reduceRDD = rdd.reduce(_ + _)
    val reduceRDD1 = rdd.reduce(_ - _)
    //如果分区数据为1结果为 -53
    val countRDD = rdd.count()
    val firstRDD = rdd.first()
    val takeRDD = rdd.take(5)
    //输出前个元素
    val topRDD = rdd.top(3)
    //从高到底输出前三个元素
    val takeOrderedRDD = rdd.takeOrdered(3) //按自然顺序从底到高输出前三个元素

    println("func +: " + reduceRDD)
    println("func -: " + reduceRDD1)
    println("count: " + countRDD)
    println("first: " + firstRDD)
    println("take:")
    takeRDD.foreach(x => print(x + " "))
    println("\ntop:")
    topRDD.foreach(x => print(x + " "))
    println("\ntakeOrdered:")
    takeOrderedRDD.foreach(x => print(x + " "))

    sc.stop
  }

  //  8.countByKey():作用于K-V类型的RDD上，统计每个key的个数，返回(K,K的个数)
  //  9.collectAsMap():作用于K-V类型的RDD上，作用与collect不同的是collectAsMap函数不包含重复的key，对于重复的key。后面的元素覆盖前面的元素
  //  10.lookup(k)：作用于K-V类型的RDD上，返回指定K的所有V值
  def action8_10: Unit = {
    def main(args: Array[String]) {
      val conf = new SparkConf().setMaster("local").setAppName("KVFunc")
      val sc = new SparkContext(conf)
      val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
      val rdd = sc.parallelize(arr, 2)
      val countByKeyRDD = rdd.countByKey()
      val collectAsMapRDD = rdd.collectAsMap()

      println("countByKey:")
      countByKeyRDD.foreach(print)

      println("\ncollectAsMap:")
      collectAsMapRDD.foreach(print)
      sc.stop
    }
  }

  //11  seqOp函数将每个分区的数据聚合成类型为U的值，comOp函数将各分区的U类型数据聚合起来得到类型为U的值
  def aggregate: Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Fold")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1, 2, 3, 4), 2)
    val aggregateRDD = rdd.aggregate(2)(_ + _, _ * _)
    println(aggregateRDD)
    sc.stop
  }

  //12  通过op函数聚合各分区中的元素及合并各分区的元素，op函数需要两个参数
  // 在开始时第一个传入的参数为zeroValue,T为RDD数据集的数据类型，
  //  其作用相当于SeqOp和comOp函数都相同的aggregate函数
  def fold: Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Fold")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array(("a", 1), ("b", 2), ("a", 2), ("c", 5), ("a", 3)), 2)
    val foldRDD = rdd.fold(("d", 0))((val1, val2) => {
      if (val1._2 >= val2._2) val1 else val2
    })
    println(foldRDD)
  }

//  13.saveAsFile(path:String):将最终的结果数据保存到指定的HDFS目录中
//  14.saveAsSequenceFile(path:String):将最终的结果数据以sequence的格式保存到指定的HDFS目录中

}
