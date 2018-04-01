package com.hzy.mongo


import org.apache.spark.{SparkContext, SparkConf}
//思考：假如有1亿条数据，如何动态的统计出出现次数最多的编程语言，然后过滤掉
// 思路：抽样-wordcount统计-调换（K，V）-排序取Top1-filter过滤即可。
/**
  * 使用到的算子：
  * sample
  * flatmap
  * map
  * reduceByKey
  * sortByKey
  * take(n)-----Action算子
  * first()----源码中即take(1)
  * filter
  */
object Sample_test {
  def main(args: Array[String]) {
    val conf= new SparkConf().setAppName("sample").setMaster("local")
    val context =new SparkContext(conf)
    val textRDD= context.textFile("words.txt")
    //抽样
    val sampleRDD=textRDD.sample(false,0.9)
    //拿到编程语言---（语言，1）---根据key求和---转换（语言，出现次数）--（次数，语言）---排序---取Top1---取Top1对应的编程语言
    val WordRDD= sampleRDD.map(x=>{(x.split(" ")(1),1)}).reduceByKey(_+_)
    //first=take(1)----Action类的算子，返回一个非RDD的值
    val code= WordRDD.map(x=>{(x._2,x._1)}).sortByKey(false).first()._2
    //过滤
    textRDD.filter(x=>{!x.contains(code)}).foreach(println)
    context.stop()
  }
}
