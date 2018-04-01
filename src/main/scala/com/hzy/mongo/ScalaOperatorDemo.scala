package com.hzy.mongo

import com.google.gson.Gson
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by hzy on 2018/4/1.
  */
object ScalaOperatorDemo {

  //  Transformaion
  //  将原来RDD的每个数据项通过map中的用户自定义函数f映射转变为一个新的元素
  case class User(id: Int, name: String, pwd: String, sex: Int)

  val conf = new SparkConf().setAppName(ScalaOperatorDemo.getClass.getSimpleName).setMaster("local")
  val sc = new SparkContext(conf)
  val datasInt = Array(1, 2, 3, 7, 4, 5, 8)
  val datasStr = Array("a b c c d e")
  val datasStr1 = Array("x g g h b e")

  //  map
  //  将原来RDD的每个数据项通过map中的用户自定义函数f映射转变为一个新的元素
  def map: Unit = {
    //    val conf = new SparkConf().setAppName(ScalaOperatorDemo.getClass.getSimpleName).setMaster("local")
    //    val sc = new SparkContext(conf)

    val datas: Array[String] = Array(
      "{'id':1,'name':'xl1','pwd':'xl123','sex':2}",
      "{'id':2,'name':'xl2','pwd':'xl123','sex':1}",
      "{'id':3,'name':'xl3','pwd':'xl123','sex':2}")

    sc.parallelize(datas)
      .map(v => {
        new Gson().fromJson(v, classOf[User])
      })
      .foreach(user => {
        println("id: " + user.id
          + " name: " + user.name
          + " pwd: " + user.pwd
          + " sex:" + user.sex)
      })
  }

  //  filter
  //  对元素进行过滤，对每个元素应用f函数，返回值为true的元素在RDD中保留，返回为false的将过滤掉
  def filter: Unit = {
    sc.parallelize(datasInt)
      .filter(v => v >= 3)
      .foreach(println)
  }

  //  flatMap
  //  与map类似，但每个输入的RDD成员可以产生0或多个输出成员
  def flatMap: Unit = {
    sc.parallelize(datasStr)
      .flatMap(line => line.split(","))
      .foreach(println)
  }

  //  mapPartitions
  //  与Map类似，但map中的func作用的是RDD中的每个元素，而mapPartitions中的func作用的对象是RDD的一整个分区。
  //  所以func的类型是Iterator<T> => Iterator<U>，其中T是输入RDD元素的类型。
  //  preservesPartitioning表示是否保留输入函数的partitioner，默认false。
  def mapPartitions: Unit = {
    sc.parallelize(datasStr, 3)
      .mapPartitions(
        n => {
          val result = ArrayBuffer[String]()
          while (n.hasNext) {
            result.append(n.next())
          }
          result.iterator
        }
      )
      .foreach(println)
  }

  //  mapPartitionsWithIndex
  //  与mapPartitions类似，但输入会多提供一个整数表示分区的编号
  //  所以func的类型是(Int, Iterator<T>) => Iterator<R>，多了一个Int
  def mapPartitionsWithIndex: Unit = {
    sc.parallelize(datasInt, 3)
      .mapPartitionsWithIndex(
        (m, n) => {
          val result = ArrayBuffer[String]()
          while (n.hasNext) {
            result.append("分区索引:" + m + "\t" + n.next())
          }
          result.iterator
        }
      )
      .foreach(println)
  }

  //  sample
  //  对RDD进行抽样，其中参数withReplacement为true时表示抽样之后还放回，可以被多次抽样，false表示不放回；
  //  fraction表示抽样比例；seed为随机数种子，比如当前时间戳
  def sample: Unit = {
    sc.parallelize(datasStr)
      .sample(withReplacement = false, 0.5, System.currentTimeMillis)
      .foreach(println)
  }

  //  union
  //  合并两个RDD，不去重，要求两个RDD中的元素类型一致
  def union: Unit = {
    // sc.parallelize(datas1)
    //     .union(sc.parallelize(datas2))
    //     .foreach(println)

    // 或

    (sc.parallelize(datasStr) ++ sc.parallelize(datasStr1))
      .foreach(println)
  }

  //  intersection
  //  返回两个RDD的交集
  def intersection: Unit = {
    sc.parallelize(datasStr)
      .intersection(sc.parallelize(datasStr1))
      .foreach(println)
  }

  //  distinct
  //  对原RDD进行去重操作，返回RDD中没有重复的成员
  def distinct: Unit = {
    sc.parallelize(datasStr)
      .distinct()
      .foreach(println)
  }

  //  groupByKey
  //  对<key, value>结构的RDD进行类似RMDB的group by聚合操作
  //  具有相同key的RDD成员的value会被聚合在一起，返回的RDD的结构是(key, Iterator<value>)
  def groupBy(sc: SparkContext): Unit = {
    sc.parallelize(1 to 9, 3)
      .groupBy(x => {
        if (x % 2 == 0) "偶数"
        else "奇数"
      })
      .collect()
      .foreach(println)

    val datas2 = Array("dog", "tiger", "lion", "cat", "spider", "eagle")
    sc.parallelize(datas2)
      .keyBy(_.length)
      .groupByKey()
      .collect()
      .foreach(println)
  }

  //  reduceByKey
  //  对<key, value>结构的RDD进行聚合，对具有相同key的value调用func来进行reduce操作
  //  func的类型必须是(V, V) => V
  def reduceByKey: Unit = {
    val textFile = sc.textFile("file:///home/zkpk/spark-2.0.1/README.md")
    val words = textFile.flatMap(line => line.split(" "))
    val wordPairs = words.map(word => (word, 1))
    val wordCounts = wordPairs.reduceByKey((a, b) => a + b)
    println("wordCounts: ")
    wordCounts.collect().foreach(println)
  }

  //  aggregateByKey
  //  aggregateByKey函数对PairRDD中相同Key的值进行聚合操作，在聚合过程中同样使用了一个中立的初始值。
  // 和aggregate函数类似，aggregateByKey返回值得类型不需要和RDD中value的类型一致。
  // 因为aggregateByKey是对相同Key中的值进行聚合操作，所以aggregateByKey函数最终返回的类型还是Pair RDD，对应的结果是Key和聚合好的值
  // 而aggregate函数直接返回非RDD的结果。
  //  zeroValue：表示在每个分区中第一次拿到key值时,用于创建一个返回类型的函数,这个函数最终会被包装成先生成一个返回类型,
  // 然后通过调用seqOp函数,把第一个key对应的value添加到这个类型U的变量中。
  //  seqOp：这个用于把迭代分区中key对应的值添加到zeroValue创建的U类型实例中。
  //  combOp：这个用于合并每个分区中聚合过来的两个U类型的值。
  def aggregateByKey(sc: SparkContext): Unit = {

    // 合并在同一个partition中的值，a的数据类型为zeroValue的数据类型，b的数据类型为原value的数据类型
    def seq(a: Int, b: Int): Int = {
      println("seq: " + a + "\t" + b)
      math.max(a, b)
    }

    // 合并在不同partition中的值，a,b的数据类型为zeroValue的数据类型
    def comb(a: Int, b: Int): Int = {
      println("comb: " + a + "\t" + b)
      a + b
    }

    // 数据拆分成两个分区
    // 分区一数据: (1,3) (1,2)
    // 分区二数据: (1,4) (2,3)
    // zeroValue 中立值，定义返回value的类型，并参与运算
    // seqOp 用来在一个partition中合并值的
    // 分区一相同key的数据进行合并
    // seq: 0   3  (1,3)开始和中位值合并为3
    // seq: 3   2  (1,2)再次合并为3
    // 分区二相同key的数据进行合并
    // seq: 0   4  (1,4)开始和中位值合并为4
    // seq: 0   3  (2,3)开始和中位值合并为3
    // comb 用来在不同partition中合并值的
    // 将两个分区的结果进行合并
    // key为1的, 两个分区都有, 合并为(1,7)
    // key为2的, 只有一个分区有, 不需要合并(2,3)
    sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3)), 2)
      .aggregateByKey(0)(seq, comb)
      .collect()
      .foreach(println)

    // 结果
    //    (2,3)
    //    (1,7)
  }


  //  sortByKey
  //  对<key, value>结构的RDD进行升序或降序排列
  //  comp：排序时的比较运算方式。
  //  ascending：false降序；true升序。
  def sortByKey(sc: SparkContext): Unit = {
    sc.parallelize(Array(60, 70, 80, 55, 45, 75))
      .sortBy(v => v, false)
      .foreach(println)

    sc.parallelize(List((3, 3), (2, 2), (1, 4), (2, 3)))
      .sortByKey(true)
      .foreach(println)
  }

  //  join
  //  对<K, V>和<K, W>进行join操作，返回(K, (V, W))外连接函数为leftOuterJoin、rightOuterJoin和fullOuterJoin
  def join: Unit = {
    sc.parallelize(List((1, "苹果"), (2, "梨"), (3, "香蕉"), (4, "石榴")))
      .join(sc.parallelize(List((1, 7), (2, 3), (3, 8), (4, 3), (5, 9))))
      .foreach(println)
  }

  //join、leftOuterJoin、rightOuterJoin和fullOuterJoin
  def join1: Unit = {
    val pairRDD1 = sc.parallelize(List(("cat", 2), ("cat", 5), ("book", 4), ("cat", 12)))
    val pairRDD2 = sc.parallelize(List(("cat", 2), ("cup", 5), ("mouse", 4), ("cat", 12)))
    pairRDD1.leftOuterJoin(pairRDD2).collect
    pairRDD1.rightOuterJoin(pairRDD2).collect
    pairRDD1.fullOuterJoin(pairRDD2).collect
  }

  //  cogroup
  //  cogroup:对多个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合
  // 与reduceByKey不同的是针对两个RDD中相同的key的元素进行合并。
  def cogroup(sc: SparkContext): Unit = {
    val datas1 = List((1, "苹果"),
      (2, "梨"),
      (3, "香蕉"),
      (4, "石榴"))

    val datas2 = List((1, 7),
      (2, 3),
      (3, 8),
      (4, 3))
    val datas3 = List((1, "7"),
      (2, "3"),
      (3, "8"),
      (4, "3"),
      (4, "4"),
      (4, "5"),
      (4, "6"))
    sc.parallelize(datas1)
      .cogroup(sc.parallelize(datas2),
        sc.parallelize(datas3))
      .foreach(println)

    // 结果
    //  (4,(CompactBuffer(石榴),CompactBuffer(3),CompactBuffer(3, 4, 5, 6)))
    //  (1,(CompactBuffer(苹果),CompactBuffer(7),CompactBuffer(7)))
    //  (3,(CompactBuffer(香蕉),CompactBuffer(8),CompactBuffer(8)))
    //  (2,(CompactBuffer(梨),CompactBuffer(3),CompactBuffer(3)))
  }

  //  cartesian
  //  两个RDD进行笛卡尔积合并
  def cartesian: Unit = {
    val rdd1 = sc.makeRDD(List(1, 2, 3))
    val rdd2 = sc.makeRDD(List(4, 5, 6, 7))
    rdd1.cartesian(rdd2)
      .foreach(println)

    val result = rdd1.cartesian(rdd2)
    result.collect
  }

  //  pipe
  //  执行cmd命令，创建RDD
  def pipe(sc: SparkContext): Unit = {
    val data = List("hi", "hello", "how", "are", "you")
    sc.makeRDD(data)
      .pipe("/Users/zhangws/echo.sh")
      .collect()
      .foreach(println)
  }

  //  coalesce
  //  用于将RDD进行重分区，使用HashPartitioner。且该RDD的分区个数等于numPartitions个数
  //  如果shuffle设置为true，则会进行shuffle。
  def coalesce(sc: SparkContext): Unit = {
    val datas = List("hi", "hello", "how", "are", "you")
    val datasRDD = sc.parallelize(datas, 4)
    println("RDD的分区数: " + datasRDD.partitions.length)
    val datasRDD2 = datasRDD.coalesce(2)
    println("RDD的分区数: " + datasRDD2.partitions.length)
  }

  //  repartition
  //  该函数其实就是coalesce函数第二个参数为true的实现
  def repartition: Unit = {
    val rdd1 = sc.makeRDD(List(1, 2, 3))
    var rdd2 = rdd1.repartition(1)
    println(rdd2.partitions.size)
  }

  //  repartitionAndSortWithinPartitions
  //  根据给定的Partitioner重新分区，并且每个分区内根据comp实现排序。
  def repartitionAndSortWithinPartitions(sc: SparkContext): Unit = {

    def partitionFunc(key: String): Int = {
      key.substring(7).toInt
    }

    val datas = new Array[String](1000)
    val random = new Random(1)
    for (i <- 0 until 10; j <- 0 until 100) {
      val index: Int = i * 100 + j
      datas(index) = "product" + random.nextInt(10) + ",url" + random.nextInt(100)
    }
    val datasRDD = sc.parallelize(datas)
    val pairRDD = datasRDD.map(line => (line, 1))
      .reduceByKey((a, b) => a + b)
    //        .foreach(println)

    pairRDD.repartitionAndSortWithinPartitions(new Partitioner() {
      override def numPartitions: Int = 10

      override def getPartition(key: Any): Int = {
        val str = String.valueOf(key)
        str.substring(7, str.indexOf(',')).toInt
      }
    }).foreach(println)
  }

  //  Action
  //  reduce
  //  对RDD成员使用func进行reduce操作，func接受两个参数，合并之后只返回一个值
  //  reduce操作的返回结果只有一个值。需要注意的是，func会并发执行
  def reduce(sc: SparkContext): Unit = {
    println(sc.parallelize(1 to 10)
      .reduce((x, y) => x + y))
    // 结果
    //  55
  }

  //  collect
  //  将RDD读取至Driver程序，类型是Array，一般要求RDD不要太大。
  def collect: Unit = {
    sc.parallelize(datasStr)
      .distinct().collect()
  }

  //  count
  //  返回RDD的成员数量
  def count(sc: SparkContext): Unit = {
    println(sc.parallelize(1 to 10)
      .count)
  }

  //  first
  //  返回RDD的第一个成员，等价于take(1)
  def first(sc: SparkContext): Unit = {
    println(sc.parallelize(1 to 10)
      .first())
  }

  //  take
  //  返回RDD前n个成员
  def take(sc: SparkContext): Unit = {
    sc.parallelize(1 to 10)
      .take(2).foreach(println)
  }

  //  takeSample
  //  和sample用法相同，只不第二个参数换成了个数。返回也不是RDD，而是collect。
  def takeSample(sc: SparkContext): Unit = {
    sc.parallelize(1 to 10)
      .takeSample(withReplacement = false, 3, 1)
      .foreach(println)
    // 结果
    //  1
    //  8
    //  10
  }

  //  takeOrdered
  //  用于从RDD中，按照默认（升序）或指定排序规则，返回前num个元素。
  def takeOrdered(sc: SparkContext): Unit = {
    sc.parallelize(Array(5, 6, 2, 1, 7, 8))
      .takeOrdered(3)(new Ordering[Int]() {
        override def compare(x: Int, y: Int): Int = y.compareTo(x)
      })
      .foreach(println)

    // 结果
    //  8
    //  7
    //  6
  }

  //  saveAsTextFile
  //  将RDD转换为文本内容并保存至路径path下，可能有多个文件(和partition数有关)
  //  路径path可以是本地路径或HDFS地址，转换方法是对RDD成员调用toString函数
  def saveAsTextFile(sc: SparkContext): Unit = {
    sc.parallelize(Array(5, 6, 2, 1, 7, 8))
      .saveAsTextFile("/Users/zhangws/Documents/test")
  }

  //  saveAsSequenceFile
  //  与saveAsTextFile类似，但以SequenceFile格式保存，成员类型必须实现Writeable接口或可以被隐式转换为Writable类型（比如基本Scala类型Int、String等）

  //  saveAsObjectFile
  //  用于将RDD中的元素序列化成对象，存储到文件中。对于HDFS，默认采用SequenceFile保存。

  //  countByKey
  //  仅适用于(K, V)类型，对key计数，返回(K, Int)
  def countByKey(sc: SparkContext): Unit = {
    println(sc.parallelize(Array(("A", 1), ("B", 6), ("A", 2), ("C", 1), ("A", 7), ("A", 8)))
      .countByKey())

    // 结果
    //  Map(B -> 1, A -> 4, C -> 1)
  }

  //  countByValue
  //  统计一个RDD中各个value的出现次数。返回一个map，map的key是元素的值，value是出现的次数。
  def countByValue: Unit = {
    val b = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 2, 4, 2, 1, 1, 1, 1, 1))
    b.countByValue
  }

  //  combineByKey用于将一个PairRDD按key进行聚合操作
  def combineByKey: Unit = {
    var a = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
    var b = sc.parallelize(List(1, 1, 2, 2, 2, 1, 2, 2, 2), 3)
    var c = b.zip(a)

    def myfunc(index: Int, iter: Iterator[(Int, String)]): Iterator[(Int, String)] = {
      println("[partID:" + index + ", val: " + iter.toList + "]")
      iter
    }

    c.mapPartitionsWithIndex(myfunc).collect

    // 同一个Partition内第一次碰到一个key的处理函数
    def myfunc1(v: String): List[String] = {
      println("myfunc1 value: " + v)
      List(v)
    }

    // 同一个Partition内不是第一次碰到一个key的处理函数
    def myfunc2(l: List[String], v: String): List[String] = {
      println("myfunc2 list: " + l + " value: " + v)
      v :: l
    }

    // 不同Partition相同key的聚合函数
    def myfunc3(l1: List[String], l2: List[String]): List[String] = {
      println("myfunc3 list1: " + l1 + " list2: " + l2)
      l1 ::: l2
    }

    var d = c.combineByKey(myfunc1, myfunc2, myfunc3)
    d.collect
  }


  //  foreach
  //  对RDD中的每个成员执行func，没有返回值，常用于更新计数器或输出数据至外部存储系统。
  //  这里需要注意变量的作用域
  def foreach: Unit = {
    sc.parallelize(1 to 10).foreach(println)
  }

  //  fold
  //  fold是aggregate的简化，将aggregate中的seqOp和combOp使用同一个函数。
  def fold: Unit = {
    val a = sc.parallelize(1 to 6, 3)
    a.fold(4)(_ + _)
//     Int = 37
  }


  //  foldByKey
  //  与fold类似，但是是基于相同的key进行计算。并且初始化只运用于Partition内的reduce操作。
  def foldByKey: Unit = {
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val b = a.map(x => (x.length, x))
    b.foldByKey("=")(_ + _).collect
//    Array[(Int, String)] = Array((4,=lion), (3,=dog=cat), (7,=panther), (5,=tiger=eagle))
  }

  //  foldByKey
  //  foldByKey函数是通过调用CombineByKey函数实现的
  //zeroVale：对V进行初始化，实际上是通过CombineByKey的createCombiner实现的  V =>  (zeroValue,V)，再通过func函数映射成新的值，即func(zeroValue,V),如例4可看作对每个V先进行  V=> 2 + V
  //  func: Value将通过func函数按Key值进行合并（实际上是通过CombineByKey的mergeValue，mergeCombiners函数实现的，只不过在这里，这两个函数是相同的）
  def foldByKey1: Unit ={
    val people = List(("Mobin", 2), ("Mobin", 1), ("Lucy", 2), ("Amy", 1), ("Lucy", 3))
    val rdd = sc.parallelize(people)
    val foldByKeyRDD = rdd.foldByKey(2)(_+_)
    foldByKeyRDD.foreach(println)
  }

//  mapValues
//  对key-value形式的RDD中的value进行映射。
  def mapValues: Unit ={
  val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
  val b = a.map(x => (x.length, x))
  b.mapValues("x" + _ + "x").collect
//  res19: Array[(Int, String)] = Array((3,xdogx), (5,xtigerx), (4,xlionx), (3,xcatx), (7,xpantherx), (5,xeaglex))

}


//  flatMapValues
//  与mapValues类似，但可以将一个value展开成多个value。
  def flatMapValues: Unit ={
  val a = sc.parallelize(List(("fruit", "apple,banana,pear"), ("animal", "pig,cat,dog,tiger")))
  a.flatMapValues(_.split(",")).collect
//  Array[(String, String)] = Array((fruit,apple), (fruit,banana), (fruit,pear),
//    (animal,pig), (animal,cat), (animal,dog), (animal,tiger))
}

//  comineByKey
//  在第一次遇到Key时创建组合器函数，将RDD数据集中的V类型值转换C类型值（V => C
//  注意前三个函数的参数类型要对应；第一次遇到Key时调用createCombiner，再次遇到相同的Key时调用mergeValue合并值
  def comineByKey: Unit ={
  val conf = new SparkConf().setMaster("local").setAppName("combinByKey")
  val sc = new SparkContext(conf)
  val people = List(("male", "Mobin"), ("male", "Kpop"), ("female", "Lucy"), ("male", "Lufei"), ("female", "Amy"))
  val rdd = sc.parallelize(people)
  val combinByKeyRDD = rdd.combineByKey(
    (x: String) => (List(x), 1),
    (peo: (List[String], Int), x : String) => (x :: peo._1, peo._2 + 1),
    (sex1: (List[String], Int), sex2: (List[String], Int)) => (sex1._1 ::: sex2._1, sex1._2 + sex2._2))
  combinByKeyRDD.foreach(println)
  sc.stop()
}







//  dependencies
//  返回本RDD依赖的RDD。
  def dependencies: Unit ={
  var a = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
  var b = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
  b.dependencies.length
//  Int = 0

  b.map(a => a).dependencies.length
//  Int = 1

  b.map(_ + 2).dependencies.length
//  Int = 1

  b.cartesian(a).dependencies.length
  b.cartesian(a).dependencies
}

//  checkpoint
//  checkpoint用于给RDD设置一个checkpoint。有了checkpoint，一定后续的计算出错，不需要从头开始重新计算，只需要从checkpoint点开始计算。
//  checkpoint需要将数据保存在磁盘上。保存前，需要确保目录已经存在。如果是保存在本地目录中，需要注意各个work节点上都有相关的目录。所以最好的方式是保存在HDFS上。
//  数据保存形式为二进制。
//  setCheckpointDir、getCheckpointFile和isCheckpointed
//  setCheckpointDir：用于设置checkpoint文件的路径。
//  getCheckpointFile：获取checkpoint文件的路径。如果没有做过checkpoint，则为空。
//  isCheckpointed：判断是否做过checkpoint。
  def checkpoint: Unit ={
  sc.setCheckpointDir("my_directory_name")
  val a = sc.parallelize(1 to 4)
  a.checkpoint
  a.count
}





//  glom——返回分区情况
//  将RDD的每个分区中的类型为T的元素转换换数组Array[T]
  def glom: Unit ={
  val rdd = sc.parallelize(1 to 16,4)
  val glomRDD = rdd.glom() //RDD[Array[T]]
  glomRDD.foreach(rdd => println(rdd.getClass.getSimpleName))
}

//  randomSplit
//  根据weight权重值将一个RDD划分成多个RDD,权重越高划分得到的元素较多的几率就越大
  def randomSplit{
    val rdd = sc.parallelize(1 to 10)
    val randomSplitRDD = rdd.randomSplit(Array(1.0,2.0,7.0))
    randomSplitRDD(0).foreach(x => print(x +" "))
    randomSplitRDD(1).foreach(x => print(x +" "))
    randomSplitRDD(2).foreach(x => print(x +" "))
  }

}


