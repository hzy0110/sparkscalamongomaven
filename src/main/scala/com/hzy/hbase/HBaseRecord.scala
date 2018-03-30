
package com.hzy.hbase


import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.CellScannable
import org.apache.spark.sql.execution.datasources.hbase.examples.HBaseRecord



case class HBaseRecord(
                        col0: String,
                        col1: Boolean,
                        col2: Double,
                        col3: Float,
                        col4: Int,
                        col5: Long,
                        col6: Short,
                        col7: String,
                        col8: Byte)

object HBaseRecord {
  def apply(i: Int): HBaseRecord = {
    val s = s"""row${"%03d".format(i)}"""
    HBaseRecord(s,
      i % 2 == 0,
      i.toDouble,
      i.toFloat,
      i,
      i.toLong,
      i.toShort,
      s"String$i extra",
      i.toByte)
  }
}


object HBaseSource {
  val cat = s"""{
               |"table":{"namespace":"default", "name":"tmpLOG"},
               |"rowkey":"key",
               |"columns":{
               |"key":{"cf":"rowkey", "col":"key", "type":"int"},
               |"q1":{"cf":"cf1", "col":"q1", "type":"int"},
               |"q2":{"cf":"cf1", "col":"q2", "type":"int"},
               |"q3":{"cf":"cf1", "col":"q3", "type":"int"},
               |"q4":{"cf":"cf1", "col":"q4", "type":"int"},
               |"q5":{"cf":"cf1", "col":"q5", "type":"string"},
               |"q6":{"cf":"cf1", "col":"q6", "type":"string"},
               |"q7":{"cf":"cf1", "col":"q7", "type":"int"}
               |}
               |}""".stripMargin


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseTest")
//    sparkConf.setMaster("spark://ods18:7077")
                sparkConf.setMaster("yarn-client");
        sparkConf.set("spark.yarn.appMasterEnv.CLASSPATH",
          "$CLASSPATH:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/*")
    sparkConf.set("spark.executor.memory","256M")
    sparkConf.setJars(List(
            "hdfs://ods17/zhunian/sparkscalamaven-1.0-SNAPSHOT.jar"))
//            "hdfs://master/zhunian/json4s-ast_2.10-3.2.10.jar",
//            "hdfs://master/zhunian/json4s-core_2.10-3.2.10.jar",
//            "hdfs://master/zhunian/json4s-jackson_2.10-3.2.10.jar",
//      "hdfs://ods18/zhunian/shc-core-1.0.2-1.6-s_2.10-SNAPSHOT.jar",
//      "hdfs://ods18/zhunian/shc-examples-1.0.2-1.6-s_2.10-SNAPSHOT.jar",
//      "hdfs://ods18/zhunian/hbase-server-1.2.0-cdh5.9.0.jar",
/*      "hdfs://ods18/zhunian/parquet-hadoop-1.5.0-cdh5.9.0.jar",
      "hdfs://ods18/zhunian/spark-catalyst_2.10-1.6.0-cdh5.9.0.jar",*/
//      "hdfs://ods18/zhunian/hbase-client-1.2.0-cdh5.9.0.jar"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->cat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    // for testing connection sharing only
    /*    def withCatalog1(cat: String): DataFrame = {
          sqlContext
            .read
            .options(Map(HBaseTableCatalog.tableCatalog->cat1))
            .format("org.apache.spark.sql.execution.datasources.hbase")
            .load()
        }*/


        val data = (0 to 255).map { i =>
          HBaseRecord(i)
        }

        sc.parallelize(data).toDF.write.options(
          Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "5"))
          .format("org.apache.spark.sql.execution.datasources.hbase")
          .save()

        // for testing connection sharing only
        /*sc.parallelize(data).toDF.write.options(
          Map(HBaseTableCatalog.tableCatalog -> cat1, HBaseTableCatalog.newTable -> "5"))
          .format("org.apache.spark.sql.execution.datasources.hbase")
          .save()*/


    val df = withCatalog(cat)
    df.show
    /*    df.filter($"col0" <= "row005")
          .select($"col0", $"col1").show
        df.filter($"col0" === "row005" || $"col0" <= "row005")
          .select($"col0", $"col1").show
        df.filter($"col0" > "row250")
          .select($"col0", $"col1").show*/

    df.registerTempTable("tmpLOG")
    //    val c = sqlContext.sql("select count(col1) from table1 where col0 < 'row050'")
    val c = sqlContext.sql("insert into table tmpLOG (q1,q1,q2) VALUES('3','a',3)")
    c.show()


    sc.stop()
  }
}




