package com.hzy.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by hzy on 2017/3/13.
  */
object HbaseCRUD {
//  val conf = HBaseConfiguration.create()
//  conf.set("hbase.zookeeper.property.clientPort", "2181")
//  conf.set("hbase.zookeeper.quorum", "ods18")
//
//  //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
//  val conn = ConnectionFactory.createConnection(conf)
//
//  def convert(triple: (Int, String, Int)) = {
//    val p = new Put(Bytes.toBytes(triple._1))
//    p.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("name"),Bytes.toBytes(triple._2))
//    p.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("age"),Bytes.toBytes(triple._3))
//    (new ImmutableBytesWritable, p)
//  }
}
