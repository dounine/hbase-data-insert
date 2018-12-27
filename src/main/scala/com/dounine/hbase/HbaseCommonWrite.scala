package com.dounine.hbase

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HbaseCommonWrite {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      //      .setMaster("local[12]")
      .setAppName("HbaseCommonWrite")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()
    val sc = spark.sparkContext

    val datas = List(
      ("abc", ("ext", "type", "login")),
      ("ccc", ("ext", "type", "logout"))
    )
    val dataRdd = sc.parallelize(datas)

    val output = dataRdd.map {
      x => {
        val rowKey = Bytes.toBytes(x._1)
        val colFam = x._2._1
        val colName = x._2._2
        val colValue = x._2._3

        val put = new Put(rowKey)
        put.addColumn(Bytes.toBytes(colFam),
          Bytes.toBytes(colName),
          Bytes.toBytes(colValue.toString))
      }
    }

    output.foreachPartition(it => {
      val hConf = HBaseConfiguration.create()
      hConf.addResource("hbase-site.xml")
      val hTableName = "test_log"
      val tableName = TableName.valueOf(hTableName)
      val conn = ConnectionFactory.createConnection(hConf)
      val table = conn.getTable(tableName)
      while (it.hasNext) {
        table.put(it.next())
      }
    })
  }
}
