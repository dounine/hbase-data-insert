package com.dounine.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HbaseHdfsWrite {

  def main(args: Array[String]): Unit = {
    require(args.length == 1)
    require(args(0).startsWith("hdfs:///") || args(0).startsWith("file:/"))

    val sparkConf = new SparkConf()
      .setMaster("local[12]")
      .setAppName("HbaseHdfsWrite")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()
    val sc = spark.sparkContext

    val logDir = new Path(args(0))

    val hConf = new Configuration()
    hConf.addResource("hdfs-site.xml")
    val fs = FileSystem.get(hConf)
    if (!fs.exists(logDir)) throw new Exception(logDir.toUri.getPath + " director not exist.")
    val logFiles = fs.listFiles(logDir, true)
    while (logFiles.hasNext) {
      val file = logFiles.next()
      if (file.getPath.getName.endsWith("log")) {
        spark.sparkContext.textFile(file.getPath.toString, 10)
          .map {
            line => {
              val infos = line.split("\\|")
              val rowKey = Bytes.toBytes(infos(0))
              val colFam = "ext"
              val colName = infos(1)
              val colValue = infos(2)

              val put = new Put(rowKey)
              put.addColumn(Bytes.toBytes(colFam),
                Bytes.toBytes(colName),
                Bytes.toBytes(colValue.toString))
            }
          }
          .foreachPartition(it => {
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

  }
}
