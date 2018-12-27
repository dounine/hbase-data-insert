package com.dounine.hbase

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BulkLoad {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      //      .setMaster("local[12]")
      .setAppName("HbaseBulkLoad")

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
        val immutableRowKey = new ImmutableBytesWritable(rowKey)

        val colFam = x._2._1
        val colName = x._2._2
        val colValue = x._2._3

        val kv = new KeyValue(
          rowKey,
          Bytes.toBytes(colFam),
          Bytes.toBytes(colName),
          Bytes.toBytes(colValue.toString)
        )
        (immutableRowKey, kv)
      }
    }


    val hConf = HBaseConfiguration.create()
    hConf.addResource("hbase-site.xml")
    val hTableName = "test_log"
    hConf.set("hbase.mapreduce.hfileoutputformat.table.name", hTableName)
    val tableName = TableName.valueOf(hTableName)
    val conn = ConnectionFactory.createConnection(hConf)
    val table = conn.getTable(tableName)
    val regionLocator = conn.getRegionLocator(tableName)

    val hFileOutput = "/tmp/h_file"

    output.saveAsNewAPIHadoopFile(hFileOutput,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hConf
    )

    val bulkLoader = new LoadIncrementalHFiles(hConf)
    bulkLoader.doBulkLoad(new Path(hFileOutput), conn.getAdmin, table, regionLocator)
  }

}