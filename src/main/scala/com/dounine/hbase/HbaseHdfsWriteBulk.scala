package com.dounine.hbase

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object HbaseHdfsWriteBulk {


  val sparkConf = new SparkConf()
    //    .set("spark.shuffle.service.enabled", "true")
    //    .set("spark.dynamicAllocation.enabled", "true")
    //    .set("spark.dynamicAllocation.minExecutors", "1")
    //    .set("spark.dynamicAllocation.initialExecutors", "1")
    //    .set("spark.dynamicAllocation.maxExecutors", "3")
    //    .set("spark.dynamicAllocation.executorIdleTimeout", "60")
    //    .set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "60")
    //    .set("spark.executor.cores", "6")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    .setMaster("local[12]")
    .setAppName("HbaseHdfsWrite")

  val spark: SparkSession = SparkSession
    .builder
    .config(sparkConf)
    .getOrCreate()


  def main(args: Array[String]): Unit = {
    require(args.length == 1, "请输入log目录")

    val sc = spark.sparkContext

    val logDir = new Path(args(0))

    val hConf = new Configuration()
    hConf.addResource("hbase-site.xml")
    hConf.set("hbase.mapreduce.hfileoutputformat.table.name", "test_log")

    val fs = FileSystem.get(hConf)
    if (!fs.exists(logDir)) throw new Exception(logDir.toUri.getPath + " director not exist.")
    val logPaths = Utils.allLogs(fs, logDir).map {
      path => path.toUri.getPath
    }

    println(s"logPaths.size = ${logPaths.size}")

    val output = sc.textFile(logPaths.mkString(","))
      .repartition(100)
      .mapPartitions(lines => lines.map(line => Utils.insertDataToHbase1(mutable.Buffer(line))))
      .flatMap {
        lines => {
          lines.flatMap {
            line => {
              val rowKey = Bytes.toBytes(line._1)
              val immutableRowKey = new ImmutableBytesWritable(rowKey)
              line._2.map {
                item => {
                  val kv = new KeyValue(
                    rowKey,
                    Bytes.toBytes("ext"),
                    Bytes.toBytes(item._1),
                    Bytes.toBytes(item._2)
                  )
                  (line._1, item._1, item._2)
                }
              }
            }
          }
        }
      }
      .sortBy(_.toString)
      .map(line => {
        (
          new ImmutableBytesWritable(Bytes.toBytes(line._1)),
          new KeyValue(
            Bytes.toBytes(line._1),
            Bytes.toBytes("ext"),
            Bytes.toBytes(line._2),
            Bytes.toBytes(line._3)
          )
        )
      })


    val jobId = UUID.randomUUID().toString
    val hFileOutput = s"/tmp/bulkload/$jobId"

    val job = Job.getInstance(hConf, "HbaseBulkLoad")

    output.saveAsNewAPIHadoopFile(
      hFileOutput,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      job.getConfiguration
    )

    job.getConfiguration.set("mapred.output.dir", hFileOutput)
    output.saveAsNewAPIHadoopDataset(job.getConfiguration)


  }

}
