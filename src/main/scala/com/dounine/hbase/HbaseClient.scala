package com.dounine.hbase


import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.hadoop.hbase.util.Bytes

class HbaseClient private(conf: Configuration, tableName: String) extends Serializable {

  private val connection: Connection = ConnectionFactory.createConnection(conf)
  private val table: Table = connection.getTable(TableName.valueOf(tableName))

  def put(row: Put): Unit = {
    table.put(row)
  }

  def createTable(tableName: String): Unit = {
    val table = TableName.valueOf(tableName)
    val tableDesc = TableDescriptorBuilder.newBuilder(table)
    tableDesc.setValue(TableDescriptorBuilder.SPLIT_POLICY, classOf[KeyPrefixRegionSplitPolicy].getName)
    tableDesc.setValue(KeyPrefixRegionSplitPolicy.PREFIX_LENGTH_KEY, "2")

    val extCF = ColumnFamilyDescriptorBuilder
      .newBuilder("ext".getBytes())
      .setCompressionType(Algorithm.SNAPPY)
      .build()
    tableDesc.setColumnFamilies(util.Arrays.asList(extCF))
    val splitKeys = (0 until 256).map(Integer.toHexString).map(i=>s"0$i".takeRight(2)).map(Bytes.toBytes).toArray
    connection.getAdmin.createTable(tableDesc.build(), splitKeys)
  }
}

object HbaseClient {
  private var pools = Map[String, HbaseClient]()

  def apply(conf: Configuration, tableName: String): HbaseClient = {
    this.synchronized {
      if (pools.contains(tableName)) {
        pools(tableName)
      } else {
        val hbaseClient = new HbaseClient(conf, tableName)
        pools += (tableName -> hbaseClient)
        hbaseClient
      }
    }
  }

}
