package com.dounine.hbase

import java.security.MessageDigest
import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Date
import java.util.function.Consumer

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.codec.binary.Hex
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Utils extends Serializable {

  def getLocalDateTime(time: Long): LocalDateTime = {
    val instant = new Date(time).toInstant
    val zone = ZoneId.systemDefault
    val localDateTime = LocalDateTime.ofInstant(instant, zone)
    localDateTime
  }

  def convertLogs(lines: mutable.Buffer[String]): List[Log] = {
    lines.flatMap {
      line => {
        if (StringUtils.isNotBlank(line)) {
          val jsonObject = JSON.parseObject(line)
          if (jsonObject.containsKey("data") && jsonObject.get("data").isInstanceOf[JSONObject]) {
            val timeLong = jsonObject.getLong("ti")
            if (jsonObject.get("data") != null && timeLong != null) {
              val time = getLocalDateTime(timeLong)
              val logDataJson = jsonObject.getJSONObject("data")
              if (null != logDataJson) {
                return List(Log(time, logDataJson))
              }
            }
          }
        }
        List[Log]()
      }
    }.toList
  }

  def md5(key: String): String = md5(key.getBytes, 0, key.length)

  def md5(key: Array[Byte], offset: Int, length: Int): String = {
    val e = MessageDigest.getInstance("MD5")
    e.update(key, offset, length)
    val digest = e.digest
    new String(Hex.encodeHex(digest))
  }

  def insertDataToHbase(lines: mutable.Buffer[String]): List[Put] = {
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS")
    convertLogs(lines).flatMap {
      logEntity => {
        val dataJsonObject = logEntity.data
        if (dataJsonObject.get("ext").isInstanceOf[JSONObject]) {
          val extJsonObject = dataJsonObject.getJSONObject("ext")
          val v = dataJsonObject.getString("v")
          val v2 = dataJsonObject.getString("v2")
          if (extJsonObject.containsKey("list")) {
            val uid = StringUtils.defaultString(extJsonObject.getString("uid"), "")
            val uid2 = StringUtils.defaultString(extJsonObject.getString("uid2"), "")
            val tid = StringUtils.defaultString(extJsonObject.getString("tid"), "")
            val tid2 = StringUtils.defaultString(extJsonObject.getString("tid2"), "")
            val `type` = extJsonObject.getString("type")
            if (StringUtils.isBlank(uid) && StringUtils.isBlank(uid2) && StringUtils.isBlank(tid) && StringUtils.isBlank(tid2)) {
              return List[Put]()
            } else if (StringUtils.isBlank(`type`)) {
              return List[Put]()
            }

            val timeStr = logEntity.time.format(dtf)
            val ip = StringUtils.defaultString(extJsonObject.getString("ip"), "")
            val md5Str = md5(s"""$uid|$uid2|$tid|$tid2|${`type`}""")
            if (extJsonObject.get("list").isInstanceOf[JSONArray]) {
              val list = extJsonObject.getJSONArray("list")
              var listMerge = ListBuffer[Put]()
              list.forEach(new Consumer[Any] {
                override def accept(itemObj: Any): Unit = {
                  if (itemObj.isInstanceOf[JSONObject]) {
                    val item = itemObj.asInstanceOf[JSONObject]
                    val rowKey = s"${md5Str.substring(0, 2)}|$timeStr|${md5Str.substring(2, 8)}"

                    val row = new Put(Bytes.toBytes(rowKey))
                    import scala.collection.JavaConversions._
                    for (name <- extJsonObject.keySet) {
                      val value = extJsonObject.getString(name)
                      if (!("list" == name) && StringUtils.isNotBlank(value)) row.addColumn("ext".getBytes, Bytes.toBytes(name), Bytes.toBytes(value))
                    }
                    for (name <- item.keySet) {
                      val value = item.getString(name)
                      if (StringUtils.isNotBlank(value)) row.addColumn("ext".getBytes, Bytes.toBytes("list_" + name), Bytes.toBytes(value))
                    }
                    val MILL = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
                    row.addColumn("ext".getBytes, Bytes.toBytes("time"), Bytes.toBytes(logEntity.time.format(MILL)))
                    row.addColumn("ext".getBytes, Bytes.toBytes("ip"), Bytes.toBytes(ip))
                    if (StringUtils.isNotBlank(v)) row.addColumn("ext".getBytes, Bytes.toBytes("sdkVersion"), Bytes.toBytes(v))
                    if (StringUtils.isNotBlank(v2)) row.addColumn("ext".getBytes, Bytes.toBytes("sdkVersion"), Bytes.toBytes(v2))

                    if (logEntity.data.containsKey("device")) {
                      val deviceObj = logEntity.data.getJSONObject("device")
                      for (name <- deviceObj.keySet) {
                        val value = item.getString(name)
                        if (StringUtils.isNotBlank(value)) row.addColumn("ext".getBytes, Bytes.toBytes("device_" + name), Bytes.toBytes(value))
                      }
                    }

                    listMerge += row
                  }
                }
              })
              return listMerge.toList
            }
          }
        }
        List[Put]()
      }
    }
  }

  def insertDataToHbase1(lines: mutable.Buffer[String]): List[(String, mutable.Buffer[(String, String)])] = {
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS")
    convertLogs(lines).flatMap {
      logEntity => {
        val dataJsonObject = logEntity.data
        if (dataJsonObject.get("ext").isInstanceOf[JSONObject]) {
          val extJsonObject = dataJsonObject.getJSONObject("ext")
          val v = dataJsonObject.getString("v")
          val v2 = dataJsonObject.getString("v2")
          if (extJsonObject.containsKey("list")) {
            val uid = StringUtils.defaultString(extJsonObject.getString("uid"), "")
            val uid2 = StringUtils.defaultString(extJsonObject.getString("uid2"), "")
            val tid = StringUtils.defaultString(extJsonObject.getString("tid"), "")
            val tid2 = StringUtils.defaultString(extJsonObject.getString("tid2"), "")
            val `type` = extJsonObject.getString("type")
            if (StringUtils.isBlank(uid) && StringUtils.isBlank(uid2) && StringUtils.isBlank(tid) && StringUtils.isBlank(tid2)) {
              return List[(String, mutable.Buffer[(String, String)])]()
            } else if (StringUtils.isBlank(`type`)) {
              return List[(String, mutable.Buffer[(String, String)])]()
            }

            val timeStr = logEntity.time.format(dtf)
            val ip = StringUtils.defaultString(extJsonObject.getString("ip"), "")
            val md5Str = md5(s"""$uid|$uid2|$tid|$tid2|${`type`}""")
            if (extJsonObject.get("list").isInstanceOf[JSONArray]) {
              val list = extJsonObject.getJSONArray("list")
              var listMerge = ListBuffer[(String, mutable.Buffer[(String, String)])]()
              var cols = mutable.Buffer[(String, String)]()
              list.forEach(new Consumer[Any] {
                override def accept(itemObj: Any): Unit = {
                  if (itemObj.isInstanceOf[JSONObject]) {
                    val item = itemObj.asInstanceOf[JSONObject]
                    val rowKey = s"${md5Str.substring(0, 2)}|$timeStr|${md5Str.substring(2, 8)}"

                    extJsonObject.keySet.forEach(new Consumer[String] {
                      override def accept(name: String): Unit = {
                        val value = extJsonObject.getString(name)
                        if (!("list" == name) && StringUtils.isNotBlank(value)) {
                          cols += Tuple2(name, value)
                        }
                      }
                    })
                    item.keySet.forEach(new Consumer[String] {
                      override def accept(name: String): Unit = {
                        val value = item.getString(name)
                        if (StringUtils.isNotBlank(value)) {
                          cols += Tuple2("list_" + name, value)
                        }
                      }
                    })
                    val MILL = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
                    cols += Tuple2("time", logEntity.time.format(MILL))
                    cols += Tuple2("ip", ip)
                    if (StringUtils.isNotBlank(v)) {
                      cols += Tuple2("sdkVersion", v)
                    }
                    if (StringUtils.isNotBlank(v2)) {
                      cols += Tuple2("sdkVersion", v2)
                    }

                    if (logEntity.data.containsKey("device")) {
                      val deviceObj = logEntity.data.getJSONObject("device")
                      deviceObj.keySet.forEach(new Consumer[String] {
                        override def accept(name: String): Unit = {
                          val value = item.getString(name)
                          if (StringUtils.isNotBlank(value)) {
                            cols += Tuple2("device_" + name, value)
                          }
                        }
                      })
                    }
                    listMerge += Tuple2(rowKey, cols.sortBy(_._1))
                  }
                }
              })
              return listMerge.toList
            }
          }
        }
        List[(String, mutable.Buffer[(String, String)])]()
      }
    }
  }

  def allLogs(fs: FileSystem, dir: Path): ListBuffer[Path] = {
    try
      return printDir(fs, dir, ListBuffer[Path]())
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    ListBuffer[Path]()
  }

  @throws[Exception]
  private def printDir(fs: FileSystem, dir: Path, paths: ListBuffer[Path]): ListBuffer[Path] = {
    val globStatus: Array[FileStatus] = fs.listStatus(dir)
    val stat2Paths: Array[Path] = FileUtil.stat2Paths(globStatus)
    stat2Paths.foreach {
      file => {
        if (fs.getFileStatus(file).isDirectory) {
          printDir(fs, file, paths)
        } else if (file.getName.endsWith(".log")) {
          paths += file
        }
      }
    }
    paths
  }

  def main(args: Array[String]): Unit = {
    val ss = mutable.Buffer(Tuple2("ext:aid","a"),Tuple2("ext:valid","a"),Tuple2("ext:balid","a"))
    val cc = ss.sortBy(_._1)
    println(s"cc = ${cc}")
    val kv = new KeyValue(Bytes.toBytes("abc"),
      Bytes.toBytes("ext"),
      Bytes.toBytes("type"),
      Bytes.toBytes("login")
    )

    println(s"kv.toString = ${kv.toString}")
  }

}


case class DspLog(
                   tid: String,
                   tid2: String,
                   uid: String,
                   uid2: String,
                   ak: String,
                   adGroup: String,
                   adLocation: String,
                   adPlan: String,
                   advertiser: String,
                   redirectAppId: String,
                   ccode: String,
                   creative: String,
                   flowOwner: String,
                   production: String,
                   valid: String,
                   time: String,
                   `type`: String
                 )
