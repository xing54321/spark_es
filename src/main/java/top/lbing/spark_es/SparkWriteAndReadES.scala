package top.lbing.spark_es

import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sql.EsSparkSQL
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import java.util.UUID
import java.time.LocalDate

object SparkWriteAndReadES {
  case class StuInfo(id: String, name: String, sex: String, age: Int, msg1: String)
  val conf = new SparkConf()
    .setAppName("SparkWriteAndReadES")
    .setMaster("local[*]")
    // 设置es参数
    .set("es.nodes", "localhost")
    .set("es.port", "9200")
    // 若不存在索引，则自动创建索引
    .set("es.index.auto.create", "true")
  val sc = new SparkContext(conf)

  // 索引名=前缀+日期，方便批量删除，es 5.x后不再支持ttl
  val indexName = "prefix" + "_" + LocalDate.now().toString().replace("-", "")
  // 默认类型doc，es 6.x后不再区分类型
  val indexType = "doc"
  val resource = "/" + indexName + "/" + indexType

  /**
   * 写入es
   */
  def writeES() {
    var list: List[StuInfo] = Nil
    list = list :+ StuInfo(UUID.randomUUID().toString().replace("-", ""), "Jack", "男", 18, "This is Jack.")
    list = list :+ StuInfo(UUID.randomUUID().toString().replace("-", ""), "Karry", "女", 18, "This is Karry.")
    val rdd = sc.makeRDD(list)
    // 指定id
    val map = Map("es.mapping.id" -> "id")
    EsSpark.saveToEs(rdd, resource, map)
  }

  /**
   * 读取es
   */
  def readES() {
    val resRdd = EsSpark.esRDD(sc, resource)
    println("esRDD读取ES结果如下：")
    resRdd.foreach(println)
  }

  /**
   * 删除es
   */
  def deleteES() {
    // 弃用ttl后替代方案：https://blog.csdn.net/baidu_38225647/article/details/112108534
    // 基于时间的索引名,springboot定时任务删除
  }

  def main(args: Array[String]): Unit = {
    // writeES()
    readES()
  }
}