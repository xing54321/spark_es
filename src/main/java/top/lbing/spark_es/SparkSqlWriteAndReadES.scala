package top.lbing.spark_es

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sql.EsSparkSQL
import java.time.LocalDate
object SparkSqlWriteAndReadES {
  case class StuInfo(id: String, name: String, sex: String, age: Int)

  val spark = SparkSession.builder()
    .appName("SparkWriteAndReadES")
    .master("local[*]")
    .config("es.index.auto.create", "true")
    .config("es.nodes", "localhost")
    .config("es.port", "9200")
    .getOrCreate()

  val sc = spark.sparkContext
  
  // 索引名=前缀+日期，方便批量删除，es 5.x后不再支持ttl
  val indexName = "prefix" + "_" + LocalDate.now().toString().replace("-", "")
  // 默认类型doc，es 6.x后不再区分类型
  val indexType = "doc"
  val resource = "/" + indexName + "/" + indexType
  
  def main(args: Array[String]) {
    val df = spark.createDataFrame(sc.parallelize(Seq(
      StuInfo("1", "xiaoming", "男", 18),
      StuInfo("2", "xiaohong", "女", 17),
      StuInfo("3", "xiaozhao", "男", 19)))).toDF("id","name", "sex", "age")
    val map = Map("es.mapping.id" -> "id")
    EsSparkSQL.saveToEs(df, resource, map)
    println("============RDD写入ES成功！！！=================")
    val esQuery =
      """
        |{
        |  "query": {
        |    "match": {
        |      "sex":"男"
        |    }
        |  }
        |}
      """.stripMargin
    val resDf = EsSparkSQL.esDF(spark, resource, esQuery)
    println("============用esDF读取ES结果如下：=================")
    resDf.orderBy("name").show(false)

    spark.stop()
  }
}