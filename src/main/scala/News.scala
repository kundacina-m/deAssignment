import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source.fromURL
import org.json4s.jackson.JsonMethods.parse
import org.apache.log4j.{Level, Logger}



case class Source(
                   id: String,
                   name: String
                 )

case class Article(
                    source: Source,
                    author: String,
                    title: String,
                    description: String,
                    url: String,
                    urlToImage: String,
                    publishedAt: String,
                    content: String,
                    var publishedDate: String = ""
                  ) {
  publishedDate = publishedAt.substring(0,10)
}

case class Response(
                     status: String,
                     totalResults: Int,
                     articles: Array[Article]
                   )

object News {

  implicit val formats = org.json4s.DefaultFormats

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    checkScriptArguments(args)

    val date = args(0)
    val tableName = args(1)
    val keyword = args(2)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("hive.optimize.sort.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    import spark.implicits._
    val parsedData = parse(fromURL(s"http://newsapi.org/v2/everything?q=bitcoin&from=$date&to=$date&sortBy=publishedAt&pageSize=100&apiKey=2c6b934ffa73493cb4ed77829d57ac63")("UTF-8").mkString).extract[Response]
    val fromApiDataset = spark.createDataset(parsedData.articles)
      .filter(
        article => checkIfNotNullAndContains(Array(
          article.title,
          article.description,
          article.author,
          article.content,
          article.source.name), keyword)
      )

    spark.sql("CREATE SCHEMA IF NOT EXISTS htec")

    if (tableAlreadyExists(tableName, spark)) {

      val mergedDF = fromApiDataset.toDF().unionByName(spark.read.table(s"htec.$tableName"))
      val finalDF = mergedDF.distinct()

      spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
      finalDF.write.partitionBy("publishedDate").mode(SaveMode.Overwrite).saveAsTable("htec.playground_temp")

      val tempDF = spark.read.table("htec.playground_temp")

      tempDF.write.mode(SaveMode.Overwrite).insertInto(s"htec.$tableName")
      spark.sql("DROP table htec.playground_temp")

    } else {

      fromApiDataset.write.partitionBy("publishedDate").mode(SaveMode.Overwrite).saveAsTable(s"htec.$tableName")
    }

    System.exit(0)
  }

  def tableAlreadyExists(tableName: String, sparkSession: SparkSession): Boolean = {
    sparkSession.catalog.tableExists(s"htec.$tableName")
  }

  def checkIfNotNullAndContains(array: Array[String], keyword: String): Boolean = {
    for (string <- array) {
      if (string != null && string.toLowerCase.contains(keyword.toLowerCase)) return true
    }

    false
  }

  def checkScriptArguments(args: Array[String]): Unit = {

    val validDateRegex = """([0-9]{4}-[0-9]{2}-[0-9]{2})"""
    val validTableNameRegex = """^[A-Za-z0-9_-]*$"""

    if (args.length != 3) {
      println("You need to enter date in 'YYYY-MM-DD' format, name of table in which data will be exported and keyword for content filtering")
      System.exit(1)
    }

    if (!args(0).matches(validDateRegex)) {
      println("First argument needs to be entered in 'YYYY-MM-DD' format")
      System.exit(1)
    }

    if (!args(1).matches(validTableNameRegex)) {
      println("Second argument needs to be entered using only letters, numbers and underscores")
      System.exit(1)
    }
  }
}