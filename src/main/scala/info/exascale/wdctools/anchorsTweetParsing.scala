package info.exascale.wdctools

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.mutable.ListBuffer


object anchorsTweetParsing {
  case class Output(page: String, url: String, query: String)

  val tweetPattern = Pattern.compile("https?:\\/\\/(www\\.)?twitter.com\\/([^\\s\"'#])*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

  def extractFromHtml(content: String, page: String): Option[Seq[Output]] = {
    try {
      val pageMatcher: Matcher = tweetPattern.matcher(content)
      var output = ListBuffer[Output]()

      while (pageMatcher.find) {
        val urlGroup = pageMatcher.group(0)
        val queryGroup = pageMatcher.group(2)
        if (urlGroup != null && queryGroup != null) {
          val url = urlGroup.replace("\n", " ").replace("\r", " ").replace("\t", " ").trim
          val query = queryGroup.replace("\n", " ").replace("\r", " ").replace("\t", " ").trim
          output += Output(page, url, query)
        }
      }

      if (output.nonEmpty) {
        Some(output.toSeq)
      } else {
        None
      }
    }
    catch {
      case _: Throwable => {
        None
      }
    }
  }

  def newRows(row: sql.Row): Option[Seq[Output]] = {
    try {
      val content = row.getString(0)
      val url = row.getString(1)
      if (content != null && content.toString.length > 0 && url != null && url.toString.length > 0) {
        extractFromHtml(content.toString, url.toString)
      } else {
        None
      }
    } catch {
      case _: Throwable => {
        None
      }
    }
  }
  /*
  page: url of the page
  href: >href</a>
  url: https://twitter.com/hashtag/hello
  query: hashtag/hello
   */

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("AnchorsTweetParsing")
    conf.set("spark.sql.parquet.compression.codec", "snappy")
    conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    conf.set("spark.shuffle.consolidateFiles", "false")
    conf.set("spark.shuffle.manager", "sort")
    conf.set("spark.shuffle.service.enable", "true")
    conf.set("spark.default.parallelism", "10000")
    conf.set("spark.executor.heartbeatInterval", "5")
    conf.registerKryoClasses(Array(classOf[Output]))

    val sc = new SparkContext(conf)

    val sqlContext = new sql.SQLContext(sc)
    import sqlContext.implicits._

    val lines = sc.textFile("/user/vfelder/anchor_pages_input/*.gz")
    // val lines = sc.textFile("input/*.gz")

    val jsonParsing = lines
      .map(line => {
        parse(line)
      })
      .map(json => {
        implicit lazy val formats = org.json4s.DefaultFormats

        val content = (json \ "content").extract[String]
        val url = (json \ "url").extract[String]
        Row(content, url)
      })

    val newRowsCreated = jsonParsing
      .map(newRows)

    val deoptionized = newRowsCreated
      .filter(_.isDefined)
      .map(_.get)

    val df = deoptionized
      .flatMap(row => row)
      //.saveAsTextFile("output")
      .toDF("page", "href", "url", "query")

    df.write
      .parquet("/user/vfelder/anchor_twitter/")
  }
}
