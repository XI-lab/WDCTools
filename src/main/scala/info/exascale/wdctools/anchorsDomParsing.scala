package info.exascale.wdctools

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.jsoup._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


object anchorsDomParsing {
  case class Output(page: String, href: String, link: String, paragraph: String)

  val linkPattern = Pattern.compile("<a[^>]* href=[\\\"']?((http|\\/\\/|https){1}([^\\\"'>]){0,20}(\\.m.)?wikipedia\\.[^\\\"'>]{0,5}\\/w(iki){0,1}\\/[^\\\"'>]+)[\"']?[^>]*>(.+?)<\\/a>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

  def extractFromHtml(content: String, page: String): Option[Seq[Output]] = {
    try {
      val pageMatcher: Matcher = linkPattern.matcher(content)
      val paragraphs = Jsoup.parse(content).getElementsByTag("p").asScala
      var output = ListBuffer[Output]()

      while (pageMatcher.find) {
        val hrefGroup = pageMatcher.group(6)
        val linkGroup = pageMatcher.group(1)
        if (hrefGroup != null && linkGroup != null) {
          val href = hrefGroup.replace("\n", " ").replace("\r", " ").replace("\t", " ").trim.toLowerCase()
          val link = linkGroup.replace("\n", " ").replace("\r", " ").replace("\t", " ")
          paragraphs.foreach(paragraph => {
            if (paragraph.html().toLowerCase.contains(href)) {
              output += Output(page.toString, href, link.toString, paragraph.html())
            }
          })
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
  link: https://en.wikipedia.org/wiki/Something
  paragraph: lala<span><a href="...
   */

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("AnchorsDomParsing")
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
      //.map(x => x.page+"\t"+x.href+"\t"+x.link+"\t"+x.paragraph)
      //.saveAsTextFile("output")
      .toDF("page", "href", "link", "paragraph")

    df.write
      .parquet("/user/vfelder/paragraph/")
  }
}
