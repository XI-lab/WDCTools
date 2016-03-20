package info.exascale.wdctools

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext, sql}

import scala.xml.{Node, NodeSeq}
import scala.xml.Source.fromString
import org.json4s._
import org.json4s.native.JsonMethods._


object anchorsDomParsing {
  val linkPattern = Pattern.compile("<a[^>]* href=[\\\"']?((http|\\/\\/|https){1}([^\\\"'>]){0,20}(\\.m.)?wikipedia\\.[^\\\"'>]{0,5}\\/w(iki){0,1}\\/[^\\\"'>]+)[\"']?[^>]*>(.+?)<\\/a>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

  def findParentOfText(dom: Node, value: String): NodeSeq = (dom \\ "p").filter(_.toString.toLowerCase.contains(value))

  def extractFromHtml(content: String, page: String): Option[Array[Output]] = {
    try {
      val pageMatcher: Matcher = linkPattern.matcher(content)
      val dom = HTML5Parser.loadXML(fromString(content))
      var output = List[Output]()

      while (pageMatcher.find) {
        val hrefGroup = pageMatcher.group(6)
        val linkGroup = pageMatcher.group(1)
        if (hrefGroup != null && linkGroup != null) {
          val href = hrefGroup.replace("\n", " ").replace("\r", " ").replace("\t", " ")
          val link = linkGroup.replace("\n", " ").replace("\r", " ").replace("\t", " ")
          val paragraph = findParentOfText(dom, href.toLowerCase)
          if (paragraph.nonEmpty && paragraph.length > 0) {
            paragraph.foreach(p => {
              if (p.text.nonEmpty) {
                output = Output(page.toString, href.toString, link.toString, paragraph.text) :: output
              }
            })
          }
        }
      }

      if (output.nonEmpty) {
        Some(output.toArray)
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

  def newRows(row: sql.Row): Option[Array[Output]] = {
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

  case class Output(page: String, href: String, link: String, paragraph: String)

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("AnchorsDomParsing")
    conf.set("spark.sql.parquet.compression.codec", "snappy")
    conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    conf.set("spark.shuffle.consolidateFiles", "false")
    conf.set("spark.shuffle.manager", "sort")
    //conf.set("spark.shuffle.service.enable", "true")
    conf.set("spark.akka.heartbeat.interval", "100")

    val sc = new SparkContext(conf)
    val sqlContext = new sql.SQLContext(sc)
    import sqlContext.implicits._

    val lines = sc.textFile("/user/atonon/WDC_112015/data/anchor_pages/*.gz", 500000)

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
      .toDF("page", "href", "link", "paragraph")

    df
      .write.mode(SaveMode.Overwrite)
      .parquet("/user/vfelder/paragraph/")
  }
}
