package info.exascale.wdctools

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.{SparkConf, SparkContext, sql}
import scala.xml.{NodeSeq, Node}
import scala.xml.Source.fromString


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
              output = Output(page, href, link, paragraph.text) :: output
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
      val content = row.get(0)
      val url = row.get(1)
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

    val sc = new SparkContext(conf)
    val sqlContext = new sql.SQLContext(sc)
    import sqlContext.implicits._

    // val df = sqlContext.read.json("./input/*.gz")
    val df = sqlContext.read.json("/user/atonon/WDC_112015/data/anchor_pages/*.gz")
      .map(newRows)
      .filter(_.isDefined)
      .map(_.get)
      .flatMap(row => row)
      .toDF("page", "href", "link", "paragraph")
      .write
      .parquet("/user/vfelder/paragraph/")
      // .parquet("output")

  }
}
