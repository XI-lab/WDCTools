package info.exascale.wdctools

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.xml.{NodeSeq, Node}
import scala.xml.Source.fromString
import org.apache.spark.sql.Row
import org.apache.spark.sql._


object anchorsDomParsing {
  val linkPattern = Pattern.compile("<a[^>]* href=[\\\"']?((http|\\/\\/|https){1}([^\\\"'>]){0,20}(\\.m.)?wikipedia\\.[^\\\"'>]{0,5}\\/w(iki){0,1}\\/[^\\\"'>]+)[\"']?[^>]*>(.+?)<\\/a>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

  def findParentOfText(dom: Node, value: String): NodeSeq = (dom \\ "p").filter(_.text.toLowerCase.contains(value))

  def extractFromHtml(content: String, page: String): Array[Output] = {
    val pageMatcher: Matcher = linkPattern.matcher(content)
    val dom = HTML5Parser.loadXML(fromString(content))
    var output = List[Output]()

    while (pageMatcher.find) {
      val href = pageMatcher.group(6).replace("\n", " ").replace("\r", " ").replace("\t", " ")
      val link = pageMatcher.group(1).replace("\n", " ").replace("\r", " ").replace("\t", " ")

      val paragraph = findParentOfText(dom, href.toLowerCase)
      if (paragraph.nonEmpty) {
        output = Output(page, href, link, paragraph.text) :: output
      } else {
        output = Output(page, href, link, "") :: output
      }
    }

    output.toArray
  }

  def newRows(row: Row): Array[Output] = {
    val default = Array(Output("", "", "", ""))
    try {
      val content = row.get(0)
      val url = row.get(1)
      if (content != null && content.toString.length > 0 && url != null && url.toString.length > 0) {
        extractFromHtml(content.toString, url.toString)
      } else {
        default
      }
    } catch {
      case _: Throwable => {
        default
      }
    }
  }

  case class Output(page: String, href: String, link: String, paragraph: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AnchorsDomParsing").set("spark.sql.parquet.compression.codec", "snappy")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df: RDD[Output] = sqlContext.read.json("/user/atonon/WDC_112015/data/anchor_pages/*.gz")
      .map(newRows)
      .flatMap(row => row)

    df
      .toDF()
      .write
      .parquet("/user/vfelder/anchorsContext/anchors.parquet/")

  }
}
