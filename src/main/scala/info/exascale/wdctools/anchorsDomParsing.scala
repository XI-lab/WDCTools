package info.exascale.wdctools

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType, StringType}
import org.apache.spark.{SparkConf, SparkContext}
import scala.xml.{NodeSeq, Node}
import scala.xml.Source.fromString
import org.apache.spark.sql.Row
import org.apache.spark.sql._


object anchorsDomParsing {
  val linkPattern = Pattern.compile("<a[^>]* href=[\\\"']?((http|\\/\\/|https){1}([^\\\"'>]){0,20}(\\.m.)?wikipedia\\.[^\\\"'>]{0,5}\\/w(iki){0,1}\\/[^\\\"'>]+)[\"']?[^>]*>(.+?)<\\/a>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

  def findParentOfText(dom: Node, value: String): NodeSeq = (dom \\ "p").filter(_.text.toLowerCase.contains(value))

  def extractFromHtml(content: String, page: String): Array[Row] = {
    val pageMatcher: Matcher = linkPattern.matcher(content)
    val dom = HTML5Parser.loadXML(fromString(content))
    var output = List[Row]()

    while (pageMatcher.find) {
      val href = pageMatcher.group(6).replace("\n", " ").replace("\r", " ").replace("\t", " ")
      val link = pageMatcher.group(1).replace("\n", " ").replace("\r", " ").replace("\t", " ")

      val paragraph = findParentOfText(dom, href.toLowerCase)
      if (paragraph.nonEmpty) {
        output = Row(page, href, link, paragraph.text) :: output
      } else {
        output = Row(page, href, link, "") :: output
      }
    }

    output.toArray
  }

  def newRows(row: Row): Array[Row] = {
    val default = Array(Row("", "", "", ""))
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


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AnchorsDomParsing").set("spark.sql.parquet.compression.codec", "snappy")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val schema = StructType(
        StructField("page", StringType, true) :: StructField("href", StringType, true) ::
        StructField("link", StringType, true) :: StructField("paragraph", StringType, true) :: Nil
      )

    val rdd: RDD[Row] = sqlContext.read.json("/user/atonon/WDC_112015/data/anchor_pages/*.gz")
      .map(newRows)
      .flatMap(row => row)

      val df = sqlContext.createDataFrame(rdd, schema)
        .write.parquet("/user/vfelder/anchorsContext/anchors.parquet/")

  }
}
