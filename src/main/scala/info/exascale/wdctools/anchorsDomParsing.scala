package info.exascale.wdctools

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType, StringType}
import org.apache.spark.{SparkConf, SparkContext}
import scala.xml.Node
import scala.xml.Source.fromString
import org.apache.spark.sql.Row
import org.apache.spark.sql._


object anchorsDomParsing {
  def findParentOfText(dom: Node, value: String) = (dom \\ "p").filter(_.text.contains(value))

  def extractFromHtml(content: String, page: String, pattern: Pattern): Array[Row] = {
    val pageMatcher: Matcher = pattern.matcher(content)
    val dom = HTML5Parser.loadXML(fromString(content))
    var output = List[Row]()

    while (pageMatcher.find) {
      val href = pageMatcher.group(6).replace("\n", " ").replace("\r", " ").replace("\t", " ")
      val link = pageMatcher.group(1).replace("\n", " ").replace("\r", " ").replace("\t", " ")
      val exp = pageMatcher.group(0).replace("\n", " ").replace("\r", " ").replace("\t", " ")
      val paragraph = findParentOfText(dom, exp)
        if (paragraph.nonEmpty) {
          output = Row(
            content, page,
            page, href,
            link, paragraph.text
          ) :: output
        }
    }

    output.toArray
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AnchorsDomParsing").set("spark.sql.parquet.compression.codec", "snappy")
    val sc = new SparkContext(conf)
    // val sc = new SparkContext("local", "shell")
    val sqlContext = new SQLContext(sc)

    val linkPattern = Pattern.compile("<a[^>]* href=[\\\"']?((http|\\/\\/|https){1}([^\\\"'>]){0,20}(\\.m.)?wikipedia\\.[^\\\"'>]{0,5}\\/w(iki){0,1}\\/[^\\\"'>]+)[\"']?[^>]*>(.+?)<\\/a>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

    val schema = StructType(
        StructField("content", StringType, true) :: StructField("url", StringType, true) ::
        StructField("page", StringType, true) :: StructField("href", StringType, true) ::
        StructField("link", StringType, true) :: StructField("paragraph", StringType, true) :: Nil
      )

    val rowRDD = sqlContext.read.json("/user/atonon/WDC_112015/data/anchor_pages/*.gz")
        .toJavaRDD

    val stillRDD: RDD[Row] = sqlContext.createDataFrame(rowRDD, schema)
      .map(row => extractFromHtml(row.get(0).toString, row.get(1).toString, linkPattern))
      .flatMap(rows => rows.toSeq)

    val df = sqlContext.createDataFrame(stillRDD, schema)
      .coalesce(1000)
      .write.parquet("/user/vfelder/anchorsContext/anchors1.parquet/")

  }
}
