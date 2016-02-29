package info.exascale.feedTransform

import com.netaporter.uri.Uri
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.functions._
import scala.language.postfixOps

object feedsTransform {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("RecreateWithHostname")
      .set("spark.sql.parquet.compression.codec", "snappy")
    val sc = new SparkContext(conf)

    val sqlContext = new sql.SQLContext(sc)
    val df = sqlContext.read.parquet("/user/vfelder/feeds/feedscoalesced.parquet/")

    val hostnamePattern = "((\\/\\/|https\\:\\/\\/|http\\:\\/\\/)([^\\/\\:]+))"r

    val getHost: (String => String) = (page: String) => {
      val preFiltered = hostnamePattern findFirstIn page
      if (preFiltered.isEmpty) {
        println(s"prefiltering failed: $page")
        ""
      } else {
        val prefilteredString = preFiltered.get
        try {
          val host = Uri.parse(prefilteredString).host
          if (host.isEmpty) {
            println(s"parsing failed: $page prefiltered as: $prefilteredString")
            ""
          } else {
            host.get
          }
        } catch {
          case e: Throwable => {
            val exception = e.toString
            println(s"caught $exception")
            ""
          }
        }
      }
    }

    val relPattern = """rel=["']?([^'"]*)["']?""".r
    val getRel: (String => String) = (tag: String) => {
      val rel = relPattern findFirstIn tag
      if (rel.isEmpty)
        ""
      else
        rel.get
    }

    val hrefPattern = """href=["']?([^'"]*)["']?""".r
    val getHref: (String => String) = (tag: String) => {
      val href = hrefPattern findFirstIn tag
      if (href.isEmpty)
        ""
      else
        href.get
    }

    val titlePattern = """title=["']?([^'"]*)["']?""".r
    val getTitle: (String => String) = (tag: String) => {
      val title = titlePattern findFirstIn tag
      if (title.isEmpty)
        ""
      else
        title.get
    }

    val sqlGetHost = udf(getHost)
    val sqlGetRel = udf(getRel)
    val sqlGetHref = udf(getHref)
    val sqlGetTitle = udf(getTitle)

    df
      .withColumn("hostname", sqlGetHost(col("page")))
      .withColumn("rel", sqlGetRel(col("tag")))
      .withColumn("href", sqlGetHref(col("tag")))
      .withColumn("title", sqlGetTitle(col("tag")))
      .write.parquet("/user/vfelder/feeds/feedsparsed.parquet/")
  }
}
