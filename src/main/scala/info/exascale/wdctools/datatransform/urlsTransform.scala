package info.exascale.wdctools.datatransform

import com.netaporter.uri.Uri
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext, sql}

import scala.language.postfixOps

object urlsTransform {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("RecreateURLsWithHostname")
      .set("spark.sql.parquet.compression.codec", "snappy")
    val sc = new SparkContext(conf)

    val sqlContext = new sql.SQLContext(sc)
    val df = sqlContext.read.parquet("/user/vfelder/urls/urls.parquet/")

    // Some URLs (eg. http://example.com:/foo.html) made Uri crash, I had to implement a regex
    // based check
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

    val sqlGetHost = udf(getHost)

    df
      .withColumn("hostname", sqlGetHost(col("page")))
      .write.parquet("/user/vfelder/urls/urlsparsed.parquet/")
  }
}
