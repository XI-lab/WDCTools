package info.exascale.wdctools.datatransform

import com.netaporter.uri.Uri
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext, sql}

import scala.language.postfixOps

object feedsTransform {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("TransformFeeds")
      .set("spark.sql.parquet.compression.codec", "snappy") // snappy compression for parquet

    val sc = new SparkContext(conf)
    val sqlContext = new sql.SQLContext(sc)

    // schema for the CSV we'll load
    val feedSchema = StructType(Array(
      StructField("page", StringType, true),
      StructField("tag", StringType, true),
      StructField("type", StringType, true)))

    // read the CSV with our schema using databricks' spark-csv
    val df = sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .schema(feedSchema)
      .load("/data/WDC_112015/data/feeds/*.csv.gz")


    // extract from each page url their hostname
    // some URLs (eg. http://example.com:/foo.html) made Uri crash, I had to implement a regex
    // based check
    val hostnamePattern = "((\\/\\/|https\\:\\/\\/|http\\:\\/\\/)([^\\/\\:]+))"r

    val getHost: (String => String) = (page: String) => {
      val preFiltered = hostnamePattern findFirstIn page
      if (preFiltered.isEmpty) {
        println(s"prefiltering failed: $page")
        ""
      } else {
        val preFilteredString = preFiltered.get
        try {
          val host = Uri.parse(preFilteredString).host
          if (host.isEmpty) {
            println(s"parsing failed: $page prefiltered as: $preFilteredString")
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

    // extract rel="…"
    val relPattern = """.*rel=["']?([^'"]*)["']?.*""".r
    val getRel: (String => String) = (tag: String) => {
      tag match {
        // pattern-matching regex matches is so nice
        case relPattern(captured) => captured
        case _ => ""
      }
    }

    // extract href="…"
    val hrefPattern = """.*href=["']?([^'"]*)["']?.*""".r
    val getHref: (String => String) = (tag: String) => {
      tag match {
        case hrefPattern(captured) => captured
        case _ => ""
      }
    }

    // extract title="…"
    val titlePattern = """.*title=["']?([^'"]*)["']?.*""".r
    val getTitle: (String => String) = (tag: String) => {
      tag match {
        case titlePattern(captured) => captured
        case _ => ""
      }
    }

    // transform to lowercase
    val getLCType: (String => String) = (str: String) => {
      str.toLowerCase
    }

    // create spark sql user-defined functions for each of these scala function
    val sqlGetHost = udf(getHost)
    val sqlGetRel = udf(getRel)
    val sqlGetHref = udf(getHref)
    val sqlGetTitle = udf(getTitle)
    val sqlGetLCType = udf(getLCType)

    df
      // add hostname column based on page column
      .withColumn("hostname", sqlGetHost(col("page")))
      // add rel column based on tag column
      .withColumn("rel", sqlGetRel(col("tag")))
      // add href column based on tag column
      .withColumn("href", sqlGetHref(col("tag")))
      // add title column based on tag column
      .withColumn("title", sqlGetTitle(col("tag")))
      // replace type column with its lowercase version
      .withColumn("type", sqlGetLCType(col("type")))
      // only output 1,000 parquet files from the 35k csv.gz input files
      .coalesce(1000)
      // write as parquet to my hdfs home folder
      .write.parquet("/user/vfelder/feeds/feedsparsed.parquet/")
  }
}
