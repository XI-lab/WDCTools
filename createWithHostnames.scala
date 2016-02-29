import com.netaporter.uri.Uri
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql.functions._

object NewParquetWithHostname {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RecreateWithHostname")
    val sc = new SparkContext(conf)

    val sqlContext = new sql.SQLContext(sc)
    val df = sqlContext.parquetFile("/user/vfelder/feeds/feeds.parquet/")

    val getHost: (String => String) = (page: String) => {
      val uri = Uri.parse(page)
      uri.host.toString
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
      .saveAsParquetFile("/user/vfelder/feeds/feedsparsed.parquet/")
  }

}
