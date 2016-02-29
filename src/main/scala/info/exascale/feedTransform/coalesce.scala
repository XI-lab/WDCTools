package info.exascale.feedTransform

import org.apache.spark.{SparkConf, SparkContext, sql}

object coalesce {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Coalesce4k")
      .set("spark.io.compression.codec", "snappy")
    val sc = new SparkContext(conf)

    val sqlContext = new sql.SQLContext(sc)
    val df = sqlContext.read.parquet("/user/vfelder/feeds/feeds.parquet/")

    df
      .coalesce(4000)
      .write.parquet("/user/vfelder/feeds/feedscoalesced.parquet/")
  }
}