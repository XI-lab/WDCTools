package info.exascale.wdctools

import org.apache.spark.{SparkConf, SparkContext, sql}

object coalesce {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Coalesce1k")
      .set("spark.sql.parquet.compression.codec", "snappy")
    val sc = new SparkContext(conf)

    val sqlContext = new sql.SQLContext(sc)
    val df = sqlContext.read.parquet("/user/vfelder/feeds/feeds.parquet/")

    df
      .coalesce(1000)
      .write.parquet("/user/vfelder/feeds/feedscoalesced.parquet/")
  }
}
