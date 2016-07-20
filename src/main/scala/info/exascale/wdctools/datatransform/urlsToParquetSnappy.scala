package info.exascale.wdctools.datatransform

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext, sql}

import scala.language.postfixOps

object urlsToParquetSnappy {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("urlsToParquetSnappy")
      .set("spark.sql.parquet.compression.codec", "snappy")
    val sc = new SparkContext(conf)


    val urlSchema = StructType(Array(StructField("page", StringType, true)))

    val sqlContext = new sql.SQLContext(sc)
    val df = sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .schema(urlSchema)
      .load("/user/atonon/WDC_112015/data/urls/*.csv.gz")

    df
      .coalesce(1000)
      .write.parquet("/user/vfelder/urls/urls.parquet/")
  }
}
