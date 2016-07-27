package info.exascale.wdctools.datatransform.rawdata2parquet

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by alberto on 27/07/16.
  */
object rawPagesToParquet {


  /**
    * Converts the Web Pages dataset downloadable from
    * http://voldemort.exascale.info/data/raw_pages/
    * in the snappy+parquet format.
    * The parameters of the script are:
    * - the directory containing the downloaded dataset
    * - the directory where to store the result of the conversion.
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Annotations2parquet")
      .set("spark.sql.parquet.compression.codec", "snappy")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val rawPagesDir = args(0)
    val outputDir = args(1)

    val rawPages = sqlContext.read.json(rawPagesDir)

    rawPages.write.parquet(outputDir)
  }//main
}//rawPagesToParquet
