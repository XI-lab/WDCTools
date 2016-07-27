package info.exascale.wdctools.datatransform.rawdata2parquet

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by alberto on 27/07/16.
  */
object wikiAnchorsToParquet {

  /**
    * Converts the Wikipedia Links dataset downloadable
    * from http://voldemort.exascale.info/data/wiki_anchors_en/
    * in snappy+parquet format.
    * Arguments of the script are: inputDirectory, outputDirectory.
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Annotations2parquet")
      .set("spark.sql.parquet.compression.codec", "snappy")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val rawWikiLinksDir = args(0)
    val outputDir = args(1)

    val rawWikilinks = sc.textFile(rawWikiLinksDir).map { line =>
      val Array(url, anchorText, wikiPage) = line.split("\t")
      (url, anchorText, wikiPage)
    }.toDF("url", "anchorText", "wikiLink")

    rawWikilinks.write.parquet(outputDir)
  }//main


}//annotationsToParquet
