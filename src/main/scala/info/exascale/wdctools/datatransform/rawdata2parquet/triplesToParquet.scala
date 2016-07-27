package info.exascale.wdctools.datatransform.rawdata2parquet

import java.net.URL

import com.google.common.net.InternetDomainName
import org.apache.spark.{SparkConf, SparkContext, sql}

import scala.language.postfixOps


/**
  * Created by alberto on 02/03/16.
  *
  * Stores the 5-ples in parquet format.
  * The stored data includes the pay level domain of the webpage
  * from which each triple was extracted.
  * Arguments passed to the main are:
  * - the input basename of the triple files (e.g. WDC_112015/data/part*.gz)
  * - the directory where to store the output
  */
object triplesToParquet {

  val AnnotationPattern = """(\S+)\s+(\S+)\s+(.+)\s+<([^>]+)>\s+(<[^>]+>)\s+\.""".r

  case class AnnotationTPD(subject: String, predicate: String, obj: String, url: String, tpd: String, extractor: String)

  def getTopPrivateDomain(urlStr: String): String = {
    var url: URL = null
    try {
      url = new URL(urlStr)
    } catch {
      case e: Throwable => return ""
    }

    val hostname = url.getHost
    try {
      val idn = InternetDomainName.from(hostname)
      if (idn.isPublicSuffix)
        return idn.name()

      if (idn.hasPublicSuffix) {
        val publicSuffixLen = idn.publicSuffix.name().length
        val toReturn = idn.topPrivateDomain.name()
        toReturn.substring(0, toReturn.length - publicSuffixLen - 1) // -1 is for the .
      } else {
        // if it does not have a public suffix return the last piece of the hostname, just in case :P
        hostname.substring(hostname.lastIndexOf(".") + 1)
      } //if
    } catch {
      case e: IllegalArgumentException if e.getMessage.contains("Not a valid domain name") =>
        hostname
    }
  } //getTopPrivateDomain


  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Triples_to_parquet")
      .set("spark.sql.parquet.compression.codec", "snappy")
//      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val triplesFile = args(0)
    val outputFile = args(1)

    val sqlContext = new sql.SQLContext(sc)

    import sqlContext.implicits._

    val triplesRDD = sc.textFile(triplesFile, 8).map {
      case AnnotationPattern(s, p, o, page, extractor) =>
        val tpd = getTopPrivateDomain(page.substring(1, page.length - 1))
        AnnotationTPD(s, p, o, page, tpd, extractor)
      case badLine =>
        AnnotationTPD("match_error", "", "", "", "", "")
    }.toDF()

    triplesRDD.write.parquet(outputFile)
  } //main_TPD

}

//TriplesToParquet
