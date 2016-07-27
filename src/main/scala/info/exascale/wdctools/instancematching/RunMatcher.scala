package info.exascale.wdctools.instancematching

import info.exascale.wdctools.instancematching.matchingstrategies.NameExactMatch
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * Created by alberto on 20/07/16.
  * Runs an instance matching algorithm on the Voldemort dataset.
  * Instance matching algorithms must implement the InterPageMatchingStrategy
  * trait. You can then replace the content of the line marked with
  * *** put here an instance of your favourite matcher *** with
  * an instantiation of your class.
  *
  * The scripts produces a set of semantic annotations
  * (subject, predicate, object, pageUrl)
  * composing the knowledge base created by applying the instance matching algorithm.
  *
  * Command line ariguments of the script are:
  * - path to the directory containing the Web Pages dataset converted into snappy+parquet
  * - path to the directory containing the Schema.org Annotations dataset converted into snappy+parquet
  * - path to the directory containing the Wikipedia Links dataset converted into snappy+parquet
  * - path of the directory where to store the output of the script.
  *
  * To convert the files downloadable from http://voldemort.exascale.info into snappy+parquet
  * you can use the scripts contained in the info.exascale.wdctools.datatransform.rawdata2parquet
  * package.
  *
  * Good luck :-)
  */
object RunMatcher {

  def triplesFromString(triples_str: String) = {
    triples_str.split("\n").map { line =>
      val Array(s, p, o) = line.split("\t")
      (s, p, o)
    }
  }//triplesFromString

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("TransformFeeds")
      .set("spark.sql.parquet.compression.codec", "snappy") // snappy compression for parquet
//      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sqlContext = new sql.SQLContext(sc)
    import sqlContext.implicits._


    val pagesDatasetPath = args(0)
    val triplesDatasetPath = args(1)
    val wikipediaLinks = args(2)
    val outputBasename = args(3)


    //TODO: this is ugly!
    val triplesByPage = sqlContext.read.parquet(triplesDatasetPath).
      select("url", "subject", "predicate", "obj").
      rdd.
      groupBy(r => r.getAs[String]("url")).
      map { case (pageUrl, items) =>
        val tmp = new StringBuilder()
        (tmp /: items) { case (acc, row) =>
          //TODO: this is really ugly!
          val (s, p, o) = (row.getAs[String]("subject"), row.getAs[String]("predicate"), row.getAs[String]("obj"))
          acc ++= s"\n$s\t$p\t$o"
        } //fold
        pageUrl -> tmp.result().trim
      }.toDF("url", "triples_str") // a datafrane (url, triples as a string)

    // schema: (url, content)
    val pages = sqlContext.read.parquet(pagesDatasetPath)

    //schema: (url, [wikilink])
    val wikilinks = sqlContext.read.parquet(wikipediaLinks).select("url", "wikiLink").rdd.
      groupBy(r => r.getAs[String]("url")).
      map { case (url, items) =>
        val tmp = new ListBuffer[String]
        (tmp /: items) { case (acc, row) =>
          acc += row.getAs[String]("wikiLink")
        } //fold
        url -> tmp.result()
      }.toDF("url", "wikilinks") // a dataframe

    // schema: (url, content, triples, wikilinks)
    val pagesAndTriples = pages.join(triplesByPage, "url").join(wikilinks, "url")

    // schema: (pageUrl, localEntityUri, dbPediaUri)
    val mappings = pagesAndTriples.mapPartitions { rowItr =>
      // *** put here an instance of your favourite matcher ***
      val matcher: InterPageMatchingStrategy = new NameExactMatch("diuflx75", 6379, 1)
      rowItr.flatMap { row =>
        matcher.matchInPage(row.getAs[String]("url"),
          row.getAs[String]("content"),
          triplesFromString(row.getAs[String]("triples_str")),
          row.getAs[Seq[String]]("wikilinks"))
      }
    }.toDF()

    // schema: (url, subject, predicate, obj)
    val originalTriples = sqlContext.read.parquet(triplesDatasetPath).
      select("url", "subject", "predicate", "obj")

    // substitutes all local urls with their DBpedia equivalent
    // schema: subject, predicate, obj, triplePageUrl, dbPediaUri
    val subjectSubstituted = mappings.join(originalTriples,
      mappings("pageUrl") === originalTriples("url") &&
        mappings("localEntityUri") === originalTriples("subject")).
      drop("subject").drop("localEntityUri").
      withColumnRenamed("dbPediaUri", "subject").
      withColumnRenamed("pageUrl", "triplePageUrl") // have to rename this otherwise later we get an "ambiguous column" error

    val substituteObj = udf((obj: String, dbpediaEnt: String) => dbpediaEnt match {
      case null => obj
      case _ => dbpediaEnt
    })

    val subjObjSubstituted = mappings.join(subjectSubstituted,
      mappings("pageUrl") === subjectSubstituted("triplePageUrl") &&
        mappings("localEntityUri") === subjectSubstituted("obj"), "right").
      withColumn("objSubstituted", substituteObj(col("obj"), col("dbPediaUri"))).
      drop("obj").
      drop("pageUrl").
      drop("localEntityUri").
      drop("dbPediaUri").
      withColumnRenamed("objSubstituted", "obj")

    subjObjSubstituted.write.mode(SaveMode.Overwrite).parquet(outputBasename)
  } //main
}

//RunMatcher
