package info.exascale.wdctools.instancematching

import info.exascale.wdctools.instancematching.matchingstrategies.NameExactMatch
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * Created by alberto on 20/07/16.
  */
object RunMatcher {

  def triplesFromString(triples_str: String) = {
    triples_str.split("\n").map { line =>
      val Array(s, p, o) = line.split("\t")
      (s, p, o)
    }
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("TransformFeeds")
      .set("spark.sql.parquet.compression.codec", "snappy") // snappy compression for parquet
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sqlContext = new sql.SQLContext(sc)
    import sqlContext.implicits._


    val pagesDatasetPath = "/Users/alberto/Documents/Projects/Voldemort/Datasets/raw_pages.parquet" //args(0)
    val triplesDatasetPath = "/Users/alberto/Documents/Projects/Voldemort/Datasets/test_imfw_annotations.parquet" //args(1)
    val wikipediaLinks = "/Users/alberto/Documents/Projects/Voldemort/Datasets/test_imfw_wikianchors.parquet" //args(2)
    val outputBasename = "test_output" //args(3)


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
        // remove < and > from page URL
        //TODO: keep this line?
        //        pageUrl.substring(1, pageUrl.length - 1).trim -> tmp.result().trim
        pageUrl -> tmp.result().trim
      }.toDF("url", "triples_str") // a datafrane (url [(subject, predicate, obj)])

    // url, content
    val pages = sqlContext.read.parquet(pagesDatasetPath)

    // url, anchorText, wikiUrl
    val wikilinks = sqlContext.read.parquet(wikipediaLinks).select("url", "wikiLink").rdd.
      groupBy(r => r.getAs[String]("url")).
      map { case (url, items) =>
        val tmp = new ListBuffer[String]
        (tmp /: items) { case (acc, row) =>
          acc += row.getAs[String]("wikiLink")
        } //fold
        url -> tmp.result()
      }.toDF("url", "wikilinks") // a dataframe (url, [wikilink])

    // (url, content, triples, wikilinks)
    val pagesAndTriples = pages.join(triplesByPage, "url").join(wikilinks, "url")

    //    pagesAndTriples.write.mode(SaveMode.Overwrite).parquet(outputBasename)
    val mappings = pagesAndTriples.mapPartitions { rowItr =>
      // *** put here an instance of your favourite matcher ***
      val matcher: InterPageMatchingStrategy = new NameExactMatch("diuflx75", 6379, 1)
      rowItr.flatMap { row =>
        matcher.matchInPage(row.getAs[String]("url"),
          row.getAs[String]("content"),
          triplesFromString(row.getAs[String]("triples_str")),
          row.getAs[Seq[String]]("wikilinks"))
      }
    }.toDF() // a dataframe (pageUrl: String, localEntityUri: String, dbPediaUri: String)

    //    mappings.write.mode(SaveMode.Overwrite).parquet(outputBasename)

    //     subject, predicate, obj, pageUrl, tpd, extractor
    val originalTriples = sqlContext.read.parquet(triplesDatasetPath).
      select("url", "subject", "predicate", "obj")

    // substitutes all local urls with their DBpedia equivalent
    // subject, predicate, obj, pageUrl, dbPediaUri
    val subjectSubstituted = mappings.join(originalTriples,
      mappings("pageUrl") === originalTriples("url") &&
        mappings("localEntityUri") === originalTriples("subject")).
      drop("subject").drop("localEntityUri").
      withColumnRenamed("dbPediaUri", "subject").
      withColumnRenamed("pageUrl", "triplePageUrl")

//    subjectSubstituted.write.mode(SaveMode.Overwrite).parquet(outputBasename)

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
