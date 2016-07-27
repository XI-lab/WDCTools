package info.exascale.wdctools.instancematching.matchingstrategies

import com.redis.RedisClient
import info.exascale.wdctools.instancematching.{InstanceMatch, InterPageMatchingStrategy}

/**
  * Created by alberto on 20/07/16.
  */
class NameExactMatch(redisHost: String,
                     redisPort: Int,
                     redisDB: Int = 1) extends InterPageMatchingStrategy{

  val redis = new RedisClient(redisHost, redisPort, database = redisDB)

  val NamePredicate_re = "<http://schema.org(/[^/]+)?/name>".r

  val NonWords_re = "\\W+".r
  val DoubleSpace_re = "\\s\\s+".r
  def cleanName(s: String) = {
    DoubleSpace_re.replaceAllIn(NonWords_re.replaceAllIn(s, " "), " ").trim.toLowerCase
  }

  /**
    * Method to find matching between Web Entities and DBpedia entities
    * found in the given page
    *
    * @param pageUrl         the URL of the current page
    * @param pageSource      the source code of the current page
    * @param annotations     the semantic annotations found in the page in
    *                        the form (subject, predicate, object)
    * @param dbpediaEntities the URLs of the Wikipedia pages of the
    *                        DBpedia entities found in the page.
    * @return a list of mappings between Web Entities and DBpedia entities.
    */
  override def matchInPage(pageUrl: String,
                           pageSource: String,
                           annotations: Seq[(String, String, String)],
                           dbpediaEntities: Seq[String]): Seq[InstanceMatch] = {

    // a map mention -> [url, mention]
    val webEntitiesNames = annotations.filter { case (_, predicate, _) =>
      predicate.toLowerCase match {
        case NamePredicate_re(_*) => true
        case _ => false
    }}.map{ case (s, _, name) => (s, cleanName(name)) }.groupBy(_._2)

    val prefix = "wikipedia.org/wiki/"
    val start_from = prefix.length

    // a map mention -> entity
    val dbpediaEntitiesNames = dbpediaEntities.toSet.
      flatMap { url: String =>
        val wikiTitle = url.substring(url.lastIndexOf(prefix) + start_from)
        val mentions = redis.hgetall(wikiTitle).getOrElse(Map[String, String]()).
          keySet.map(m => cleanName(m) -> wikiTitle)
        mentions
      }.toMap

    val mapping = for {
      (weMention, webEntities) <- webEntitiesNames
      we <- webEntities
      if dbpediaEntitiesNames.contains(weMention)
    } yield InstanceMatch(pageUrl, we._1, dbpediaEntitiesNames(weMention))
    mapping.toSeq
  }//matchInPage


}//NameExactMatch
