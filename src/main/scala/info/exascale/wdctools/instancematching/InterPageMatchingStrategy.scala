package info.exascale.wdctools.instancematching

/**
  * Created by alberto on 20/07/16.
  */

/**
  * Class defining a match between a Web Entity and a DBpedia entity.
  * @param localEntityUri the uri of the Web Entity in the webpage where it is found
  * @param dbPediaUri the uri of the DBpedia entity
  */
case class InstanceMatch(pageUrl: String, localEntityUri: String, dbPediaUri: String)


/**
  * Trait that should be used to define strategies to match Web Entities
  * to DBpedia entities contained in a single webpage.
  */
trait InterPageMatchingStrategy {

  /**
    * Method to find matching between Web Entities and DBpedia entities
    * found in the given page
    * @param pageUrl the URL of the current page
    * @param pageSource the source code of the current page
    * @param annotations the semantic annotations found in the page in
    *                    the form (subject, predicate, object)
    * @param dbpediaEntities the URLs of the Wikipedia pages of the
    *                        DBpedia entities found in the page.
    *
    * @return a list of mappings between Web Entities and DBpedia entities.
    */
  def matchInPage(pageUrl: String,
                  pageSource: String,
                  annotations: Seq[(String, String, String)],
                  dbpediaEntities: Seq[String]): Seq[InstanceMatch]

}//InterPageMatchingStrategy
