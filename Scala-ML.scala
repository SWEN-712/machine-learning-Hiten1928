// Databricks notebook source
def requestFormatter(givenTweet:String):String={
  s"""{
    "documents":[
        {
        "language":"en",
        "id":1,
        "text":"${givenTweet}"
        }
    ]
  }"""
}

// COMMAND ----------

def sendPostRequest(textAnalyticsUrl:String,subscriptionKey:String,requestBody:String):String={
  import scalaj.http.Http
  Thread.sleep(3000)
  val result = Http(textAnalyticsUrl).postData(requestBody)
  .header("Content-Type", "application/json")
  .header("Ocp-Apim-Subscription-Key", subscriptionKey).asString
  result.body
}

// COMMAND ----------

def removeHttpLines(textLine:String):Boolean={
  import scala.util.matching.Regex
  val pattern = "^http".r
  pattern.findFirstIn(textLine) match {
    case Some(x)=>false
    case _ => true
  }
}

// COMMAND ----------

def removeError(textLine:String):Boolean={
  print(textLine)
  import scala.util.matching.Regex
  val pattern2 = "^MissingRequiredBody".r
  pattern2.findFirstIn(textLine) match {
    case Some(x)=>false
    case _ => true
  }
}

// COMMAND ----------

val tweetsSentimentsRdd = sc.textFile("/FileStore/tables/TanjaKleut_tweets.txt").filter(removeHttpLines).map(x=>requestFormatter(x)).map(y=>sendPostRequest("https://eastus.api.cognitive.microsoft.com/text/analytics/v2.1/sentiment","64d3039684ea43c78a50c84fd5d91c8f",y))

// COMMAND ----------

val tweetsSentimentList = tweetsSentimentsRdd.collect().filter(eachObject=>(!eachObject.contains("MissingRequiredBody")))

// COMMAND ----------

case class ResponseBody(id:String, score:Double)
case class AzureTextAnalyticsResponse(documents: List[ResponseBody], errors: List[String])

// COMMAND ----------

object ResponseJsonUtility extends java.io.Serializable {
 import spray.json._
 import DefaultJsonProtocol._
object MyJsonProtocol extends DefaultJsonProtocol {
 implicit val responseBodyFormat = jsonFormat(ResponseBody,"id","score") //this represents the inner document object of the Json
 implicit val responseFormat = jsonFormat(AzureTextAnalyticsResponse,"documents","errors") //this represents the outer key-value pairs of the Json
 }
//and lastly, a function to parse the Json (string) needs to be written which after parsing the Json string returns data in the form of case class object.
import MyJsonProtocol._
import spray.json._
 
 def parser(givenJson:String):AzureTextAnalyticsResponse = {
   givenJson.parseJson.convertTo[AzureTextAnalyticsResponse]
 }
}

// COMMAND ----------

val tweetsSentimentScore = tweetsSentimentList.filter(eachResponse=>eachResponse.contains("documents")).filter(eachResponse=>(!eachResponse.contains("Document text is empty."))).map(eachResponse=>ResponseJsonUtility.parser(eachResponse)).map(parsedResponse=>parsedResponse.documents(0).score)

// COMMAND ----------

(tweetsSentimentScore.sum)/(tweetsSentimentScore.length)

// COMMAND ----------

tweetsSentimentScore.max
