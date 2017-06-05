import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import scala.collection._

object TweetAnalysis {
  def main(args: Array[String]) = {
     //System.setProperty("hadoop.home.dir", "c:/winutils")
     
     val conf = new SparkConf().setAppName("TrumpTweets").setMaster("local[4]")
     val sc = new SparkContext(conf) 
     
     val positiveLines = sc.textFile("src/main/scala/positiveWordsPhrases.txt")
     val negativeLines = sc.textFile("src/main/scala/negativeWordsPhrases.txt")
     val trumpTweets = sc.textFile("src/main/scala/trumpTwitter2015-2017.csv")
     
     case class Tweet(text: String, created: String, retweets: Int, favs: Int, isRetweet: String)
     val positiveWords = positiveLines.flatMap(line => (line.split(","))).map(x => (x.toLowerCase().trim,1)).collectAsMap() 
     val negativeWords = negativeLines.flatMap(line => (line.split(","))).map(x => (x.toLowerCase().trim,1)).collectAsMap() 
     val tweetsText = trumpTweets.map(line => line.split(",").map(_.trim))
     val header = tweetsText.first
     val tweetData = tweetsText.filter(_(0) != header(0)).map(x => Tweet(x(2).trim, x(3).trim, x(4).trim.toInt, x(6).trim.toInt, x(7)))
     val cleanText = tweetData.map(x => (x.text, x.text.replaceAll("[^a-zA-Z ]", "")))
     val flatMapTweets = cleanText.map(x => (x._1,getWordSubset(x._2))).flatMap{case(x,y) => y.map(str => (x,str))}
     val posCountMap = flatMapTweets.filter{case(x,y) => positiveWords.contains(y.trim)}.map{case(x,y) => (x,(1,0))}
     val negCountMap = flatMapTweets.filter{case(x,y) => negativeWords.contains(y.trim)}.map{case(x,y) => (x,(0,1))}
     case class TweetSide(tweet: String, posCount: Int, negCount: Int)
     val totalCount = posCountMap.union(negCountMap).reduceByKey((x,y) => (x._1+y._1,x._2+y._2)).map{case(x,(x1,y2)) => TweetSide(x,x1,y2)}
     val posCount = totalCount.filter(x => x.posCount > x.negCount).count
     val negCount = totalCount.filter(x => x.posCount < x.negCount).count
     val neutralCount = totalCount.filter(x => x.posCount == x.negCount).count
     
     
     println("Total Tweets: " + cleanText.count)  
     println("Positive: " + posCount + " Negative: " + negCount + " Neutral: " + neutralCount)
  }
  
  def getWordSubset(x: String) : Array[String] = {
    val arr = x.split(" ")
    var allSubSet = Array[String]()
    
    for (i <- 0 to arr.length - 1) {
      var word = ""
      val max = if (i + 2 < arr.length) i else if(i + 1 < arr.length) i - 1 else i - 2
      for (j <- i to max + 2) {
        word = word + " " + arr(j)
        allSubSet = allSubSet :+ word.toLowerCase()
      }
    }
    
    return allSubSet
  }
}