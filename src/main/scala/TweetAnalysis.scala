import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import scala.collection._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object TweetAnalysis {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("log4j").setLevel(Level.OFF)
  def main(args: Array[String]) = {
     //System.setProperty("hadoop.home.dir", "c:/winutils")
     
     val conf = new SparkConf().setAppName("TrumpTweets").setMaster("local")
     val sc = new SparkContext(conf) 
     
     val positiveLines = sc.textFile("src/main/scala/positiveWordsPhrases.txt")
     val negativeLines = sc.textFile("src/main/scala/negativeWordsPhrases.txt")
     val englishWords = sc.textFile("src/main/scala/englishWords.txt").map(x => (x.toLowerCase().trim,1)).collectAsMap()
     val trumpTweets = sc.textFile("src/main/scala/trumpTwitter2015-2017.csv")
     
     case class Tweet(text: String, created: String, retweets: Int, favs: Int, isRetweet: String)
     val positiveWords = positiveLines.flatMap(line => (line.split(","))).map(x => (x.toLowerCase().trim,1)).collectAsMap()
     val negativeWords = negativeLines.flatMap(line => (line.split(","))).map(x => (x.toLowerCase().trim,1)).collectAsMap()
     val tweetsText = trumpTweets.map(line => line.split(",").map(_.trim))
     val header = tweetsText.first
     val tweetData = tweetsText.filter(_(0) != header(0)).map(x => Tweet(x(2).trim, x(3).trim, x(4).trim.toInt, x(6).trim.toInt, x(7)))
     val cleanText = tweetData.map(x => ((x.text, x.retweets), x.text.replaceAll("[^a-zA-Z ]", "")))
     val flatMapTweets = cleanText.map(x => (x._1,getWordSubset(x._2))).flatMap{case(x,y) => y.map(str => (x,str))}
     
     val posCountMap = flatMapTweets.filter{case(x,y) => positiveWords.contains(y.trim)}.map{case(x,y) => (x,(1,0))}
     val negCountMap = flatMapTweets.filter{case(x,y) => negativeWords.contains(y.trim)}.map{case(x,y) => (x,(0,1))}
     case class TweetSide(tweet: String, posCount: Int, negCount: Int, retweets : Int)
     val totalCount = posCountMap.union(negCountMap).reduceByKey((x,y) => (x._1+y._1,x._2+y._2)).map{case(x,(x1,y2)) => TweetSide(x._1,x1,y2, x._2)}
     val posCount = totalCount.filter(x => x.posCount > x.negCount).count
     val negCount = totalCount.filter(x => x.posCount < x.negCount).count
     val neutralCount = totalCount.filter(x => x.posCount == x.negCount).count
      
     println("Total Tweets: " + cleanText.count)  
     println("Positive: " + posCount + " Negative: " + negCount + " Neutral: " + neutralCount)
          
     //Get top 20 most frequently used words
     val wordCount = cleanText.flatMap(l => l._1._1.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
     val countWord = wordCount.map({case (k, v) => (v, k)}).filter({case (k, v) => v.size > 3})
     val sortedCount = countWord.sortByKey(false).take(25).map({case (k, v) => (v, k)})
     
     println("--------- TOP 25 MOST FREQUENTLY USED WORDS -----------")
     sortedCount.foreach({case (k, v) => println("Word: " + k + " Count: " + v.toString())})
     println()
     
     
     val filterMentionTweets = wordCount.map({case (k, v) => (v, k)}).filter({case (k, v) => v.contains("@")})
     val sortedMentionTweets = filterMentionTweets.sortByKey(false).take(25).map({case (k, v) => (v, k)})
     
     println("--------- TOP 10 USERS TRUMP TWEETS -----------")
     
     sortedMentionTweets.foreach({case (k, v) => println("User: " + k + " Count: " + v.toString())})
     println()
     
     val mostPositive = totalCount.sortBy(x => x.posCount * -1).take(10)
     val mostNegative = totalCount.sortBy(x => x.negCount * -1).take(10)
     
     println()
     println("Total Tweets: " + cleanText.count)  
     println("Positive: " + posCount + " Negative: " + negCount + " Neutral: " + neutralCount)
     println()
     println("\nTop Positive Tweets")
     mostPositive.foreach(x => println("--> " + x.tweet + "\n"))
     
     println("\n\nTop Negative Tweets")
     mostNegative.foreach(x => println("--> " + x.tweet + "\n"))
     println()
     
     //begin vocabulary 
     println("\n\n---------- VOCABULARY ANALYSIS ----------")
     val justText = cleanText.map{case(x,y) => y}
     val justWords = justText.flatMap(_.split(" ")).map(_.trim.toLowerCase).filter(_.length > 0).filter(x => englishWords.contains(x))
     val wordCounts = justWords.map(x => (x,1)).reduceByKey((x,y) => (x + y))
     val orderedWordCounts = wordCounts.sortBy{case(x,y) => x}
//     orderedWordCounts.foreach{case(x,y) => println(y + "|" + x)}
     
     println("Total unique words: " + orderedWordCounts.count)
     
     val oneChar = orderedWordCounts.filter{case(x,y) => x.length == 1}.map{case(x,y) => (x.length,y)}.reduceByKey((x,y) => (x + y))
     val twoChar = orderedWordCounts.filter{case(x,y) => x.length == 2}.map{case(x,y) => (x.length,y)}.reduceByKey((x,y) => (x + y))
     val threeChar = orderedWordCounts.filter{case(x,y) => x.length == 3}.map{case(x,y) => (x.length,y)}.reduceByKey((x,y) => (x + y))
     val fourChar = orderedWordCounts.filter{case(x,y) => x.length == 4}.map{case(x,y) => (x.length,y)}.reduceByKey((x,y) => (x + y))
     val fiveChar = orderedWordCounts.filter{case(x,y) => x.length == 5}.map{case(x,y) => (x.length,y)}.reduceByKey((x,y) => (x + y))
     val sixChar = orderedWordCounts.filter{case(x,y) => x.length == 6}.map{case(x,y) => (x.length,y)}.reduceByKey((x,y) => (x + y))
     val sevenChar = orderedWordCounts.filter{case(x,y) => x.length == 7}.map{case(x,y) => (x.length,y)}.reduceByKey((x,y) => (x + y))
     val eightChar = orderedWordCounts.filter{case(x,y) => x.length == 8}.map{case(x,y) => (x.length,y)}.reduceByKey((x,y) => (x + y))
     val nineChar = orderedWordCounts.filter{case(x,y) => x.length == 9}.map{case(x,y) => (x.length,y)}.reduceByKey((x,y) => (x + y))
     val tenOrMoreChar = orderedWordCounts.filter{case(x,y) => x.length >= 10}.map{case(x,y) => (x.length,y)}.reduceByKey((x,y) => (x + y))
     
     println("Words used with 1 character: " + oneChar.first._2 + "\n" +
     "Words used with 2 characters: " + twoChar.first()._2 + "\n" +
     "Words used with 3 characters: " + threeChar.first()._2 + "\n" +
     "Words used with 4 characters: " + fourChar.first()._2 + "\n" +
     "Words used with 5 characters: " + fiveChar.first()._2 + "\n" +
     "Words used with 6 characters: " + sixChar.first()._2 + "\n" +
     "Words used with 7 characters: " + sevenChar.first()._2 + "\n" +
     "Words used with 8 characters: " + eightChar.first()._2 + "\n" +
     "Words used with 9 characters: " + nineChar.first()._2 + "\n" +
     "Words used with 10 or more characters: " + tenOrMoreChar.first()._2)
     
     // MOST POPULAR TWEETS
     // Tweet(text: String, created: String, retweets: Int, favs: Int, isRetweet: String)
     println("\n\n---------- MOST POPULAR TWEETS ----------")
     println("--- Top 10 Most Favorited Tweets ---")
     tweetData.sortBy(_.favs, false).take(10).foreach(tweet => println(tweet.favs + " favorites: " + tweet.text + "\n"))

     //case class TweetSide(tweet: String, posCount: Int, negCount: Int)
     println("--- Top 10 Most Retweets (Positive) ---")
     totalCount.filter(tweet => tweet.negCount < tweet.posCount).sortBy(_.retweets,false).take(10).foreach(tweet => println(tweet.retweets + " retweets: " + tweet.tweet + "\n"))

     println("--- Top 10 Most Retweets (Negative) ---")
     totalCount.filter(tweet => tweet.negCount > tweet.posCount).sortBy(_.retweets,false).take(10).foreach(tweet => println(tweet.retweets + " retweets: " + tweet.tweet + "\n"))
     
     println("--- Top 10 Most Favorited and Retweeted Tweets ---")
     tweetData.sortBy(tweet => tweet.retweets + tweet.favs, false).take(10).foreach(tweet => println(tweet.retweets + " retweets & " + tweet.favs + " favorites: " + tweet.text + "\n"))
     
     //MOST POPULAR TWEET TIMES
     println("\n\n---------- MOST POPULAR TWEETS TIMES ----------")
     tweetData.map(tweet => (tweet.created.split(" ")(3).split(":")(0), 1))
     .reduceByKey{(x, y) => x + y}
     .sortByKey().collect
     .foreach(time => println("HOUR " + time._1 + ": " + time._2 + " tweets"))
     
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
