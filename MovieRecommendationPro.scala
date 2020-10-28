

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.ml.recommendation._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType, StringType, StructType}

import scala.collection.mutable


object MovieRecommendationPro{

  // case class for movie dataset
  case class Movies(movieId: Int, movieTitle: String)

  // case class for ratings
  case class Ratings(userID: Int, movieID: Int, rating: Float)

  // function to get movie names using movie ids from movies dataframe
  def getMovieName(movieData:Array[Movies],movieId:Int): String ={

    val result = movieData.filter(_.movieId==movieId)(0)
    // returns the movie title
    result.movieTitle

  }

  // main function
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // building a new spark session
    val spark =SparkSession
      .builder
      .appName("MovieRecommendationPro")
      .master("local[*]")
      .getOrCreate()


    println("Loading movie names...")
    // defining schema(structure) for movies dataframe
    val schemaMovie = new StructType()
      .add("movieId",IntegerType,nullable = true)
      .add("movieTitle", StringType, nullable = true)

    // defining schema(structure) for ratings dataframe
    val ratingSchema = new StructType()
      .add("userID",IntegerType,nullable = true)
      .add("movieID",IntegerType,nullable = true)
      .add("rating",IntegerType,nullable = true)
      .add("timeStamp",LongType,nullable = true)

    import spark.implicits._

    val movieData = spark.read
      .option("sep","|")
      .option("charset", "ISO-8859-1")
      .schema(schemaMovie)
      .csv("data/ml-100k/u.item")
      .as[Movies]

    val movieDataset = movieData.collect()

    val ratings = spark.read
      .option("sep", "\t")
      .schema(ratingSchema)
      .csv("data/ml-100k/u.data")
      .as[Ratings]

    // Now creating an ALS prediction model

    println("\nTraining recommendation model...")
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userID")
      .setItemCol("movieID")
      .setRatingCol("rating")

    // fitting model with ratings data
    val model = als.fit(ratings)

    val userID : Int = args(0).toInt
    val data = Seq(userID).toDF("userID")
    val predictions = model.recommendForUserSubset(data,5)

    // Displaying predictions

    println("\nTop 5 recommendations for user ID " + userID + ":")
    for (res <- predictions){
      val user = res(1)
      val temp = user.asInstanceOf[mutable.WrappedArray[Row]]
      for (ret <- temp){
        val movie = ret.getAs[Int](0)
        val rating = ret.getAs[Float](1)
        val movieName = getMovieName(movieDataset,movie)
        println(movieName,rating)
      }

    }

    spark.stop()

  }

}