import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.ml.recommendation._

object MovieRecommendationPro{

  // case class for movie dataset
  case class Movies(movieId: Int, movieTitle: String)

  // case class for ratings
  case class Ratings(userID: Int, movieID: Int, rating: Float)

  def getMovieName(movieData:Array[Movies],movieId:Int):String{

  val result = movieData.filter(._movieId==movieId)
  result.movieTitle

  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark =SparkSession
      .builder()
      .appName("MovieRecommendationPro")
      .master("local[*]")
      .getOrCreate()

    val schemaMovie = new StructType()
      .add("movieId",IntegerType,nullable = true)
      .add("movieTitle", StringType, nullable = true)

    val ratingSchema = new StructType()
      .add("userID",IntegerType,nullable = true)
      .add("movieId",IntegerType,nullable = true)
      .add("rating",FloatType,nullable = true)
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
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Rating]

    // Now creating an ALS prediction model

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.1)
      .setUserCol("userID")
      .setItemCol("movieID")
      .setRatingCol("rating")

    // fitting model with ratings data
    val model = als.fit(ratings)

    val userID : Int = args.(22).toInt
    val data = seq(userID).toDF()
    val predictions = model.recommendForUserSubset(data,5)

    // Displaying predictions

    for (res <- predictions){



    }




  }

}