package drivers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.functions._
import util.Logging

class NewdayHomeworkJob(implicit sparkSession: SparkSession) extends Logging {

  import sparkSession.implicits._

  def run(moviesPath: String, ratingsPath: String, outputPath: String): Unit = {

    val (movies, ratings) = createDataFrames(moviesPath: String, ratingsPath: String)
    val (moviesRatings, userRatings) = processDataFrames(movies, ratings)

    Seq((movies, "movies"), (ratings, "ratings"), (moviesRatings, "moviesRatings"), (userRatings, "userRatings"))
        .foreach { case (df, dfName) => {
          df.show()
          df.write.parquet(s"$outputPath$dfName")
        }}

    Seq((movies, "movies"), (ratings, "ratings"), (moviesRatings, "moviesRatings"), (userRatings, "userRatings"))
        .foreach { case (df, dfName) => {
          df.show()
        }}
  }

  def processDataFrames(movies: DataFrame, ratings: DataFrame): (DataFrame, DataFrame) = {

    val ratingWindowSpec = Window.partitionBy($"UserID").orderBy($"AvgRating".desc)

    val aggregatedRatings = ratings
        .groupBy($"MovieID")
        .agg(
          max($"Rating").as("MaxRating"),
          avg($"Rating").as("AvgRating"),
          min($"Rating").as("MinRating")
        )
        .persist()

    val moviesRatings = movies.join(aggregatedRatings,
      movies("MovieID") === aggregatedRatings("MovieID")).drop(movies("MovieID"))

    val userRatings = ratings.join(aggregatedRatings, ratings("MovieID") === aggregatedRatings("MovieID"))
        .drop(ratings("MovieID"))
        .withColumn("MovieRank",  rank() over ratingWindowSpec)
        .filter($"MovieRank" === lit(1) || $"MovieRank" === lit(2) || $"MovieRank" === lit(3))

    (moviesRatings, userRatings)
  }

  def createDataFrames(moviesPath: String, ratingsPath: String): (DataFrame, DataFrame) = {

    val movies = sparkSession
        .read
        .format("csv")
        .load(moviesPath)
        .withColumn("movies_string", split($"_c0", "::"))
        .select(
          $"movies_string".getItem(0).cast(IntegerType).as("MovieID"),
          $"movies_string".getItem(1).as("Title"),
          $"movies_string".getItem(2).as("Genres")
        )
        .drop("movies_string")

    movies.show()

    val ratings = sparkSession
        .read
        .format("csv")
        .load(ratingsPath)
        .withColumn("ratings_string", split($"_c0", "::"))
        .select(
          $"ratings_string".getItem(0).cast(IntegerType).as("UserID"),
          $"ratings_string".getItem(1).cast(IntegerType).as("MovieID"),
          $"ratings_string".getItem(2).cast(IntegerType).as("Rating"),
          $"ratings_string".getItem(3).cast(LongType).as("Timestamp")
        )
        .drop("ratings_string")

    ratings.show()

    (movies, ratings)
  }

}
