package drivers

import org.apache.spark.sql.{Row, SparkSession}
import org.specs2.Specification
import org.specs2.specification.BeforeAll


class NewdayHomeworkJobSpec extends Specification with BeforeAll {
  def is =
    s2"""
      movieRatings
      should be of length 2                                                         $e1
      should have correct MaxRating                                                 $e2
      should have correct MinRating                                                 $e3
      should be of length AvgRating                                                 $e4
      userRatings
      should be of length 7                                                         $e5
      should have correct rank                                                      $e6
    """

  implicit val spark = SparkSession.builder()
      .appName("newday-homework")
      .master("local[*]")
      .getOrCreate()

  import spark.implicits._

  var movieResults: Array[Row] = _
  var ratingResults: Array[Row] = _

  def e1 = {
    movieResults.length mustEqual 4
  }

  def e2 = {
    movieResults(0).getAs[Int]("MaxRating") mustEqual 4 and(movieResults(1).getAs[Int]("MaxRating") mustEqual 5)
  }

  def e3 = {
    movieResults(0).getAs[Int]("MinRating") mustEqual 2 and(movieResults(1).getAs[Int]("MinRating") mustEqual 2)
  }

  def e4 = {
    movieResults(0).getAs[Double]("AvgRating") mustEqual 3 and(movieResults(1).getAs[Double]("AvgRating") mustEqual 4)
  }

  def e5 = {
    ratingResults.length mustEqual 7
  }

  def e6 = {
    ratingResults(0).getAs[Int]("MovieRank") mustEqual 1 and(
        ratingResults(1).getAs[Int]("MovieRank") mustEqual 2) and(
        ratingResults(2).getAs[Int]("MovieRank") mustEqual 1) and(
        ratingResults(3).getAs[Int]("MovieRank") mustEqual 1) and(
        ratingResults(4).getAs[Int]("MovieRank") mustEqual 1) and(
        ratingResults(5).getAs[Int]("MovieRank") mustEqual 2) and(
        ratingResults(6).getAs[Int]("MovieRank") mustEqual 3)
  }

  override def beforeAll(): Unit = {
    val movies = Seq(
      (10, "title1", "genre1"),
      (20, "title2", "genre2"),
      (30, "title3", "genre3"),
      (40, "title4", "genre4")
    ).toDF("MovieID", "Title", "Genre")
    val ratings = Seq(
      (1, 10, 4, 978300760L),
      (2, 10, 2, 978300761L),
      (3, 20, 5, 978300762L),
      (1, 20, 2, 978300763L),
      (5, 10, 3, 978300764L),
      (5, 30, 2, 978300765L),
      (5, 40, 1, 978300766L),
      (5, 20, 5, 978300767L)
    ).toDF("UserID", "MovieID", "Rating", "Timestamp")
    val (movieDF, ratingDF) = new NewdayHomeworkJob().processDataFrames(movies, ratings)
    movieResults = movieDF.orderBy($"MovieID").collect()
    ratingResults = ratingDF.orderBy($"UserID", $"MovieRank").collect()
    spark.stop()
  }
}