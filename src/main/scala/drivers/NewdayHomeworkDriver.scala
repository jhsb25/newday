package drivers

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import util.Logging

object NewdayHomeworkDriver extends Logging {

  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession.builder()
        .appName("newday-homework")
        .getOrCreate()

    val startTime = DateTime.now()
    logger.info("Started at " + startTime.toString)

    try{
      new NewdayHomeworkJob().run(args(0), args(1), args(2))
    } catch {
      case e: Exception =>
        logger.error("Exception thrown", e)

    } finally {
      val endTime = DateTime.now()
      logger.info("Finished at " + endTime.toString() + ", took " + ((endTime.getMillis.toDouble - startTime.getMillis.toDouble) / 1000))
      spark.stop()
    }
  }

}
