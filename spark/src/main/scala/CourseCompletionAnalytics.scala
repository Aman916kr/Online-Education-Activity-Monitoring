import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CourseCompletionAnalytics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CourseCompletionAnalytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Read events from Kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "education-events")
      .option("startingOffsets", "latest")
      .load()

    // Convert Kafka value into String
    val rawEvents = kafkaDF
      .selectExpr(
        "CAST(value AS STRING) AS event"
      )

    // Split event into columns
    val events = rawEvents
      .select(
        split(col("event"), ",").getItem(0).alias("event_id"),
        split(col("event"), ",").getItem(1).alias("student_id"),
        split(col("event"), ",").getItem(2).alias("session_id"),
        split(col("event"), ",").getItem(3).alias("course_id"),
        split(col("event"), ",").getItem(4).alias("event_type"),
        split(col("event"), ",").getItem(5).alias("event_time"),
        split(col("event"), ",").getItem(6).alias("device"),
        split(col("event"), ",").getItem(7).cast("int").alias("duration")
      )

    // Count course completions
    val courseCompletions = events
      .filter(col("event_type") === "COURSE_COMPLETE")
      .groupBy("course_id")
      .agg(
        countDistinct("student_id").alias("completed_students")
      )

    val query = courseCompletions.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option(
        "checkpointLocation",
        "/online-education/course-completion-checkpoint"
      )
      .start()

    query.awaitTermination()
  }
}