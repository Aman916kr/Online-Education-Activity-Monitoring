import org.apache.spark.sql.SparkSession

object EducationActivityStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("OnlineEducationActivityMonitoring")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val kafkaDF = spark.readStream
      .format("kafka")
      .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
      )
      .option(
        "subscribe",
        "education-events"
      )
      .option(
        "startingOffsets",
        "latest"
      )
      .load()

    val events = kafkaDF
      .selectExpr(
        "CAST(key AS STRING) AS student_id",
        "CAST(value AS STRING) AS event"
      )

    val query = events.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option(
        "checkpointLocation",
        "/online-education/checkpoint"
      )
      .start()

    query.awaitTermination()
  }
}