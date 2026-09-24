import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders

case class StudentEvent(
  student_id: String,
  event: String
)

case class StudentActivityState(
  lastEventTime: Long
)

case class InactiveStudent(
  student_id: String,
  lastEventTime: Long,
  status: String
)

object StudentInactivityDetection {

  def updateStudentState(
      studentId: String,
      events: Iterator[StudentEvent],
      state: GroupState[StudentActivityState]
  ): Iterator[InactiveStudent] = {

    val currentTime = System.currentTimeMillis()

    if (state.hasTimedOut) {

      val oldState = state.get

      state.remove()

      Iterator(
        InactiveStudent(
          studentId,
          oldState.lastEventTime,
          "INACTIVE"
        )
      )

    } else {

      val newState =
        StudentActivityState(currentTime)

      state.update(newState)

      state.setTimeoutDuration("30 seconds")

      Iterator.empty
    }
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("StudentInactivityDetection")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "education-events")
      .option("startingOffsets", "latest")
      .load()

    val events = kafkaDF
      .selectExpr(
        "CAST(key AS STRING) AS student_id",
        "CAST(value AS STRING) AS event"
      )
      .as[StudentEvent]

    val statefulEvents = events
      .groupByKey(_.student_id)
      .flatMapGroupsWithState(
        OutputMode.Append,
        GroupStateTimeout.ProcessingTimeTimeout
      )(updateStudentState)

    val query = statefulEvents.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option(
        "checkpointLocation",
        "/online-education/inactivity-checkpoint"
      )
      .start()

    query.awaitTermination()
  }
}