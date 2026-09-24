import org.apache.kafka.clients.producer._
import java.util.Properties
import java.time.Instant
import scala.util.Random

object EducationEventProducer {

  val topic = "education-events"

  val students = Seq(
    "S101", "S102", "S103", "S104",
    "S105", "S106", "S107", "S108"
  )

  val courses = Seq(
    "C101", "C102", "C103"
  )

  val events = Seq(
    "LOGIN",
    "VIDEO_START",
    "VIDEO_COMPLETE",
    "QUIZ_START",
    "QUIZ_SUBMIT",
    "COURSE_COMPLETE"
  )

  def main(args: Array[String]): Unit = {

    val properties = new Properties()

    properties.put(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      "localhost:9092"
    )

    properties.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )

    properties.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )

    val producer =
      new KafkaProducer[String, String](properties)

    val random = new Random()

    var eventNumber = 1

    while (true) {

      val student =
        students(random.nextInt(students.length))

      val course =
        courses(random.nextInt(courses.length))

      val session =
        s"SESSION-${student}-${random.nextInt(5) + 1}"

      val eventType =
        events(random.nextInt(events.length))

      val eventId =
        f"E$eventNumber%06d"

      val eventTime =
        Instant.now().toString

      val device =
        if (random.nextBoolean()) "WEB"
        else "MOBILE"

      val duration =
        random.nextInt(600)

      val message =
        s"$eventId,$student,$session,$course,$eventType,$eventTime,$device,$duration"

      val record =
        new ProducerRecord[String, String](
          topic,
          student,
          message
        )

      producer.send(record)

      println(
        s"Produced: key=$student | $message"
      )

      eventNumber += 1

      Thread.sleep(1000)
    }

    producer.close()
  }
}