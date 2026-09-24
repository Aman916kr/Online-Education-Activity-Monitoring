import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

object HBaseAlertWriter {

  def writeAlert(
      studentId: String,
      lastEventTime: Long,
      status: String
  ): Unit = {

    val configuration: Configuration =
      HBaseConfiguration.create()

    val connection =
      ConnectionFactory.createConnection(configuration)

    val table =
      connection.getTable(
        TableName.valueOf("student_alerts")
      )

    val put =
      new Put(Bytes.toBytes(studentId))

    put.addColumn(
      Bytes.toBytes("activity"),
      Bytes.toBytes("last_event_time"),
      Bytes.toBytes(lastEventTime.toString)
    )

    put.addColumn(
      Bytes.toBytes("activity"),
      Bytes.toBytes("status"),
      Bytes.toBytes(status)
    )

    table.put(put)

    table.close()
    connection.close()
  }
}