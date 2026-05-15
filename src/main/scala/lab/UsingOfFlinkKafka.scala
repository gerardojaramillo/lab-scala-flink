/**
 * UsingOfFlinkKafka.scala
 * @author
 *   Gerardo Jaramillo (https://me@gerardojaramillo.dev)
 */

package lab

import org.apache.flink.api.common.eventtime.Watermark
import org.apache.flink.api.common.eventtime.WatermarkStrategy

/** import org.apache.flink.api.common.typeinfo.TypeInformation */
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema

/**
 * import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
 */

import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flinkx.api.auto._
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.{util => ju}
import org.apache.flink.api.common.typeinfo.TypeInformation

object UsingOfFlinkKafka {

  final case class Envelope(
      topic: String,
      partition: Int,
      offset: Long,
      key: Option[String],
      value: String,
      timestamp: Long,
      timestampType: String,
      headers: Map[String, String]
  )

  class EnvelopDeserialization
      extends KafkaRecordDeserializationSchema[Envelope] {

    override def deserialize(
        record: ConsumerRecord[Array[Byte], Array[Byte]],
        out: Collector[Envelope]): Unit = {
      val key =
        Option(record.key())
          .map(k => new String(k, "UTF-8"))

      val value =
        Some(record.value())
          .map(v => new String(v, "UTF-8"))
          .getOrElse("")
      val headers = record
        .headers()
        .toArray()
        .map(h => h.key() -> new String(h.value(), "UTF-8"))
        .toMap
      out.collect(
        Envelope(
          topic = record.topic(),
          partition = record.partition(),
          offset = record.offset(),
          key = key,
          value = value,
          timestamp = record.timestamp(),
          timestampType = record.timestampType().toString,
          headers = headers
        ))
    }

    override def getProducedType(): TypeInformation[Envelope] =
      deriveTypeInformation[Envelope]
  }

  def main(args: Array[String]): Unit = {
    val source: KafkaSource[Envelope] = KafkaSource
      .builder()
      .setBootstrapServers("localhost:9094,localhost:9095")
      .setTopics(ju.List.of("event-topic"))
      .setGroupId("event-topic-group")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(new EnvelopDeserialization())
      .build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream =
      env.fromSource(source, WatermarkStrategy.noWatermarks(), "Stream")
    stream.print()
    env.execute()
  }

}
