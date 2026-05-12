/**
 * UsingOfFlinkKafkaSource.scala
 * @author
 *   Gerardo Jaramillo (https://me@gerardojaramillo.dev)
 */

package lab

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flinkx.api.semiauto._
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.util.ArrayList
import java.util.List

object UsingOfFlinkKafkaSource {

  def main(args: Array[String]): Unit = {

    val kafkaSource: KafkaSource[String] = KafkaSource
      .builder()
      .setBootstrapServers("localhost:9094,localhost:9095")
      .setGroupId("flink-topic-group")
      .setTopics(List.of("flink-topic"))
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(
        new KafkaRecordDeserializationSchema[String] {
          override def deserialize(
              record: ConsumerRecord[Array[Byte], Array[Byte]],
              out: Collector[String]): Unit =
            out.collect(new String(record.value(), "UTF-8"))
          override def getProducedType: TypeInformation[String] =
            TypeInformation.of(classOf[String])
        }
      )
      .build()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromSource(
      kafkaSource,
      WatermarkStrategy.noWatermarks(),
      "Kafka-Telemetry-Source")

    stream.print()
    env.execute("Job")
  }

}
