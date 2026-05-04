/**
 * UsingOfFlinkKafkaSource.scala
 * @author
 *   Gerardo Jaramillo
 */

package lab

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber
import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flinkx.api.semiauto._

import java.util.ArrayList
import java.util.List

object UsingOfFlinkKafkaSource {

  def main(args: Array[String]): Unit = {

    val kafkaSource: KafkaSource[String] = KafkaSource
      .builder()
      .setBootstrapServers("localhost:9092")
      .setGroupId("wherever")
      .setTopics(List.of("mytopic"))
      .setStartingOffsets(OffsetsInitializer.earliest())
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
