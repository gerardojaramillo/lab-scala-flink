/**
 * UsingOfFlinkKafkaSource.scala
 * @author
 *   Gerardo Jaramillo (https://me@gerardojaramillo.dev)
 */

package lab

import magnolia1.Monadic.Ops
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.csv.CsvReaderFormat
import org.apache.flink.util.Collector
import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flinkx.api.semiauto._
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.time.Duration
import java.util.ArrayList
import java.util.List

object UsingOfFlinkKafkaSource {

  def kafkaSource(): Unit = {
    val kafkaSource: KafkaSource[String] = KafkaSource
      .builder()
      .setBootstrapServers("localhost:9094,localhost:9095")
      .setTopics(List.of("flink-topic"))
      .setGroupId("flink-topic-group")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(new KafkaRecordDeserializationSchema[String] {
        override def deserialize(
            record: ConsumerRecord[Array[Byte], Array[Byte]],
            out: Collector[String]): Unit = {
          out.collect(new String(record.value(), "UTF-8"))
        }
        override def getProducedType: TypeInformation[String] =
          TypeInformation.of(classOf[String])
      })
      .build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream =
      env.fromSource(
        kafkaSource,
        WatermarkStrategy.noWatermarks(),
        "Kafka Source")
    stream.print
    env.execute("kafkaSource")
  }

  def csvSource(dir: String): Unit = {
    val source: FileSource[String] =
      FileSource
        .forRecordStreamFormat(new TextLineInputFormat(), new Path(dir))
        .monitorContinuously(Duration.ofSeconds(5))
        .build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val lines =
      env.fromSource(source, WatermarkStrategy.noWatermarks(), "Wherever")
    lines
      .filter(_.nonEmpty)
      .map { line =>
        val split = line.split(",").map(_.trim)
        s"Line: ${split(0)} - ${split(1)} - ${split(2)}"
      }
      .print()
    env.execute("cvsSource.")
  }

  def jsonFileSystemSource(dir: String): Unit = {
    ???
  }

  def main(args: Array[String]): Unit = {
    csvSource("/Users/millodev/Files")
  }

}
